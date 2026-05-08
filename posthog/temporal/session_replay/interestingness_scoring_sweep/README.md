# Session interestingness scoring

Temporal pipeline that runs an XGBoost model over recently-created sessions and
writes the resulting interestingness score (a Float32 in `[0, 1]`) onto
`raw_sessions_v3.interestingness_score`.

The score is intended to drive downstream session-summarization work — sessions
with higher scores are prioritized.

## Architecture

```text
Schedule (every 5 min, ScheduleOverlapPolicy.SKIP)
   │
   ▼
ScoreSessionsBatchWorkflow             (parent — scaffolding only)
   │
   ├── list_chunks_activity            (one cheap CH count, returns N specs)
   │
   └── asyncio.gather over chunks
         score_chunk_activity(spec)    (× N, runs in parallel)
            internally:
              1. CH SELECT eligible (hash-partitioned, IS NULL) sessions
                 from raw_sessions_v3, INNER JOIN feature CTEs over
                 session_replay_features (same expressions as the
                 training query)
              2. validate_features (hard fail on schema drift)
              3. xgboost.Booster.predict
              4. ClickhouseProducer → Kafka topic
                 (clickhouse_raw_sessions_v3_interestingness_score)
              5. CH Kafka engine table + MV → writable_raw_sessions_v3
              6. return ChunkResult(scored=N)
```

### Score writeback path

Producer side (`activities._publish_scores`):

- One JSONEachRow message per scored session on
  `KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE`.
- `session_id_v7` is sent as a decimal string because uint128 exceeds
  JSON-safe integer precision.
- `session_timestamp` is **not** sent — the writable table derives it from
  `session_id_v7` via its `DEFAULT` expression.
- `producer.flush(timeout=30s)` runs after the loop so the activity doesn't
  ack `scored=N` to the workflow before librdkafka has actually delivered.

Consumer side (CH-managed, see
`posthog/models/raw_sessions/sessions_v3_score_kafka.py`):

- `kafka_raw_sessions_v3_interestingness_score` — Kafka engine table.
- `raw_sessions_v3_interestingness_score_mv` — materialized view that does
  `INSERT INTO writable_raw_sessions_v3 (team_id, session_id_v7,
interestingness_score)` after `toUInt128(session_id_v7)`. Every other
  column gets its empty `AggregateFunction` state, which the
  AggregatingMergeTree merges as a no-op against the existing session row.

In tests `ClickhouseProducer` short-circuits to `sync_execute(sql, data)`
against `INSERT_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_SQL`, so unit tests
exercise the same writable-table contract without needing a Kafka cluster.

### Two CH tables, one pipeline

- **`raw_sessions_v3`** is where the score lives (`interestingness_score`,
  `Nullable(Float32)`, write-once via the `IS NULL` filter on the read side
  and `max` on merge so a real score never gets clobbered by NULL).
- **`session_replay_features`** is where the model's input features live
  (the table populated by the replay feature pipeline). The pipeline keys
  on `(team_id, session_id)` with string `session_id`; the JOIN back to
  `raw_sessions_v3` is via `toString(session_id_v7)`.

The serving SELECT mirrors the training query verbatim: same
`aggregated_sufficient_statistics` and `replay_features` CTE shape, same
column names, same arithmetic — so any drift between training and serving
shows up as a `validate_features` failure rather than silent score skew.

Sessions without replay features are dropped by the inner join and stay
NULL in `raw_sessions_v3`. They re-appear on subsequent ticks until they
age out of the lookback window. That's deliberate — the model can't score
them, and writing a sentinel would either need a separate column or break
the write-once semantics. Keep the lookback tight so the wasted scan cost
is bounded.

### Why this shape

- **Workflow stays tiny** — no per-session work in workflow code, payloads stay
  far below the 2 MiB Temporal hard limit.
- **Hash partitioning** by `cityHash64(session_id_v7) % of_chunks` gives every
  session exactly one bucket, lining up with the table's sharding key.
- **Idempotent on retry** — each chunk re-queries with
  `HAVING max(interestingness_score) IS NULL`, so a partial-failure retry
  naturally skips already-scored sessions. No claim/lock table needed.
- **No Redis, no S3** — every chunk is fetch-predict-write end-to-end inside
  one activity. Add Redis only if/when fetch and predict need to live on
  different worker pools (e.g., GPU vs CPU).
- **Kafka-mediated partial-column INSERT** — score writeback flows through
  the same Kafka → CH MV pipeline as every other CH writer in PostHog. We
  do not produce 200k INSERT statements per tick; we produce 200k JSONEachRow
  messages and let the CH Kafka engine table + MV do the batched insert.
  At-least-once delivery is harmless because the score column is
  `SimpleAggregateFunction(max, …)` and the pipeline only ever produces a
  single score per session.

## Throughput sizing

200k sessions / 5 min = ~667/sec. With XGBoost predict on tabular features,
the bottleneck is fetch + write, not score.

| chunk_size | of_chunks | concurrent on 2 workers | per-chunk wall time | tick wall time |
| ---------- | --------- | ----------------------- | ------------------- | -------------- |
| 5,000      | 40        | 4 in flight             | ~15s                | ~150s          |
| **10,000** | **20**    | **4 in flight**         | **~25s**            | **~125s**      |
| 20,000     | 10        | 4 in flight             | ~45s                | ~115s          |

Defaults in `constants.py` are `chunk_size=10_000`, `of_chunks=20`. Tweak
`TARGET_SESSIONS_PER_TICK` and `DEFAULT_OF_CHUNKS` for capacity changes.

## libomp thread budgeting

XGBoost on Linux uses `libgomp`; on macOS it uses `libomp`. Both are OpenMP
runtimes controlled by `OMP_NUM_THREADS`.

The single most important worker config: **don't oversubscribe cores.**

Recommended worker container env:

```bash
# Inside the worker pod, set OMP_NUM_THREADS to (CPU limit - 1) so libomp
# doesn't contend with the asyncio reactor and Temporal SDK threads.
OMP_NUM_THREADS=$(($(getconf _NPROCESSORS_ONLN) - 1))
```

Pair with **low Temporal concurrency** so each predict gets the whole CPU:

```python
Worker(
    ...,
    task_queue=settings.INTERESTINGNESS_SCORING_SWEEP_TASK_QUEUE,
    max_concurrent_activities=2,        # not 32 — let libomp do the parallelism
    max_concurrent_workflow_tasks=20,
)
```

The other valid setup is the inverse — `OMP_NUM_THREADS=1` and
`max_concurrent_activities=$(nproc)`. Pick one parallelism layer; the bug
that gets you in production is leaving both at default and finding 32×N
threads fighting for N cores.

### Containers + cgroups gotcha

`os.cpu_count()` returns the host's CPU count, not the pod's CPU limit. Set
`OMP_NUM_THREADS` explicitly from the pod's allocated quota (or read
`/sys/fs/cgroup/cpu.max`).

## Model file

A trained booster ships with the package at
`posthog/temporal/session_replay/interestingness_scoring_sweep/model.ubj`,
and that path is the default for `SESSION_INTERESTINGNESS_MODEL_PATH`.
Override the env var in production to point at a sidecar-mounted booster
when the bundled file should be replaced without a code deploy. First-call
latency is paid once per pod; warm with `scorer.warmup()` from the worker
bootstrap to remove that latency from the first chunk's wall time.

The bundled file is also the source of truth for the `test_sql_alignment.py`
parity test — any SQL/booster drift fails CI before the activity ever runs
in production. See "Updating features" below for the workflow when the
feature set changes.

`xgboost==3.2.0` is a top-level dependency, installed in every Python image.
It pulls `nvidia-nccl-cu12` (~322 MB of CUDA libs on Linux, even in CPU-only
mode) into the layer cache; that's a deliberate trade-off for keeping a
single image rather than maintaining a dedicated scoring-worker variant.

## Updating features

The trained XGBoost booster file is the **single source of truth** for which
features the model takes. `feature_names` is embedded inside the `.ubj` file
when training calls `xgb.DMatrix(..., feature_names=...)`, and serving reads
it back via `scorer.get_feature_names()`. A retrained booster with a
different feature set updates serving without a code change to `features.py`.

What still needs to be kept in sync **outside** the booster file:

1. `sql.fetch_features_sql` — the SELECT column list (plus the
   `_AGGREGATED_STATS_FRAGMENT` and `_REPLAY_FEATURES_FRAGMENT` CTEs that
   produce them) must produce a column named exactly the same as every
   `feature_names` entry, in the same order. `test_sql_alignment.py`
   asserts this against the bundled `model.ubj` at CI time;
   `validate_features` enforces it again at runtime as a defense in depth.
2. `features.FEATURE_RANGES` — runtime dtype + value-bounds contract.
   xgboost does **not** carry value ranges in the model file, so this is
   the runtime guard against "model trained on [0, 1] but the SQL started
   returning 9999". `assert_ranges_cover` runs inside `_load_booster` at
   warmup; any feature in the booster without a `FEATURE_RANGES` entry
   raises `MissingFeatureRangeError` before a single chunk is dispatched.
   `test_sql_alignment.py` also asserts equality (not just superset) so
   stale entries get flagged in CI too.

Workflow when changing features:

1. Retrain on the new feature set; replace `model.ubj` (committed to repo).
2. Update `_AGGREGATED_STATS_FRAGMENT`, `_REPLAY_FEATURES_FRAGMENT`, and
   the final SELECT in `sql.py` to produce/expose the new aliases.
3. Update `FEATURE_RANGES` to mirror the new set.
4. Bump `features.MODEL_FEATURE_SCHEMA_VERSION`.
5. `pytest posthog/temporal/tests/session_replay/interestingness_scoring_sweep/test_sql_alignment.py`
   passes when all four are aligned.

`validate_features` is a hard gate — any mismatch raises
`FeatureValidationError`, marked `non_retryable=True` in the workflow so a
schema bug fails fast rather than burning retries.

## Schedule lifecycle

Singleton, region-scoped (one Schedule per Temporal cluster). Use
`schedule.a_upsert_schedule()` to register/update during deploy and
`schedule.a_delete_schedule_if_exists()` to retire it.

The schedule fires every 5 min with `ScheduleOverlapPolicy.SKIP` — if a tick
is still running when the next is due, the new one is dropped (the next
tick's CH `IS NULL` filter naturally picks up whatever the slow tick missed).

## Open follow-ups

These are deliberately out of scope for the initial PR:

- **Extend the feature set.** The committed `model.ubj` was trained on the
  36 features the live `session_replay_features` DDL exposes today. To add
  more (e.g. `*_path_visit_count`, `network_4xx_count`, `mutation_count`,
  `unique_form_field_count`), they need to land in
  `posthog/session_recordings/sql/session_replay_feature_sql.py` (CH
  migration + Kafka MV) first, then a retrained `model.ubj` + matching
  changes to `sql.py`/`FEATURE_RANGES` get gated by `test_sql_alignment.py`.
- **Integration test for `score_chunk_activity`.** Unit coverage exists for
  `validate_features` and `scorer` (load + predict + thread safety + range
  guards). The end-to-end activity flow against real CH is still untested;
  start with a fixture-backed smoke test of `fetch_features_sql`.
- **Backfill.** Existing `raw_sessions_v3` rows are NULL, which is fine for
  "score going forward". If we ever want to score historical sessions, write
  a one-off Dagster job that walks `cityHash64(session_id_v7) % N` buckets
  and triggers the same `score_chunk_activity` per bucket.
- **Metrics.** Expose `total_scored`, `chunks_failed`, and the chunk-wall-time
  histogram to whatever observability stack the `INTERESTINGNESS_SCORING_SWEEP_TASK_QUEUE`
  worker pool uses.
