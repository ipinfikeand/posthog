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
              1. CH SELECT features for this hash bucket
              2. validate_features (hard fail on schema drift)
              3. xgboost.Booster.predict
              4. CH INSERT (team_id, session_id_v7, session_timestamp, score)
              5. return ChunkResult(scored=N)
```

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
- **Partial-column INSERT** — `INSERT INTO writable_raw_sessions_v3 (team_id,
session_id_v7, session_timestamp, interestingness_score) VALUES (…)` writes
  only the score; every other column gets its empty aggregate state, which the
  AggregatingMergeTree merges as a no-op against the existing row.

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
    task_queue=settings.SESSION_SCORING_TASK_QUEUE,
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

Loaded once per worker process from
`SESSION_INTERESTINGNESS_MODEL_PATH` (defaults to
`/models/session_interestingness/model.ubj`). Bake into the worker image or
mount via a sidecar. First-call latency is paid once per pod; warm with
`scorer.warmup()` from the worker bootstrap to remove that latency from the
first chunk's wall time.

## Updating features

The schema lives in two places that must move together:

1. `sql.FEATURE_SELECT_FRAGMENT` — what CH projects.
2. `features.FEATURE_NAMES` + `features.FEATURE_RANGES` — what the model
   expects, plus runtime range checks.

Bump `features.MODEL_FEATURE_SCHEMA_VERSION` whenever the set changes; it
gets logged on every chunk so distribution shifts can be traced to a deploy.
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

- Train the actual XGBoost model. The pipeline assumes a `model.ubj` is
  available at the configured path.
- Pick the final feature set. The defaults in `sql.FEATURE_SELECT_FRAGMENT`
  and `features.FEATURE_NAMES` are placeholders pending the trained model.
- Tests. `score_chunk_activity` is integration-heavy (CH + xgboost); start
  with unit coverage on `validate_features` and a smoke test that exercises
  `fetch_features_sql` against a fixture session.
- Backfill. Existing rows are NULL, which is fine for "score going forward".
  If we ever want to score historical sessions, write a one-off Dagster job
  that walks `cityHash64(session_id_v7) % N` buckets and triggers the same
  `score_chunk_activity` per bucket.
- Metrics. Expose `total_scored`, `chunks_failed`, and the chunk-wall-time
  histogram to whatever observability stack the SESSION_SCORING_TASK_QUEUE
  worker pool uses.
