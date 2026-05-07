"""Temporal activities for the session interestingness scoring pipeline.

Two activities:
    * `list_chunks_activity` — runs once per workflow tick to gauge backlog
      and emit deterministic chunk specs. Cheap.
    * `score_chunk_activity` — runs once per chunk; does fetch-features
      (CH SELECT) → predict (XGBoost) → publish-scores (Kafka) end to end.
      The work for each chunk is fully self-contained: no Redis, no S3, no
      cross-activity state.

Score writeback uses the standard `ClickhouseProducer` -> Kafka -> CH-Kafka-MV
pipeline; see `posthog.models.raw_sessions.sessions_v3_score_kafka`. Producing
per-row gives us at-least-once delivery with the same async, durable, retry-able
semantics as the rest of the platform's CH writers.

Idempotency guarantees:
    * Hash partitioning (`cityHash64(session_id_v7) %% of_chunks = chunk_id`)
      gives every session exactly one bucket.
    * The CH SELECT filters `HAVING max(interestingness_score) IS NULL` —
      sessions already scored in a previous attempt are skipped naturally.
    * `interestingness_score` is `SimpleAggregateFunction(max, ...)` and the
      Temporal pipeline only ever writes a single score per session, so even if
      Kafka redelivers a message after a worker crash the merge is a no-op.
    * Re-running a failed `score_chunk_activity` thus never double-scores
      a session and never burns the same CPU twice on already-written rows.
"""

from __future__ import annotations

from typing import Any

import structlog
from asgiref.sync import sync_to_async
from temporalio import activity
from temporalio.exceptions import ApplicationError

from posthog.clickhouse.client import sync_execute
from posthog.kafka_client.client import ClickhouseProducer
from posthog.kafka_client.routing import get_producer
from posthog.kafka_client.topics import KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE
from posthog.models.raw_sessions.sessions_v3_score_kafka import INSERT_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_SQL
from posthog.temporal.session_replay.interestingness_scoring_sweep import sql as interestingness_scoring_sweep_sql
from posthog.temporal.session_replay.interestingness_scoring_sweep.constants import (
    CH_FEATURE_QUERY_TIMEOUT_S,
    DEFAULT_OF_CHUNKS,
    KAFKA_PRODUCE_FLUSH_TIMEOUT_S,
    SCORE_LOOKBACK_DAYS,
    TARGET_CHUNK_SIZE,
)
from posthog.temporal.session_replay.interestingness_scoring_sweep.features import (
    ID_COLUMNS,
    MODEL_FEATURE_SCHEMA_VERSION,
    FeatureValidationError,
    validate_features,
)
from posthog.temporal.session_replay.interestingness_scoring_sweep.scorer import get_feature_names, predict
from posthog.temporal.session_replay.interestingness_scoring_sweep.types import (
    ChunkResult,
    ChunkSpec,
    ListChunksResult,
    ScoreSessionsBatchInputs,
)

logger = structlog.get_logger(__name__)


# --------------------------------------------------------------------------- #
# list_chunks_activity                                                         #
# --------------------------------------------------------------------------- #


def _count_unscored_in_one_bucket(lookback_days: int, of_chunks: int) -> int:
    rows = sync_execute(
        interestingness_scoring_sweep_sql.count_unscored_sql(),
        {"lookback_days": lookback_days, "of_chunks": of_chunks},
        settings={"max_execution_time": CH_FEATURE_QUERY_TIMEOUT_S},
    )
    return int(rows[0][0]) if rows else 0


@activity.defn
async def list_chunks_activity(_inputs: ScoreSessionsBatchInputs) -> ListChunksResult:
    """Build the chunk fan-out plan for one tick.

    Runs a cheap COUNT against a single hash bucket to estimate total backlog.
    If the estimate is zero we return an empty plan so the parent workflow
    can short-circuit instead of dispatching N empty score activities.
    """
    lookback_days = SCORE_LOOKBACK_DAYS
    of_chunks = DEFAULT_OF_CHUNKS
    chunk_size = TARGET_CHUNK_SIZE

    sampled = await sync_to_async(_count_unscored_in_one_bucket, thread_sensitive=False)(lookback_days, of_chunks)
    estimated_total = sampled * of_chunks  # extrapolate from one bucket

    if estimated_total == 0:
        logger.info("interestingness_scoring_sweep.list_chunks.empty", lookback_days=lookback_days)
        return ListChunksResult(chunks=[], estimated_unscored_sessions=0)

    chunks = [
        ChunkSpec(
            chunk_id=i,
            of_chunks=of_chunks,
            chunk_size=chunk_size,
            lookback_days=lookback_days,
        )
        for i in range(of_chunks)
    ]
    logger.info(
        "interestingness_scoring_sweep.list_chunks.dispatched",
        of_chunks=of_chunks,
        chunk_size=chunk_size,
        estimated_unscored_sessions=estimated_total,
    )
    return ListChunksResult(chunks=chunks, estimated_unscored_sessions=estimated_total)


# --------------------------------------------------------------------------- #
# score_chunk_activity                                                         #
# --------------------------------------------------------------------------- #


def _fetch_features_dataframe(spec: ChunkSpec) -> Any:
    """Run the feature SELECT and return a pandas DataFrame.

    Imported lazily to keep workers that don't run scoring (e.g., per-team
    summarization workers on the same image) from paying pandas's import cost
    and to avoid importing heavy ML deps at module load time on workers that
    don't pull this task queue.
    """
    import pandas as pd  # noqa: PLC0415  (intentional: lazy import, see docstring)

    rows, column_metadata = sync_execute(
        interestingness_scoring_sweep_sql.fetch_features_sql(),
        {
            "lookback_days": spec.lookback_days,
            "of_chunks": spec.of_chunks,
            "chunk_id": spec.chunk_id,
            "chunk_size": spec.chunk_size,
        },
        settings={"max_execution_time": CH_FEATURE_QUERY_TIMEOUT_S},
        with_column_types=True,
    )
    columns = [name for name, _type in column_metadata]
    return pd.DataFrame(rows, columns=columns)


def _publish_scores(df: Any, scores: Any) -> int:
    """Produce one Kafka message per scored session and flush before returning.

    Per-row produce keeps the activity's failure mode dead simple: a crash
    mid-loop just means the chunk is retried, sessions already produced are
    filtered out by the next tick's `HAVING max(interestingness_score) IS NULL`
    (after CH has consumed) or harmlessly re-merged via the `max`-typed score
    column (before CH has consumed).

    `ClickhouseProducer` falls back to `sync_execute(sql, data)` in TEST mode
    so unit tests can assert against the writable table directly without
    needing a Kafka cluster.

    Returns the number of rows successfully handed off to the producer (after
    flush completes). The value is what `score_chunk_activity` returns to the
    workflow as `ChunkResult.scored`.
    """
    producer = ClickhouseProducer()
    rows_published = 0
    for row, score in zip(df[list(ID_COLUMNS)].itertuples(index=False), scores, strict=True):
        producer.produce(
            sql=INSERT_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_SQL,
            topic=KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE,
            data={
                "team_id": int(row.team_id),
                "session_id_v7": str(int(row.session_id_v7)),
                "interestingness_score": float(score),
            },
        )
        rows_published += 1

    if rows_published:
        # Flush the singleton sync producer for this topic so we don't ack the
        # activity to the workflow before librdkafka has actually delivered every
        # message. In TEST mode the flush is a no-op (`sync_execute` already ran).
        get_producer(topic=KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE).flush(
            timeout=KAFKA_PRODUCE_FLUSH_TIMEOUT_S
        )

    return rows_published


@activity.defn
async def score_chunk_activity(spec: ChunkSpec) -> ChunkResult:
    """Score one hash-partitioned chunk of unscored sessions, end to end."""
    activity.heartbeat({"phase": "fetch", "chunk_id": spec.chunk_id})
    df = await sync_to_async(_fetch_features_dataframe, thread_sensitive=False)(spec)

    if df.empty:
        return ChunkResult(chunk_id=spec.chunk_id, scored=0)

    # Pull the feature schema from the booster (single source of truth). This
    # also triggers `_load_booster` on first call, so the model file is read
    # before we attempt to validate or predict, and `MissingFeatureRangeError`
    # surfaces here rather than mid-validate.
    feature_names = await sync_to_async(get_feature_names, thread_sensitive=False)()

    activity.logger.info(
        "interestingness_scoring_sweep.fetched",
        chunk_id=spec.chunk_id,
        rows=len(df),
        feature_schema_version=MODEL_FEATURE_SCHEMA_VERSION,
        feature_count=len(feature_names),
    )

    try:
        validate_features(df, feature_names=feature_names)
    except FeatureValidationError as e:
        # Non-retryable: schema drift will fail the same way on retry. Surface
        # as ApplicationError so Temporal stops the activity immediately and
        # an operator can react (deploy a fixed model or fix the SELECT).
        raise ApplicationError(
            f"feature validation failed for chunk {spec.chunk_id}: {e}",
            type="FeatureValidationError",
            non_retryable=True,
        ) from e

    activity.heartbeat({"phase": "predict", "chunk_id": spec.chunk_id, "rows": len(df)})
    scores = await sync_to_async(predict, thread_sensitive=False)(df)

    activity.heartbeat({"phase": "publish", "chunk_id": spec.chunk_id, "rows": len(df)})
    published = await sync_to_async(_publish_scores, thread_sensitive=False)(df, scores)

    activity.logger.info(
        "interestingness_scoring_sweep.chunk_done",
        chunk_id=spec.chunk_id,
        scored=published,
        feature_count=len(feature_names),
    )
    return ChunkResult(chunk_id=spec.chunk_id, scored=published)
