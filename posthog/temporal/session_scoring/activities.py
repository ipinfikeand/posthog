"""Temporal activities for the session interestingness scoring pipeline.

Two activities:
    * `list_chunks_activity` — runs once per workflow tick to gauge backlog
      and emit deterministic chunk specs. Cheap.
    * `score_chunk_activity` — runs once per chunk; does fetch-features
      (CH SELECT) → predict (XGBoost) → write-scores (CH INSERT) end to end.
      The work for each chunk is fully self-contained: no Redis, no S3, no
      cross-activity state.

Idempotency guarantees:
    * Hash partitioning (`cityHash64(session_id_v7) %% of_chunks = chunk_id`)
      gives every session exactly one bucket.
    * The CH SELECT filters `HAVING max(interestingness_score) IS NULL` —
      sessions already scored in a previous attempt are skipped naturally.
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
from posthog.temporal.session_scoring import sql as session_scoring_sql
from posthog.temporal.session_scoring.constants import (
    CH_FEATURE_QUERY_TIMEOUT_S,
    CH_INSERT_QUERY_TIMEOUT_S,
    DEFAULT_OF_CHUNKS,
    SCORE_LOOKBACK_DAYS,
    TARGET_CHUNK_SIZE,
)
from posthog.temporal.session_scoring.features import (
    FEATURE_NAMES,
    ID_COLUMNS,
    MODEL_FEATURE_SCHEMA_VERSION,
    FeatureValidationError,
    validate_features,
)
from posthog.temporal.session_scoring.scorer import predict
from posthog.temporal.session_scoring.types import ChunkResult, ChunkSpec, ListChunksResult, ScoreSessionsBatchInputs

logger = structlog.get_logger(__name__)


# --------------------------------------------------------------------------- #
# list_chunks_activity                                                         #
# --------------------------------------------------------------------------- #


def _count_unscored_in_one_bucket(lookback_days: int, of_chunks: int) -> int:
    rows = sync_execute(
        session_scoring_sql.count_unscored_sql(),
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
        logger.info("session_scoring.list_chunks.empty", lookback_days=lookback_days)
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
        "session_scoring.list_chunks.dispatched",
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
        session_scoring_sql.fetch_features_sql(),
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


def _insert_scores(df: Any, scores: Any) -> None:
    """Batch INSERT (team_id, session_id_v7, session_timestamp, score) into the writable table.

    Partial-column write — see `sql.insert_scores_sql`. The AggregatingMergeTree
    handles merging the new score onto each session's existing aggregates.
    """
    rows = [
        (
            int(row.team_id),
            int(row.session_id_v7),
            row.session_timestamp,
            float(score),
        )
        for row, score in zip(df[list(ID_COLUMNS)].itertuples(index=False), scores, strict=True)
    ]
    if not rows:
        return
    sync_execute(
        session_scoring_sql.insert_scores_sql(),
        rows,
        settings={"max_execution_time": CH_INSERT_QUERY_TIMEOUT_S},
    )


@activity.defn
async def score_chunk_activity(spec: ChunkSpec) -> ChunkResult:
    """Score one hash-partitioned chunk of unscored sessions, end to end."""
    activity.heartbeat({"phase": "fetch", "chunk_id": spec.chunk_id})
    df = await sync_to_async(_fetch_features_dataframe, thread_sensitive=False)(spec)

    if df.empty:
        return ChunkResult(chunk_id=spec.chunk_id, scored=0)

    activity.logger.info(
        "session_scoring.fetched",
        chunk_id=spec.chunk_id,
        rows=len(df),
        feature_schema_version=MODEL_FEATURE_SCHEMA_VERSION,
    )

    try:
        validate_features(df)
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

    activity.heartbeat({"phase": "write", "chunk_id": spec.chunk_id, "rows": len(df)})
    await sync_to_async(_insert_scores, thread_sensitive=False)(df, scores)

    activity.logger.info(
        "session_scoring.chunk_done",
        chunk_id=spec.chunk_id,
        scored=len(df),
        feature_count=len(FEATURE_NAMES),
    )
    return ChunkResult(chunk_id=spec.chunk_id, scored=len(df))
