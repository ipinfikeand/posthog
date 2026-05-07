"""ClickHouse statements for the session-scoring pipeline.

Two operations:
    * `FETCH_FEATURES_SQL`: select unscored session keys + model features for one
      hash-partitioned chunk. Uses `HAVING max(interestingness_score) IS NULL`
      because the column is `SimpleAggregateFunction(max, Nullable(Float32))`
      on an `AggregatingMergeTree` and unmerged parts can have both a NULL and
      a real score for the same session — only the post-aggregate value is
      authoritative.

    * `INSERT_SCORES_SQL`: a partial-column insert into `writable_raw_sessions_v3`.
      Only the ORDER BY key columns + `interestingness_score` are written;
      every other column gets its default empty aggregate state. The
      AggregatingMergeTree merges this with the existing event-MV row for the
      same session, so the score lands without disturbing any of the existing
      feature aggregates.

Feature columns are kept in one place (FEATURE_SELECT_FRAGMENT) so the SELECT
that pulls features and `features.FEATURE_NAMES` (used by the model) cannot
drift apart silently — `validate_features` cross-checks them at predict time.
"""

from posthog.models.raw_sessions.sessions_v3 import DISTRIBUTED_RAW_SESSIONS_TABLE_V3, WRITABLE_RAW_SESSIONS_TABLE_V3

# --------------------------------------------------------------------------- #
# Feature definitions.                                                         #
#                                                                              #
# Each entry projects an aggregate over the v3 raw_sessions table to a single  #
# scalar feature. Order MUST match `features.FEATURE_NAMES` and the column     #
# order the XGBoost model was trained on; `validate_features` enforces this.   #
# --------------------------------------------------------------------------- #
FEATURE_SELECT_FRAGMENT = """
    sum(pageview_count)                                                        AS pageview_count,
    sum(autocapture_count)                                                     AS autocapture_count,
    sum(screen_count)                                                          AS screen_count,
    toInt64(dateDiff('millisecond', min(min_timestamp), max(max_timestamp)))   AS duration_ms,
    toUInt8(max(has_replay_events))                                            AS has_replay_events,
    toUInt8(max(has_autocapture))                                              AS has_autocapture,
    toUInt32(length(arrayDistinct(arrayFlatten(groupArray(urls)))))            AS unique_url_count,
    toUInt32(length(arrayDistinct(arrayFlatten(groupArray(event_names)))))     AS unique_event_count,
    toUInt32(length(arrayDistinct(arrayFlatten(groupArray(hosts)))))           AS unique_host_count
""".strip()


def fetch_features_sql(table: str = DISTRIBUTED_RAW_SESSIONS_TABLE_V3()) -> str:
    """Return the parameterized SELECT used by `score_chunk_activity`.

    Parameters bound at call time:
        %(of_chunks)s, %(chunk_id)s, %(lookback_days)s, %(chunk_size)s
    """
    return f"""
SELECT
    team_id,
    session_id_v7,
    session_timestamp,
    {FEATURE_SELECT_FRAGMENT}
FROM {table}
WHERE session_timestamp >= now() - toIntervalDay(%(lookback_days)s)
  AND cityHash64(session_id_v7) %% %(of_chunks)s = %(chunk_id)s
GROUP BY team_id, session_id_v7, session_timestamp
HAVING max(interestingness_score) IS NULL
LIMIT %(chunk_size)s
""".strip()


def count_unscored_sql(table: str = DISTRIBUTED_RAW_SESSIONS_TABLE_V3()) -> str:
    """Return a cheap COUNT to estimate per-tick backlog before fanning out.

    The cost matters — `lookback_days` controls how far back the scan goes,
    and `cityHash64(...) % %(of_chunks)s = 0` means we sample one bucket
    rather than scanning the full table. Multiply the result by `of_chunks`
    in the caller for a backlog estimate (good enough for "should we even
    bother dispatching this tick" decisions).
    """
    return f"""
SELECT count()
FROM (
    SELECT session_id_v7
    FROM {table}
    WHERE session_timestamp >= now() - toIntervalDay(%(lookback_days)s)
      AND cityHash64(session_id_v7) %% %(of_chunks)s = 0
    GROUP BY team_id, session_id_v7, session_timestamp
    HAVING max(interestingness_score) IS NULL
)
""".strip()


def insert_scores_sql(table: str = WRITABLE_RAW_SESSIONS_TABLE_V3()) -> str:
    """Return the partial-column INSERT used by `ch_insert_scores`.

    Only the ORDER BY key columns + `interestingness_score` are listed; every
    other column gets the engine's default. For `AggregateFunction(...)`
    columns the default is the empty aggregate state, which merges with the
    real existing state to leave the existing value untouched. Net effect:
    we patch the score onto the row without rewriting any feature column.
    """
    return f"""
INSERT INTO {table} (team_id, session_id_v7, session_timestamp, interestingness_score)
VALUES
""".strip()
