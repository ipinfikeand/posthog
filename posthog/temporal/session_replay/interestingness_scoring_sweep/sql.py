"""ClickHouse statements for the session interestingness scoring pipeline.

The serving SELECT mirrors the training query:

    1. `eligible_sessions` — hash-partitioned slice of unscored sessions from
       `raw_sessions_v3` (the table the score is written back to).
       `HAVING max(interestingness_score) IS NULL` is the unscored filter.
       `ORDER BY session_id_v7 LIMIT chunk_size` makes the chunk
       deterministic across the two CTE evaluations (see note below).

       The `session_id_str` cast converts `raw_sessions_v3.session_id_v7`
       (UInt128, ClickHouse UUID layout = two 64-bit halves swapped) back
       to the canonical hyphenated UUID string used by
       `session_replay_features.session_id`. Without the byte-swap we
       produce a decimal string that never matches anything in features.

    2. `aggregated_sufficient_statistics` — pulls raw aggregates from
       `session_replay_features` for those sessions, mirroring the
       training query. We filter via `(team_id, session_id) GLOBAL IN ...`
       so the lookup hits the (team_id, session_id) primary key on
       `session_replay_features` for index-friendly granule skipping
       instead of a full partition scan, and `GLOBAL IN` ships the
       eligible-session set as a temp table to every shard so each shard
       only scans its locally-resident replay rows.

    3. `replay_features` — derives the rates/ratios/stats the model was
       trained on. Same expressions as the training query. Carries
       `team_id` through so the final join is on the same primary-key
       prefix and can't accidentally cross tenants.

    4. Final SELECT — joins `replay_features` back to `eligible_sessions`
       on `(team_id, session_id)` (inner join: sessions without replay
       features are dropped and stay NULL in raw_sessions_v3 — they
       re-appear on the next tick within the lookback window, then
       naturally fall out once they age past it).

CTE evaluation note: ClickHouse inlines `WITH ... AS` as a subquery — it
does not materialize the CTE once and reuse the result. `eligible_sessions`
is therefore evaluated twice (once for the GLOBAL IN subquery, once for
the final FROM). The `ORDER BY session_id_v7` before the LIMIT is what
keeps the two evaluations consistent; without it, two un-ordered LIMITs
of the same query are not guaranteed to return the same rows, and any
mismatch would be silently dropped by the inner join.

Score writeback flows through Kafka — see
`posthog.models.raw_sessions.sessions_v3_score_kafka` for the topic / Kafka
engine table / MV trio. The activity emits one JSONEachRow message per scored
session; CH consumes via the MV which performs a partial-column insert into
`writable_raw_sessions_v3`. The AggregatingMergeTree then merges the score
onto the existing session row without disturbing any other column.

Feature alignment contract: the final SELECT alias list must match the
booster's `feature_names` exactly (set + order). `feature_columns_in_select`
extracts the alias list at test time so a unit test can assert parity with
the bundled `model.ubj` before any chunk ever runs in CH. Drift = silently
mis-scored sessions, so we catch it at CI rather than at runtime.
"""

import re

from posthog.models.raw_sessions.sessions_v3 import DISTRIBUTED_RAW_SESSIONS_TABLE_V3

# Distributed `session_replay_features` table name. Hardcoded because there is
# no Python helper for it; the schema is in
# posthog/session_recordings/sql/session_replay_feature_sql.py.
SESSION_REPLAY_FEATURES_TABLE = "session_replay_features"


# --------------------------------------------------------------------------- #
# Aggregate fragment over `session_replay_features`.                           #
# Identical column-by-column to the training query's CTE.                      #
# Only references columns that exist on the live `session_replay_features`     #
# DDL today (see posthog/session_recordings/sql/session_replay_feature_sql.py).#
# --------------------------------------------------------------------------- #
_AGGREGATED_STATS_FRAGMENT = """
SELECT
    f.team_id,
    f.session_id,
    dateDiff('second', min(f.min_first_timestamp), max(f.max_last_timestamp)) AS session_duration_s,
    sum(f.event_count)                              AS event_count,
    sum(f.click_count)                              AS click_count,
    sum(f.keypress_count)                           AS keypress_count,
    sum(f.mouse_activity_count)                     AS mouse_activity_count,
    sum(f.rage_click_count)                         AS rage_click_count,
    sum(f.dead_click_count)                         AS dead_click_count,
    sum(f.quick_back_count)                         AS quick_back_count,
    sum(f.page_visit_count)                         AS page_visit_count,
    sum(f.text_selection_count)                     AS text_selection_count,
    sum(f.scroll_event_count)                       AS scroll_event_count,
    sum(f.console_error_count)                      AS console_error_count,
    sum(f.console_error_after_click_count)          AS console_error_after_click_count,
    sum(f.network_request_count)                    AS network_request_count,
    sum(f.network_failed_request_count)             AS network_failed_request_count,
    sum(f.mouse_position_count)                     AS mouse_position_count,
    sum(f.mouse_sum_x)                              AS mouse_sum_x,
    sum(f.mouse_sum_x_squared)                      AS mouse_sum_x_squared,
    sum(f.mouse_sum_y)                              AS mouse_sum_y,
    sum(f.mouse_sum_y_squared)                      AS mouse_sum_y_squared,
    sum(f.mouse_distance_traveled)                  AS mouse_distance_traveled,
    sum(f.mouse_direction_change_count)             AS mouse_direction_change_count,
    sum(f.mouse_velocity_sum)                       AS mouse_velocity_sum,
    sum(f.mouse_velocity_sum_of_squares)            AS mouse_velocity_sum_of_squares,
    sum(f.mouse_velocity_count)                     AS mouse_velocity_count,
    sum(f.total_scroll_magnitude)                   AS total_scroll_magnitude,
    sum(f.scroll_direction_reversal_count)          AS scroll_direction_reversal_count,
    sum(f.rapid_scroll_reversal_count)              AS rapid_scroll_reversal_count,
    max(f.max_scroll_y)                             AS max_scroll_y,
    sum(f.inter_action_gap_count)                   AS inter_action_gap_count,
    sum(f.inter_action_gap_sum_ms)                  AS inter_action_gap_sum_ms,
    sum(f.inter_action_gap_sum_of_squares_ms)       AS inter_action_gap_sum_of_squares_ms,
    max(f.max_idle_gap_ms)                          AS max_idle_gap_ms,
    sum(f.network_request_duration_sum)             AS network_request_duration_sum,
    sum(f.network_request_duration_sum_of_squares)  AS network_request_duration_sum_of_squares,
    sum(f.network_request_duration_count)           AS network_request_duration_count,
    uniqExactMerge(f.unique_url_count)              AS unique_urls,
    uniqExactMerge(f.unique_click_target_count)     AS unique_click_targets
FROM {features_table} AS f
WHERE (f.team_id, f.session_id) GLOBAL IN (SELECT team_id, session_id_str FROM eligible_sessions)
  AND f.min_first_timestamp >= now() - toIntervalDay(%(lookback_days)s)
GROUP BY f.team_id, f.session_id
""".strip()


# --------------------------------------------------------------------------- #
# Derived feature fragment over the aggregated stats CTE.                      #
# Identical column-by-column to the training query's `replay_features` CTE.    #
# --------------------------------------------------------------------------- #
_REPLAY_FEATURES_FRAGMENT = """
SELECT
    f.team_id,
    f.session_id,
    f.event_count                          / nullIf(f.session_duration_s, 0) AS event_rate,
    f.click_count                          / nullIf(f.session_duration_s, 0) AS click_rate,
    f.keypress_count                       / nullIf(f.session_duration_s, 0) AS keypress_rate,
    f.mouse_activity_count                 / nullIf(f.session_duration_s, 0) AS mouse_activity_rate,
    f.rage_click_count                     / nullIf(f.session_duration_s, 0) AS rage_click_rate,
    f.dead_click_count                     / nullIf(f.session_duration_s, 0) AS dead_click_rate,
    f.quick_back_count                     / nullIf(f.session_duration_s, 0) AS quick_back_rate,
    f.page_visit_count                     / nullIf(f.session_duration_s, 0) AS page_visit_rate,
    f.text_selection_count                 / nullIf(f.session_duration_s, 0) AS text_selection_rate,
    f.scroll_event_count                   / nullIf(f.session_duration_s, 0) AS scroll_event_rate,
    f.console_error_count                  / nullIf(f.session_duration_s, 0) AS console_error_rate,
    f.console_error_after_click_count      / nullIf(f.session_duration_s, 0) AS console_error_after_click_rate,
    f.network_request_count                / nullIf(f.session_duration_s, 0) AS network_request_rate,
    f.network_failed_request_count         / nullIf(f.session_duration_s, 0) AS network_failed_request_rate,
    f.mouse_sum_x / nullIf(f.mouse_position_count, 0) AS mouse_mean_x,
    f.mouse_sum_y / nullIf(f.mouse_position_count, 0) AS mouse_mean_y,
    sqrt(greatest(0, f.mouse_sum_x_squared / nullIf(f.mouse_position_count, 0)
                  - pow(f.mouse_sum_x   / nullIf(f.mouse_position_count, 0), 2))) AS mouse_stddev_x,
    sqrt(greatest(0, f.mouse_sum_y_squared / nullIf(f.mouse_position_count, 0)
                  - pow(f.mouse_sum_y   / nullIf(f.mouse_position_count, 0), 2))) AS mouse_stddev_y,
    f.mouse_distance_traveled              / nullIf(f.session_duration_s, 0)         AS mouse_distance_per_s,
    f.mouse_direction_change_count         / nullIf(f.mouse_distance_traveled, 0)    AS mouse_direction_change_rate,
    f.mouse_velocity_sum / nullIf(f.mouse_velocity_count, 0) AS mouse_velocity_mean,
    sqrt(greatest(0, f.mouse_velocity_sum_of_squares / nullIf(f.mouse_velocity_count, 0)
                  - pow(f.mouse_velocity_sum     / nullIf(f.mouse_velocity_count, 0), 2))) AS mouse_velocity_stddev,
    f.total_scroll_magnitude               / nullIf(f.session_duration_s, 0)  AS scroll_magnitude_per_s,
    f.total_scroll_magnitude               / nullIf(f.scroll_event_count, 0)  AS scroll_magnitude_per_event,
    f.scroll_direction_reversal_count      / nullIf(f.session_duration_s, 0)  AS scroll_direction_reversal_rate,
    f.rapid_scroll_reversal_count          / nullIf(f.session_duration_s, 0)  AS rapid_scroll_reversal_rate,
    f.max_scroll_y,
    f.inter_action_gap_sum_ms              / nullIf(f.inter_action_gap_count, 0) AS inter_action_gap_mean_ms,
    sqrt(greatest(0, f.inter_action_gap_sum_of_squares_ms / nullIf(f.inter_action_gap_count, 0)
                  - pow(f.inter_action_gap_sum_ms     / nullIf(f.inter_action_gap_count, 0), 2))) AS inter_action_gap_stddev_ms,
    f.max_idle_gap_ms,
    f.network_request_duration_sum / nullIf(f.network_request_duration_count, 0) AS network_request_duration_mean_ms,
    sqrt(greatest(0, f.network_request_duration_sum_of_squares / nullIf(f.network_request_duration_count, 0)
                  - pow(f.network_request_duration_sum     / nullIf(f.network_request_duration_count, 0), 2))) AS network_request_duration_stddev_ms,
    f.network_failed_request_count / nullIf(f.network_request_count, 0)          AS network_failure_ratio,
    f.unique_urls,
    f.unique_click_targets,
    greatest(0, f.page_visit_count - f.unique_urls) AS page_revisit_count
FROM aggregated_sufficient_statistics f
""".strip()


def fetch_features_sql(
    raw_sessions_table: str | None = None,
    features_table: str = SESSION_REPLAY_FEATURES_TABLE,
) -> str:
    """Return the parameterized SELECT used by `score_chunk_activity`.

    Bound parameters: %(of_chunks)s, %(chunk_id)s, %(lookback_days)s, %(chunk_size)s.

    Returned columns: `team_id`, `session_id_v7`, `session_timestamp`, then the
    feature columns. The SELECT alias list must match the booster's
    `feature_names` (= `scorer.get_feature_names()`); `validate_features`
    enforces this on every chunk. Row count <= chunk_size, minus any
    sessions that have no replay features (inner-joined out).
    """
    raw_table = raw_sessions_table or DISTRIBUTED_RAW_SESSIONS_TABLE_V3()
    return f"""
WITH eligible_sessions AS (
    SELECT
        team_id,
        session_id_v7,
        session_timestamp,
        -- session_replay_features.session_id is a hyphenated UUID string
        -- (e.g. "01939d3e-7c80-7b56-bf8d-1e74e5c3b3a1"); raw_sessions_v3
        -- stores it as the equivalent UInt128 with the two halves swapped
        -- (CH UUID layout). Reverse the byte-swap and reinterpret to get
        -- back the canonical UUID string for the join.
        toString(reinterpretAsUUID(
            bitOr(bitShiftLeft(session_id_v7, 64), bitShiftRight(session_id_v7, 64))
        )) AS session_id_str
    FROM {raw_table}
    WHERE session_timestamp >= now() - toIntervalDay(%(lookback_days)s)
      AND cityHash64(session_id_v7) %% %(of_chunks)s = %(chunk_id)s
    GROUP BY team_id, session_id_v7, session_timestamp
    HAVING max(interestingness_score) IS NULL
    -- ORDER BY makes LIMIT deterministic across the two CTE evaluations
    -- (CH inlines CTEs as subqueries — without a stable order, the GLOBAL IN
    -- subquery and the final FROM could pick different subsets and the
    -- inner join would silently drop the difference).
    ORDER BY session_id_v7
    LIMIT %(chunk_size)s
),
aggregated_sufficient_statistics AS (
    {_AGGREGATED_STATS_FRAGMENT.format(features_table=features_table)}
),
replay_features AS (
    {_REPLAY_FEATURES_FRAGMENT}
)
SELECT
    e.team_id,
    e.session_id_v7,
    e.session_timestamp,
    rf.event_rate,
    rf.click_rate,
    rf.keypress_rate,
    rf.mouse_activity_rate,
    rf.rage_click_rate,
    rf.dead_click_rate,
    rf.quick_back_rate,
    rf.page_visit_rate,
    rf.text_selection_rate,
    rf.scroll_event_rate,
    rf.console_error_rate,
    rf.console_error_after_click_rate,
    rf.network_request_rate,
    rf.network_failed_request_rate,
    rf.mouse_mean_x,
    rf.mouse_mean_y,
    rf.mouse_stddev_x,
    rf.mouse_stddev_y,
    rf.mouse_distance_per_s,
    rf.mouse_direction_change_rate,
    rf.mouse_velocity_mean,
    rf.mouse_velocity_stddev,
    rf.scroll_magnitude_per_s,
    rf.scroll_magnitude_per_event,
    rf.scroll_direction_reversal_rate,
    rf.rapid_scroll_reversal_rate,
    rf.max_scroll_y,
    rf.inter_action_gap_mean_ms,
    rf.inter_action_gap_stddev_ms,
    rf.max_idle_gap_ms,
    rf.network_request_duration_mean_ms,
    rf.network_request_duration_stddev_ms,
    rf.network_failure_ratio,
    rf.unique_urls,
    rf.unique_click_targets,
    rf.page_revisit_count
FROM eligible_sessions e
INNER JOIN replay_features rf ON rf.team_id = e.team_id AND rf.session_id = e.session_id_str
""".strip()


def count_unscored_sql(raw_sessions_table: str | None = None) -> str:
    """Return a cheap COUNT estimate of unscored sessions in one hash bucket.

    Bound parameter: %(lookback_days)s, %(of_chunks)s.

    Sampling one bucket and extrapolating (multiply by `of_chunks` in the
    caller) is far cheaper than scanning all unscored sessions to decide
    whether to dispatch the tick.
    """
    raw_table = raw_sessions_table or DISTRIBUTED_RAW_SESSIONS_TABLE_V3()
    return f"""
SELECT count()
FROM (
    SELECT session_id_v7
    FROM {raw_table}
    WHERE session_timestamp >= now() - toIntervalDay(%(lookback_days)s)
      AND cityHash64(session_id_v7) %% %(of_chunks)s = 0
    GROUP BY team_id, session_id_v7, session_timestamp
    HAVING max(interestingness_score) IS NULL
)
""".strip()


# --------------------------------------------------------------------------- #
# Feature-alignment helper                                                     #
# --------------------------------------------------------------------------- #

# Matches a `<table_alias>.<column_name>` expression on its own line in the
# final SELECT (one column per line, optional trailing comma). The alias
# group `(\w+)` comes back as `e` for ID columns or `rf` for features —
# the caller filters by alias.
_SELECT_ALIAS_RE = re.compile(r"^\s*(\w+)\.(\w+)\s*,?\s*$", re.MULTILINE)


def feature_columns_in_select(sql: str, *, feature_table_alias: str = "rf") -> tuple[str, ...]:
    """Return the ordered tuple of feature column aliases from the final SELECT.

    Pure-string parser used by the SQL/booster parity test in
    `test_sql_alignment.py` — drift between this list and the booster's
    `feature_names` would silently mis-score sessions (validate_features
    would catch it at runtime, but the test catches it at CI before any
    deploy).

    Walks `fetch_features_sql()`'s output, extracts every line of the form
    `<feature_table_alias>.<name>` from after the last CTE close-paren, and
    returns them in source order. ID columns (alias `e.`) are ignored by
    matching only `feature_table_alias`. Returns the empty tuple if the
    final SELECT is malformed — the caller should treat that as a hard fail.
    """
    # Locate the body after the CTE block: everything from the final
    # `)\nSELECT` to the next `FROM`. Anchoring on the FROM keeps the parser
    # from accidentally picking up `<alias>.<col>` references that live in
    # ON clauses or aggregate args inside earlier CTEs.
    final_select = re.search(
        r"\)\s*SELECT\b(?P<body>.*?)\bFROM\s+eligible_sessions\b",
        sql,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not final_select:
        return ()
    body = final_select.group("body")
    return tuple(name for alias, name in _SELECT_ALIAS_RE.findall(body) if alias == feature_table_alias)
