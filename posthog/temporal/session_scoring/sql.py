"""ClickHouse statements for the session-scoring pipeline.

The serving SELECT mirrors the training query:

    1. `eligible_sessions` — hash-partitioned slice of unscored sessions from
       `raw_sessions_v3` (the table the score is written back to).
       `HAVING max(interestingness_score) IS NULL` is the unscored filter.

    2. `aggregated_sufficient_statistics` — pulls raw aggregates from
       `session_replay_features` for those session IDs, mirroring the
       training query verbatim. The `f.session_id GLOBAL IN (...)` form
       streams the eligible-session set to every shard so each shard only
       scans its locally-resident replay rows.

    3. `replay_features` — derives the rates/ratios/stats the model was
       trained on. Same expressions as the training query.

    4. Final SELECT — joins `replay_features` back to `eligible_sessions`
       (inner join: sessions without replay features are dropped and stay
       NULL in raw_sessions_v3 — they re-appear on the next tick within the
       lookback window, then naturally fall out once they age past it).

`INSERT_SCORES_SQL` writes the score back to `writable_raw_sessions_v3` as
a partial-column insert; AggregatingMergeTree merges it onto the existing
session row without disturbing any other column (every other AggregateFunction
column gets the engine's empty state, which is a merge no-op).

Schema note: a handful of columns referenced by the training query do not yet
exist on `session_replay_features` (`scroll_to_top_count`, `backspace_count`,
`long_idle_gap_count`, `console_warn_count`, `network_4xx_count`,
`network_5xx_count`, `mutation_count`, `viewport_resize_count`,
`touch_event_count`, `selection_copy_count`, every `*_path_visit_count`, and
`unique_form_field_count`). These need to be added to the table + Kafka MV
before this pipeline can run end-to-end. See README.md.
"""

from posthog.models.raw_sessions.sessions_v3 import DISTRIBUTED_RAW_SESSIONS_TABLE_V3, WRITABLE_RAW_SESSIONS_TABLE_V3

# Distributed `session_replay_features` table name. Hardcoded because there is
# no Python helper for it; the schema is in
# posthog/session_recordings/sql/session_replay_feature_sql.py.
SESSION_REPLAY_FEATURES_TABLE = "session_replay_features"


# --------------------------------------------------------------------------- #
# Aggregate fragment over `session_replay_features`.                           #
# Identical column-by-column to the training query's CTE.                      #
# --------------------------------------------------------------------------- #
_AGGREGATED_STATS_FRAGMENT = """
SELECT
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
    sum(f.scroll_to_top_count)                      AS scroll_to_top_count,
    sum(f.backspace_count)                          AS backspace_count,
    sum(f.long_idle_gap_count)                      AS long_idle_gap_count,
    sum(f.console_warn_count)                       AS console_warn_count,
    sum(f.network_4xx_count)                        AS network_4xx_count,
    sum(f.network_5xx_count)                        AS network_5xx_count,
    sum(f.mutation_count)                           AS mutation_count,
    sum(f.viewport_resize_count)                    AS viewport_resize_count,
    sum(f.touch_event_count)                        AS touch_event_count,
    sum(f.selection_copy_count)                     AS selection_copy_count,
    sum(f.login_path_visit_count)                   AS login_path_visit_count,
    sum(f.signup_path_visit_count)                  AS signup_path_visit_count,
    sum(f.checkout_path_visit_count)                AS checkout_path_visit_count,
    sum(f.cart_path_visit_count)                    AS cart_path_visit_count,
    sum(f.billing_path_visit_count)                 AS billing_path_visit_count,
    sum(f.settings_path_visit_count)                AS settings_path_visit_count,
    sum(f.account_path_visit_count)                 AS account_path_visit_count,
    sum(f.error_path_visit_count)                   AS error_path_visit_count,
    sum(f.not_found_path_visit_count)               AS not_found_path_visit_count,
    sum(f.admin_path_visit_count)                   AS admin_path_visit_count,
    sum(f.dashboard_path_visit_count)               AS dashboard_path_visit_count,
    sum(f.onboarding_path_visit_count)              AS onboarding_path_visit_count,
    sum(f.cancel_path_visit_count)                  AS cancel_path_visit_count,
    sum(f.refund_path_visit_count)                  AS refund_path_visit_count,
    uniqExactMerge(f.unique_url_count)              AS unique_urls,
    uniqExactMerge(f.unique_click_target_count)     AS unique_click_targets,
    uniqExactMerge(f.unique_form_field_count)       AS unique_form_fields
FROM {features_table} AS f
WHERE f.session_id GLOBAL IN (SELECT session_id_str FROM eligible_sessions)
  AND f.min_first_timestamp >= now() - toIntervalDay(%(lookback_days)s)
GROUP BY f.session_id
""".strip()


# --------------------------------------------------------------------------- #
# Derived feature fragment over the aggregated stats CTE.                      #
# Identical column-by-column to the training query's `replay_features` CTE.    #
# --------------------------------------------------------------------------- #
_REPLAY_FEATURES_FRAGMENT = """
SELECT
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
    f.network_4xx_count            / nullIf(f.network_request_count, 0)          AS network_4xx_ratio,
    f.network_5xx_count            / nullIf(f.network_request_count, 0)          AS network_5xx_ratio,
    f.scroll_to_top_count          / nullIf(f.session_duration_s, 0)             AS scroll_to_top_rate,
    f.backspace_count              / nullIf(f.keypress_count, 0)                 AS backspace_ratio,
    f.long_idle_gap_count,
    f.console_warn_count           / nullIf(f.session_duration_s, 0)             AS console_warn_rate,
    f.mutation_count               / nullIf(f.session_duration_s, 0)             AS mutation_rate,
    f.viewport_resize_count,
    f.touch_event_count            / nullIf(f.session_duration_s, 0)             AS touch_event_rate,
    f.selection_copy_count,
    f.login_path_visit_count,
    f.signup_path_visit_count,
    f.checkout_path_visit_count,
    f.cart_path_visit_count,
    f.billing_path_visit_count,
    f.settings_path_visit_count,
    f.account_path_visit_count,
    f.error_path_visit_count,
    f.not_found_path_visit_count,
    f.admin_path_visit_count,
    f.dashboard_path_visit_count,
    f.onboarding_path_visit_count,
    f.cancel_path_visit_count,
    f.refund_path_visit_count,
    f.unique_urls,
    f.unique_click_targets,
    f.unique_form_fields,
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
    feature columns in `features.FEATURE_NAMES` order. Row count <= chunk_size,
    minus any sessions that have no replay features (inner-joined out).
    """
    raw_table = raw_sessions_table or DISTRIBUTED_RAW_SESSIONS_TABLE_V3()
    return f"""
WITH eligible_sessions AS (
    SELECT
        team_id,
        session_id_v7,
        session_timestamp,
        toString(session_id_v7) AS session_id_str
    FROM {raw_table}
    WHERE session_timestamp >= now() - toIntervalDay(%(lookback_days)s)
      AND cityHash64(session_id_v7) %% %(of_chunks)s = %(chunk_id)s
    GROUP BY team_id, session_id_v7, session_timestamp
    HAVING max(interestingness_score) IS NULL
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
    rf.network_4xx_ratio,
    rf.network_5xx_ratio,
    rf.scroll_to_top_rate,
    rf.backspace_ratio,
    rf.long_idle_gap_count,
    rf.console_warn_rate,
    rf.mutation_rate,
    rf.viewport_resize_count,
    rf.touch_event_rate,
    rf.selection_copy_count,
    rf.login_path_visit_count,
    rf.signup_path_visit_count,
    rf.checkout_path_visit_count,
    rf.cart_path_visit_count,
    rf.billing_path_visit_count,
    rf.settings_path_visit_count,
    rf.account_path_visit_count,
    rf.error_path_visit_count,
    rf.not_found_path_visit_count,
    rf.admin_path_visit_count,
    rf.dashboard_path_visit_count,
    rf.onboarding_path_visit_count,
    rf.cancel_path_visit_count,
    rf.refund_path_visit_count,
    rf.unique_urls,
    rf.unique_click_targets,
    rf.unique_form_fields,
    rf.page_revisit_count
FROM eligible_sessions e
INNER JOIN replay_features rf ON rf.session_id = e.session_id_str
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


def insert_scores_sql(table: str | None = None) -> str:
    """Return the partial-column INSERT used to write scores back to raw_sessions_v3.

    Only the ORDER BY key columns + `interestingness_score` are listed; every
    other column gets its engine default. For `AggregateFunction(...)` columns
    the default is the empty aggregate state, which merges with the existing
    state to leave the value untouched. Net effect: we patch the score onto
    the row without rewriting any feature column.
    """
    target = table or WRITABLE_RAW_SESSIONS_TABLE_V3()
    return f"""
INSERT INTO {target} (team_id, session_id_v7, session_timestamp, interestingness_score)
VALUES
""".strip()
