from posthog.clickhouse.client.connection import NodeRole
from posthog.clickhouse.client.migration_tools import run_sql_with_exceptions
from posthog.models.raw_sessions.sessions_v3 import (
    DISTRIBUTED_RAW_SESSIONS_TABLE_V3,
    SHARDED_RAW_SESSIONS_TABLE_V3,
    WRITABLE_RAW_SESSIONS_TABLE_V3,
)
from posthog.models.raw_sessions.sessions_v3_score_kafka import (
    KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE_SQL,
    RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV_SQL,
)

# Wires the Temporal session interestingness scoring pipeline into ClickHouse:
# Producer (Temporal worker) -> Kafka topic -> kafka_raw_sessions_v3_interestingness_score
# (Kafka engine table) -> raw_sessions_v3_interestingness_score_mv -> writable_raw_sessions_v3.
#
# Order matters: the column must exist on every raw_sessions_v3 variant BEFORE the
# Kafka MV is created, otherwise CH validates the MV's INSERT target column list
# against the table schema and rejects the CREATE.
#
# Type is SimpleAggregateFunction(max, Nullable(Float32)):
#   - The base SimpleAggregateFunction matches the rest of the boolean/maxed columns
#     on the table (`has_replay_events`, `has_autocapture`).
#   - max() is null-safe in CH — null values are skipped — so the original score-less
#     row (inserted by the events MV before scoring runs) merges cleanly with the
#     scored row inserted by this Kafka pipeline.
#   - Re-scoring a session is idempotent for unchanged scores and "new model wins"
#     for higher scores; if you ever need true last-write-wins semantics, switch
#     to argMax(value, max_inserted_at) in a follow-up.
_INTERESTINGNESS_COLUMN_DDL = "interestingness_score SimpleAggregateFunction(max, Nullable(Float32))"

operations = [
    run_sql_with_exceptions(
        f"ALTER TABLE {SHARDED_RAW_SESSIONS_TABLE_V3()} ADD COLUMN IF NOT EXISTS {_INTERESTINGNESS_COLUMN_DDL}",
        node_roles=[NodeRole.DATA],
        sharded=True,
    ),
    run_sql_with_exceptions(
        f"ALTER TABLE {WRITABLE_RAW_SESSIONS_TABLE_V3()} ADD COLUMN IF NOT EXISTS {_INTERESTINGNESS_COLUMN_DDL}",
        node_roles=[NodeRole.DATA],
    ),
    run_sql_with_exceptions(
        f"ALTER TABLE {DISTRIBUTED_RAW_SESSIONS_TABLE_V3()} ADD COLUMN IF NOT EXISTS {_INTERESTINGNESS_COLUMN_DDL}",
        node_roles=[NodeRole.DATA],
    ),
    run_sql_with_exceptions(
        KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE_SQL(),
        node_roles=[NodeRole.DATA],
    ),
    run_sql_with_exceptions(
        RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV_SQL(),
        node_roles=[NodeRole.DATA],
    ),
]
