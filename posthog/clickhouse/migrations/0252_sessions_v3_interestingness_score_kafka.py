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

# Adds interestingness_score to raw_sessions_v3 and wires the Kafka writeback pipeline
# (Temporal scorer -> topic -> Kafka engine table -> MV -> writable_raw_sessions_v3).
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
