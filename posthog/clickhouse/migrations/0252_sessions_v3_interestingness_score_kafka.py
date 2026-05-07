from posthog.clickhouse.client.connection import NodeRole
from posthog.clickhouse.client.migration_tools import run_sql_with_exceptions
from posthog.models.raw_sessions.sessions_v3_score_kafka import (
    KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE_SQL,
    RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV_SQL,
)

# Wires the Temporal session interestingness scoring pipeline into ClickHouse:
# Producer (Temporal worker) -> Kafka topic -> kafka_raw_sessions_v3_interestingness_score
# (Kafka engine table) -> raw_sessions_v3_interestingness_score_mv -> writable_raw_sessions_v3.
#
# Both objects live on the same nodes as the rest of the raw_sessions_v3 pipeline
# (NodeRole.DATA), matching every prior 01[4-9]x_sessions_v3_*.py migration.
operations = [
    run_sql_with_exceptions(
        KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE_SQL(),
        node_roles=[NodeRole.DATA],
    ),
    run_sql_with_exceptions(
        RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV_SQL(),
        node_roles=[NodeRole.DATA],
    ),
]
