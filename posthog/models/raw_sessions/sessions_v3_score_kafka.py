"""Kafka writeback pipeline for the `interestingness_score` column on `raw_sessions_v3`.

Producers (Temporal `score_chunk_activity`) emit JSONEachRow messages of the form

    {
        "team_id": 42,
        "session_id_v7": "1234567890123456789012345678",  # uint128 as decimal string
        "interestingness_score": 0.42                      # nullable float
    }

`session_id_v7` is sent as a string because uint128 can exceed JSON-safe integer
precision; the materialized view casts it back via `toUInt128`. `session_timestamp`
is intentionally omitted — the writable table derives it from `session_id_v7`
through its `DEFAULT` expression, so the MV insert is genuinely partial-column.

Every other column on `writable_raw_sessions_v3` defaults to its engine's empty
`AggregateFunction` state, which merges as a no-op against the existing session
row in the AggregatingMergeTree. Net effect: we patch the score onto the row
without rewriting any feature column.
"""

from django.conf import settings

from posthog.clickhouse.kafka_engine import CONSUMER_GROUP_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE, kafka_engine
from posthog.kafka_client.topics import KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE
from posthog.models.raw_sessions.sessions_v3 import WRITABLE_RAW_SESSIONS_TABLE_V3

KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE = "kafka_raw_sessions_v3_interestingness_score"
RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV = "raw_sessions_v3_interestingness_score_mv"


def KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE_SQL() -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE}
(
    team_id Int64,
    session_id_v7 String,
    interestingness_score Nullable(Float32)
)
ENGINE = {
        kafka_engine(
            topic=KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE,
            group=CONSUMER_GROUP_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE,
        )
    }
""".strip()


def RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV_SQL() -> str:
    return f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV}
TO {settings.CLICKHOUSE_DATABASE}.{WRITABLE_RAW_SESSIONS_TABLE_V3()} (team_id, session_id_v7, interestingness_score)
AS SELECT
    team_id,
    toUInt128(session_id_v7) AS session_id_v7,
    interestingness_score
FROM {settings.CLICKHOUSE_DATABASE}.{KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE}
""".strip()


def DROP_KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE_SQL() -> str:
    return f"DROP TABLE IF EXISTS {KAFKA_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_TABLE}"


def DROP_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV_SQL() -> str:
    return f"DROP TABLE IF EXISTS {RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_MV}"


# Per-row INSERT used by the test fallback in `ClickhouseProducer.produce`. Mirrors
# what the Kafka MV does in production: writes only ORDER BY keys + the score, lets
# the writable table apply the `session_timestamp` DEFAULT expression. Bound params
# match the Kafka payload schema exactly so the test path and prod path stay aligned.
INSERT_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_SQL = f"""
INSERT INTO {WRITABLE_RAW_SESSIONS_TABLE_V3()} (team_id, session_id_v7, interestingness_score)
SELECT %(team_id)s, toUInt128(%(session_id_v7)s), %(interestingness_score)s
""".strip()
