"""Tests for the Kafka writeback path in `score_chunk_activity`.

`_publish_scores` is the producer-side glue that turns the in-memory predictions
into JSONEachRow Kafka messages on
`KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE`. We verify both pieces
of the contract:

1. Payload shape matches what the CH Kafka engine table + MV expect (column
   names, types, `session_id_v7` as decimal-string for uint128 safety).
2. Per-row produce + final flush — the activity must not return until every
   message has been ack'd by the broker, otherwise the workflow happily reports
   `scored=N` while messages are still buffered in librdkafka.
"""

from __future__ import annotations

import pytest
from unittest import mock

import numpy as np
import pandas as pd
from parameterized import parameterized

from posthog.kafka_client.topics import KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE
from posthog.models.raw_sessions.sessions_v3_score_kafka import INSERT_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_SQL
from posthog.temporal.session_replay.interestingness_scoring_sweep.activities import _publish_scores


@pytest.fixture
def id_frame() -> pd.DataFrame:
    """A minimal frame with the three ID columns `_publish_scores` reads from."""
    return pd.DataFrame(
        {
            # Mix of small and uint128-sized session IDs to exercise the str() cast.
            "team_id": [1, 2, 42],
            "session_id_v7": [
                12345,
                2**64 + 1,
                2**127 - 1,
            ],
            # Not consumed by `_publish_scores` directly but kept for parity with
            # the production frame (writable_raw_sessions_v3 derives it via DEFAULT).
            "session_timestamp": pd.to_datetime(["2026-05-07 10:00:00", "2026-05-07 10:01:00", "2026-05-07 10:02:00"]),
        }
    )


class TestPublishScores:
    def test_no_rows_skips_flush(self) -> None:
        empty = pd.DataFrame({"team_id": [], "session_id_v7": [], "session_timestamp": []})
        with mock.patch(
            "posthog.temporal.session_replay.interestingness_scoring_sweep.activities.get_producer"
        ) as get_producer_mock:
            published = _publish_scores(empty, np.empty(0, dtype=np.float32))
            assert published == 0
            # No rows -> no flush call. Avoids creating the singleton producer
            # in unit tests that don't exercise the topic.
            get_producer_mock.assert_not_called()

    def test_publishes_one_message_per_row_and_flushes_once(self, id_frame: pd.DataFrame) -> None:
        scores = np.array([0.1, 0.5, 0.9], dtype=np.float32)
        with (
            mock.patch(
                "posthog.temporal.session_replay.interestingness_scoring_sweep.activities.ClickhouseProducer"
            ) as producer_cls_mock,
            mock.patch(
                "posthog.temporal.session_replay.interestingness_scoring_sweep.activities.get_producer"
            ) as get_producer_mock,
        ):
            producer_instance = producer_cls_mock.return_value
            published = _publish_scores(id_frame, scores)

            assert published == 3
            assert producer_instance.produce.call_count == 3

            # Topic + SQL fixed across all rows.
            for call in producer_instance.produce.call_args_list:
                assert call.kwargs["topic"] == KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE
                assert call.kwargs["sql"] == INSERT_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE_SQL

            payloads = [call.kwargs["data"] for call in producer_instance.produce.call_args_list]
            assert payloads == [
                {
                    "team_id": 1,
                    "session_id_v7": "12345",
                    "interestingness_score": pytest.approx(0.1, rel=1e-5),
                },
                {
                    "team_id": 2,
                    "session_id_v7": str(2**64 + 1),
                    "interestingness_score": pytest.approx(0.5, rel=1e-5),
                },
                {
                    "team_id": 42,
                    "session_id_v7": str(2**127 - 1),
                    "interestingness_score": pytest.approx(0.9, rel=1e-5),
                },
            ]

            # Exactly one flush after the loop, scoped to our topic.
            get_producer_mock.assert_called_once_with(topic=KAFKA_CLICKHOUSE_RAW_SESSIONS_V3_INTERESTINGNESS_SCORE)
            get_producer_mock.return_value.flush.assert_called_once()

    @parameterized.expand(
        [
            ("smallest_uint128", 0, "0"),
            ("max_uint64", 2**64 - 1, str(2**64 - 1)),
            ("just_above_uint64", 2**64, str(2**64)),
            ("max_uint128", 2**128 - 1, str(2**128 - 1)),
        ]
    )
    def test_session_id_is_serialized_as_decimal_string(self, _label: str, raw_id: int, expected_str: str) -> None:
        # uint128 ids must be cast to decimal strings so JSON doesn't lose
        # precision; the CH MV does `toUInt128(session_id_v7)` to invert it.
        df = pd.DataFrame(
            {
                "team_id": [7],
                "session_id_v7": [raw_id],
                "session_timestamp": pd.to_datetime(["2026-05-07 10:00:00"]),
            }
        )
        with (
            mock.patch(
                "posthog.temporal.session_replay.interestingness_scoring_sweep.activities.ClickhouseProducer"
            ) as producer_cls_mock,
            mock.patch("posthog.temporal.session_replay.interestingness_scoring_sweep.activities.get_producer"),
        ):
            _publish_scores(df, np.array([0.42], dtype=np.float32))

            (call,) = producer_cls_mock.return_value.produce.call_args_list
            assert call.kwargs["data"]["session_id_v7"] == expected_str
            assert isinstance(call.kwargs["data"]["session_id_v7"], str)

    def test_score_dtype_is_python_float(self, id_frame: pd.DataFrame) -> None:
        # confluent-kafka-python's JSON serializer can't handle numpy scalars —
        # `float(score)` is the cast that protects us. Pin the behavior so a
        # future "optimization" doesn't pass np.float32 straight through.
        scores = np.array([0.1, 0.5, 0.9], dtype=np.float32)
        with (
            mock.patch(
                "posthog.temporal.session_replay.interestingness_scoring_sweep.activities.ClickhouseProducer"
            ) as producer_cls_mock,
            mock.patch("posthog.temporal.session_replay.interestingness_scoring_sweep.activities.get_producer"),
        ):
            _publish_scores(id_frame, scores)
            for call in producer_cls_mock.return_value.produce.call_args_list:
                assert isinstance(call.kwargs["data"]["interestingness_score"], float)
                assert not isinstance(call.kwargs["data"]["interestingness_score"], np.floating)

    def test_flush_uses_configured_timeout(self, id_frame: pd.DataFrame) -> None:
        from posthog.temporal.session_replay.interestingness_scoring_sweep.constants import (
            KAFKA_PRODUCE_FLUSH_TIMEOUT_S,
        )

        with (
            mock.patch("posthog.temporal.session_replay.interestingness_scoring_sweep.activities.ClickhouseProducer"),
            mock.patch(
                "posthog.temporal.session_replay.interestingness_scoring_sweep.activities.get_producer"
            ) as get_producer_mock,
        ):
            _publish_scores(id_frame, np.array([0.1, 0.2, 0.3], dtype=np.float32))
            get_producer_mock.return_value.flush.assert_called_once_with(timeout=KAFKA_PRODUCE_FLUSH_TIMEOUT_S)
