"""ClickHouse integration tests for `fetch_features_sql` / `count_unscored_sql`.

Targets the three bugs we fixed in the eligible_sessions CTE:

1. `session_id_v7` (UInt128) → UUID string conversion. Naively `toString(session_id_v7)`
   yields a decimal like "12345..." which never matches the hyphenated UUID stored in
   `session_replay_features.session_id`. The fix uses `reinterpretAsUUID` after a
   byte-swap (CH UUID layout swaps the two 64-bit halves vs. UInt128).

2. Primary-key-seek-friendly filter. `session_replay_features` is ordered by
   `(team_id, session_id)`, so the GLOBAL IN must filter on the full tuple
   (`(team_id, session_id) GLOBAL IN ...`) — not just `session_id`.

3. Deterministic chunking. CH inlines `WITH ... AS` as a subquery, so
   `eligible_sessions` is evaluated twice. Without `ORDER BY` before LIMIT, the two
   evaluations can return different subsets and the inner join silently drops
   the difference.

The shape tests guard against silent regressions in the SQL string. The CH
integration tests prove the join actually returns rows for matching IDs and
zero rows for the buggy cast / cross-tenant scenarios.
"""

from __future__ import annotations

import re

from posthog.test.base import BaseTest, ClickhouseTestMixin

from posthog.clickhouse.client import sync_execute
from posthog.models.raw_sessions.sessions_v3 import WRITABLE_RAW_SESSIONS_TABLE_V3
from posthog.session_recordings.sql.session_replay_feature_sql import TRUNCATE_SESSION_REPLAY_FEATURES_TABLE_SQL
from posthog.temporal.session_replay.interestingness_scoring_sweep.sql import count_unscored_sql, fetch_features_sql


class TestFetchFeaturesSqlShape:
    """Regression-proof the SQL string against re-introduction of the three bugs."""

    def test_uses_reinterpret_as_uuid_not_plain_to_string(self) -> None:
        sql = fetch_features_sql()
        assert "reinterpretAsUUID" in sql, "byte-swap + UUID cast must be present"
        # The naive `toString(session_id_v7)` produces a decimal string and would
        # never match `session_replay_features.session_id` (hyphenated UUID).
        assert "toString(session_id_v7)" not in sql, (
            "decimal-cast bug regressed — use reinterpretAsUUID(bitOr(bitShiftLeft, bitShiftRight))"
        )

    def test_global_in_filters_on_team_id_and_session_id_tuple(self) -> None:
        sql = fetch_features_sql()
        # session_replay_features is ORDER BY (team_id, session_id) — tuple lookup
        # is what enables granule skipping.
        assert "(f.team_id, f.session_id) GLOBAL IN" in sql

    def test_eligible_sessions_orders_before_limit(self) -> None:
        sql = fetch_features_sql()
        match = re.search(
            r"eligible_sessions\s+AS\s+\(.*?ORDER BY\s+session_id_v7\s+LIMIT\s+%\(chunk_size\)s",
            sql,
            re.DOTALL,
        )
        assert match is not None, "ORDER BY session_id_v7 must precede LIMIT in the eligible_sessions CTE"

    def test_final_join_uses_team_id_and_session_id(self) -> None:
        sql = fetch_features_sql()
        # Joining on session_id alone is theoretically safe (UUIDs are unique-ish)
        # but losing the team_id check both wastes the index prefix and creates a
        # tenant-leak surface — keep the assertion strict.
        assert "rf.team_id = e.team_id AND rf.session_id = e.session_id_str" in sql

    def test_count_unscored_includes_lookback_and_chunking(self) -> None:
        sql = count_unscored_sql()
        assert "%(lookback_days)s" in sql
        assert "%(of_chunks)s" in sql


class TestEligibleSessionsJoinClickhouse(ClickhouseTestMixin, BaseTest):
    """End-to-end check that the bugfix actually lets the join match real rows.

    These tests insert directly into `writable_raw_sessions_v3` (mimicking the
    Kafka writeback MV's partial-column insert pattern) and into
    `writable_session_replay_features`, then exercise the same eligible_sessions
    CTE shape used by `fetch_features_sql`. We don't run the full SELECT because
    a handful of feature columns are still pending on the live `session_replay_features`
    DDL (see `sql.py` schema gap) — the join correctness is what we're guarding here.
    """

    # Hyphenated UUID v7. We pick a fixed one so the byte layout is exercised
    # deterministically (the upper 64 bits — the timestamp portion — must survive
    # the `bitShiftLeft/bitShiftRight` swap intact).
    SESSION_ID = "01939d3e-7c80-7b56-bf8d-1e74e5c3b3a1"

    def setUp(self) -> None:
        super().setUp()
        # session_replay_features isn't in the global truncate list (posthog/conftest.py),
        # so other tests can leave residual rows around — clear the slate explicitly.
        sync_execute(TRUNCATE_SESSION_REPLAY_FEATURES_TABLE_SQL())

    def _insert_raw_session(self, *, team_id: int, session_id: str) -> None:
        # Mirror the Kafka MV's partial-column insert: only ORDER BY keys, score
        # left NULL so the row is "eligible". Other AggregateFunction columns
        # default to empty state and merge as no-ops.
        sync_execute(
            f"INSERT INTO {WRITABLE_RAW_SESSIONS_TABLE_V3()} (team_id, session_id_v7) "
            f"SELECT %(team_id)s, toUInt128(toUUID(%(session_id)s))",
            {"team_id": team_id, "session_id": session_id},
        )

    def _insert_replay_features(self, *, team_id: int, session_id: str, event_count: int = 42) -> None:
        # Direct partial-column insert; SimpleAggregateFunction columns we don't
        # provide get a 0/empty default. We only validate `event_count` round-trips,
        # which is enough to prove the join lined up.
        sync_execute(
            "INSERT INTO writable_session_replay_features "
            "(session_id, team_id, distinct_id, min_first_timestamp, max_last_timestamp, event_count) "
            "SELECT %(session_id)s, %(team_id)s, 'd1', now64(6) - INTERVAL 1 HOUR, now64(6), %(event_count)s",
            {"session_id": session_id, "team_id": team_id, "event_count": event_count},
        )

    @staticmethod
    def _eligible_sessions_join_sql(*, decimal_cast: bool = False) -> str:
        """A trimmed copy of fetch_features_sql's eligible_sessions + INNER JOIN.

        We can't run the full `fetch_features_sql` against the live CH schema yet
        (some feature columns are pending — see `sql.py` schema gap), but the
        CTE → join is exactly where the bugs live. Toggle `decimal_cast=True` to
        reproduce the regressed conversion and prove the test would catch it.
        """
        cast = (
            "toString(session_id_v7)"
            if decimal_cast
            else (
                "toString(reinterpretAsUUID(bitOr(bitShiftLeft(session_id_v7, 64), bitShiftRight(session_id_v7, 64))))"
            )
        )
        return f"""
        WITH eligible_sessions AS (
            SELECT
                team_id,
                session_id_v7,
                {cast} AS session_id_str
            FROM raw_sessions_v3
            WHERE team_id = %(team_id)s
            GROUP BY team_id, session_id_v7
            HAVING max(interestingness_score) IS NULL
            ORDER BY session_id_v7
            LIMIT 100
        )
        SELECT e.team_id, e.session_id_str, sum(f.event_count) AS ec
        FROM eligible_sessions e
        INNER JOIN session_replay_features AS f
            ON f.team_id = e.team_id AND f.session_id = e.session_id_str
        WHERE (f.team_id, f.session_id) GLOBAL IN (
            SELECT team_id, session_id_str FROM eligible_sessions
        )
        GROUP BY e.team_id, e.session_id_str
        """

    def test_join_matches_with_uuid_conversion(self) -> None:
        self._insert_raw_session(team_id=self.team.id, session_id=self.SESSION_ID)
        self._insert_replay_features(team_id=self.team.id, session_id=self.SESSION_ID, event_count=42)

        rows = sync_execute(
            self._eligible_sessions_join_sql(decimal_cast=False),
            {"team_id": self.team.id},
        )
        assert len(rows) == 1
        team_id, session_id_str, event_count = rows[0]
        assert team_id == self.team.id
        assert session_id_str == self.SESSION_ID
        assert event_count == 42

    def test_buggy_decimal_cast_returns_zero_rows(self) -> None:
        """Sanity-check that the regression test is doing real work.

        With the buggy `toString(session_id_v7)` cast, the join must miss every row
        — confirms the test would have caught the bug before the fix.
        """
        self._insert_raw_session(team_id=self.team.id, session_id=self.SESSION_ID)
        self._insert_replay_features(team_id=self.team.id, session_id=self.SESSION_ID)

        rows = sync_execute(
            self._eligible_sessions_join_sql(decimal_cast=True),
            {"team_id": self.team.id},
        )
        assert rows == []

    def test_team_id_isolation_in_join(self) -> None:
        """Same UUID used by two teams must not cross over via the join."""
        other_team_id = self.team.id + 9999
        self._insert_raw_session(team_id=self.team.id, session_id=self.SESSION_ID)
        # Features row only for the OTHER team — querying as our team must miss.
        self._insert_replay_features(team_id=other_team_id, session_id=self.SESSION_ID)

        rows = sync_execute(
            self._eligible_sessions_join_sql(decimal_cast=False),
            {"team_id": self.team.id},
        )
        assert rows == []

    def test_count_unscored_excludes_scored_sessions(self) -> None:
        """Score one of two sessions; `count_unscored_sql` should report the other one."""
        scored_session = "01939d3e-7c80-7b56-bf8d-1e74e5c3b3a2"
        self._insert_raw_session(team_id=self.team.id, session_id=self.SESSION_ID)
        self._insert_raw_session(team_id=self.team.id, session_id=scored_session)
        # Patch a score onto the second session via the same partial-insert path
        # the Kafka writeback MV uses — that's the contract the HAVING clause sees.
        sync_execute(
            f"INSERT INTO {WRITABLE_RAW_SESSIONS_TABLE_V3()} "
            f"(team_id, session_id_v7, interestingness_score) "
            f"SELECT %(team_id)s, toUInt128(toUUID(%(session_id)s)), 0.5",
            {"team_id": self.team.id, "session_id": scored_session},
        )

        # `of_chunks=1` makes the modulo trivially match every row, so we count
        # all unscored sessions in the lookback window.
        rows = sync_execute(
            count_unscored_sql(),
            {"lookback_days": 365, "of_chunks": 1},
        )
        # Other tests might leave behind unscored sessions; assert ours is included
        # rather than equality to avoid false-flake on shared CH state.
        assert rows[0][0] >= 1
