# Testing ClickHouse Migrations

This guide covers how to test ClickHouse migrations before deploying them to production.

## Unit Testing Migrations

Each migration should have a corresponding test that verifies:
1. The migration applies cleanly
2. Data integrity is maintained
3. The rollback works correctly (if applicable)

### Test Structure

```python
import pytest
from posthog.clickhouse.client import sync_execute
from posthog.test.base import BaseTest


class TestMyMigration(BaseTest):
    """
    Tests for migration: 0042_add_session_duration_column
    """

    def setUp(self):
        super().setUp()
        # Ensure we start with a clean state
        self._drop_test_table_if_exists()

    def tearDown(self):
        super().tearDown()
        self._drop_test_table_if_exists()

    def _drop_test_table_if_exists(self):
        sync_execute("DROP TABLE IF EXISTS test_session_recording_events")

    def test_migration_applies_cleanly(self):
        """Verify the migration DDL executes without errors."""
        from posthog.clickhouse.migrations.0042_add_session_duration_column import Migration

        migration = Migration()
        # Should not raise
        migration.operations[0].forward()

    def test_new_column_has_correct_type(self):
        """Verify the new column exists with the expected type."""
        result = sync_execute(
            """
            SELECT name, type
            FROM system.columns
            WHERE table = 'session_recording_events'
              AND database = currentDatabase()
              AND name = 'session_duration'
            """
        )
        assert len(result) == 1
        col_name, col_type = result[0]
        assert col_name == "session_duration"
        assert "Nullable(Float64)" in col_type

    def test_existing_rows_have_null_default(self):
        """Existing rows should have NULL for the new column."""
        # Insert a row without the new column
        sync_execute(
            """
            INSERT INTO session_recording_events
            (uuid, timestamp, team_id, distinct_id, session_id)
            VALUES
            ('test-uuid-001', now(), 1, 'user1', 'sess-001')
            """
        )
        result = sync_execute(
            """
            SELECT session_duration
            FROM session_recording_events
            WHERE uuid = 'test-uuid-001'
            """
        )
        assert result[0][0] is None
```

## Integration Testing

For migrations that change query behavior, add integration tests:

```python
def test_queries_using_new_column(self):
    """Verify queries that use the new column work correctly."""
    sync_execute(
        """
        INSERT INTO session_recording_events
        (uuid, timestamp, team_id, distinct_id, session_id, session_duration)
        VALUES
        ('test-uuid-002', now(), 1, 'user2', 'sess-002', 42.5)
        """
    )
    result = sync_execute(
        """
        SELECT avg(session_duration)
        FROM session_recording_events
        WHERE team_id = 1
          AND session_duration IS NOT NULL
        """
    )
    assert abs(result[0][0] - 42.5) < 0.001
```

## Running Migration Tests Locally

```bash
# Run all migration tests
python -m pytest posthog/clickhouse/migrations/tests/ -v

# Run a specific migration test
python -m pytest posthog/clickhouse/migrations/tests/test_0042_add_session_duration_column.py -v

# Run with ClickHouse connection details
CLICKHOUSE_HOST=localhost \
CLICKHOUSE_DATABASE=posthog_test \
python -m pytest posthog/clickhouse/migrations/tests/ -v
```

## Checklist Before Merging

- [ ] Migration applies on a fresh database
- [ ] Migration applies on a database with existing data
- [ ] Rollback migration works (if `backward` is defined)
- [ ] No full table rewrites on large tables (check `EXPLAIN` output)
- [ ] Column defaults are sensible for existing rows
- [ ] Indexes are created `IF NOT EXISTS`
- [ ] Test added to `posthog/clickhouse/migrations/tests/`
