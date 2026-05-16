# ClickHouse Migration Examples

This document provides concrete examples of common ClickHouse migration patterns used in PostHog.

## Adding a Column

```python
from posthog.clickhouse.migrations.base import Migration


class Migration(Migration):
    """
    Add a nullable String column to the events table.
    Safe to run on a live cluster — ALTER TABLE ADD COLUMN is non-blocking
    for MergeTree engines.
    """

    operations = [
        """
        ALTER TABLE sharded_events
        ON CLUSTER '{cluster}'
        ADD COLUMN IF NOT EXISTS my_new_column Nullable(String)
        """,
        """
        ALTER TABLE events
        ON CLUSTER '{cluster}'
        ADD COLUMN IF NOT EXISTS my_new_column Nullable(String)
        """,
    ]
```

## Adding a Materialized Column

Materialized columns are computed from existing columns at insert time and stored on disk.
Use them to avoid expensive runtime expressions in queries.

```python
from posthog.clickhouse.migrations.base import Migration


class Migration(Migration):
    """
    Add a materialized column that extracts a top-level property from the
    JSON properties blob so it can be filtered efficiently.

    NOTE: After adding the column you must run:
        OPTIMIZE TABLE events FINAL
    on each shard to backfill historical data, or accept that old rows will
    show the default value until they are merged.
    """

    operations = [
        """
        ALTER TABLE sharded_events
        ON CLUSTER '{cluster}'
        ADD COLUMN IF NOT EXISTS mat_browser
            VARCHAR MATERIALIZED trim(BOTH '"' FROM
                JSONExtractRaw(properties, '$browser'))
        """,
        """
        ALTER TABLE events
        ON CLUSTER '{cluster}'
        ADD COLUMN IF NOT EXISTS mat_browser
            VARCHAR MATERIALIZED trim(BOTH '"' FROM
                JSONExtractRaw(properties, '$browser'))
        """,
    ]
```

## Creating a New Table

```python
from posthog.clickhouse.migrations.base import Migration


class Migration(Migration):
    """
    Create a new ReplicatedReplacingMergeTree table for storing
    pre-aggregated session data.

    The distributed table (sessions) fans out queries across shards;
    the sharded table (sharded_sessions) holds the actual data.
    """

    operations = [
        # Sharded / local table
        """
        CREATE TABLE IF NOT EXISTS sharded_sessions ON CLUSTER '{cluster}'
        (
            team_id         Int64,
            session_id      VARCHAR,
            distinct_id     VARCHAR,
            start_time      DateTime64(6, 'UTC'),
            end_time        DateTime64(6, 'UTC'),
            page_count      Int64 DEFAULT 0,
            _timestamp      DateTime,
            _offset         UInt64
        )
        ENGINE = ReplicatedReplacingMergeTree(
            '/clickhouse/tables/{shard}/posthog.sessions',
            '{replica}',
            _timestamp
        )
        PARTITION BY toYYYYMM(start_time)
        ORDER BY (team_id, toDate(start_time), session_id)
        SETTINGS index_granularity = 8192
        """,
        # Distributed view
        """
        CREATE TABLE IF NOT EXISTS sessions ON CLUSTER '{cluster}'
        AS sharded_sessions
        ENGINE = Distributed('{cluster}', posthog, sharded_sessions, rand())
        """,
    ]
```

## Dropping a Column

Always use `IF EXISTS` and run the drop **after** all application code that
references the column has been removed and deployed.

```python
from posthog.clickhouse.migrations.base import Migration


class Migration(Migration):
    operations = [
        """
        ALTER TABLE sharded_events
        ON CLUSTER '{cluster}'
        DROP COLUMN IF EXISTS deprecated_column
        """,
        """
        ALTER TABLE events
        ON CLUSTER '{cluster}'
        DROP COLUMN IF EXISTS deprecated_column
        """,
    ]
```

## Adding an Index

See `references/database-indexes.md` in the `adding-personhog-rpc` skill for
general index guidance.  For ClickHouse, prefer skipping indexes only when
you have a concrete query pattern that benefits from them — they add write
overhead.

```python
from posthog.clickhouse.migrations.base import Migration


class Migration(Migration):
    """
    Add a bloom-filter skipping index on `session_id` so point-lookups
    by session can skip irrelevant granules.
    """

    operations = [
        """
        ALTER TABLE sharded_events
        ON CLUSTER '{cluster}'
        ADD INDEX IF NOT EXISTS idx_session_id session_id
            TYPE bloom_filter(0.01) GRANULARITY 1
        """,
        # Materialize the index for existing data
        """
        ALTER TABLE sharded_events
        ON CLUSTER '{cluster}'
        MATERIALIZE INDEX idx_session_id
        """,
    ]
```
