# ClickHouse Migration Patterns

This reference covers common patterns and best practices for writing ClickHouse migrations in PostHog.

## File Naming Convention

Migrations follow a sequential numbering scheme:

```
posthog/clickhouse/migrations/
  0001_events_table.py
  0002_person_distinct_id.py
  0003_add_session_id_column.py
```

Each migration file must define a `Migration` class with `operations` list.

## Basic Migration Structure

```python
from posthog.clickhouse.migrations.base import Migration, RunSQL

class Migration(Migration):
    operations = [
        RunSQL(
            """
            ALTER TABLE sharded_events
            ON CLUSTER '{cluster}'
            ADD COLUMN IF NOT EXISTS `$session_id` VARCHAR
            """,
            """
            ALTER TABLE sharded_events
            ON CLUSTER '{cluster}'
            DROP COLUMN IF EXISTS `$session_id`
            """,
        )
    ]
```

## Adding a Column

Always use `ADD COLUMN IF NOT EXISTS` to make migrations idempotent:

```python
RunSQL(
    """
    ALTER TABLE sharded_events
    ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS new_column String DEFAULT ''
    """,
    """
    ALTER TABLE sharded_events
    ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS new_column
    """,
)
```

## Creating a New Table

Use `CREATE TABLE IF NOT EXISTS` and always specify the cluster:

```python
RunSQL(
    """
    CREATE TABLE IF NOT EXISTS my_new_table ON CLUSTER '{cluster}'
    (
        id UUID,
        team_id Int64,
        created_at DateTime64(6, 'UTC') DEFAULT now(),
        properties VARCHAR
    )
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/posthog.my_new_table', '{replica}')
    ORDER BY (team_id, id)
    SETTINGS index_granularity = 8192
    """,
    "DROP TABLE IF EXISTS my_new_table ON CLUSTER '{cluster}'",
)
```

## Distributed Table Pattern

For sharded deployments, create both the local and distributed table:

```python
operations = [
    # 1. Create local (sharded) table
    RunSQL(
        """
        CREATE TABLE IF NOT EXISTS sharded_my_table ON CLUSTER '{cluster}'
        (
            team_id Int64,
            id UUID,
            data String
        )
        ENGINE = ReplicatedReplacingMergeTree(...)
        ORDER BY (team_id, id)
        """,
        "DROP TABLE IF EXISTS sharded_my_table ON CLUSTER '{cluster}'",
    ),
    # 2. Create distributed table that queries all shards
    RunSQL(
        """
        CREATE TABLE IF NOT EXISTS my_table ON CLUSTER '{cluster}'
        AS sharded_my_table
        ENGINE = Distributed('{cluster}', posthog, sharded_my_table, rand())
        """,
        "DROP TABLE IF EXISTS my_table ON CLUSTER '{cluster}'",
    ),
]
```

## Adding a Materialized Column

Materialized columns are computed from existing columns and stored on disk:

```python
RunSQL(
    """
    ALTER TABLE sharded_events
    ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS mat_browser
    VARCHAR MATERIALIZED trim(BOTH '"' FROM JSONExtractRaw(properties, '$browser'))
    """,
    """
    ALTER TABLE sharded_events
    ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS mat_browser
    """,
)
```

## Adding an Index

```python
RunSQL(
    """
    ALTER TABLE sharded_events
    ON CLUSTER '{cluster}'
    ADD INDEX IF NOT EXISTS minmax_team_id team_id TYPE minmax GRANULARITY 1
    """,
    """
    ALTER TABLE sharded_events
    ON CLUSTER '{cluster}'
    DROP INDEX IF EXISTS minmax_team_id
    """,
)
```

## Important Rules

1. **Always use `IF NOT EXISTS` / `IF EXISTS`** — migrations may be re-run in test environments.
2. **Always specify `ON CLUSTER '{cluster}'`** — required for multi-node deployments.
3. **Provide rollback SQL** — the second argument to `RunSQL` is the reverse migration.
4. **Avoid `ALTER TABLE ... MODIFY`** — prefer adding new columns over modifying existing ones.
5. **Test on both single-node and cluster** — use `{shard}` and `{replica}` macros.
6. **Never delete data in a migration** — use TTL policies or separate cleanup jobs.
