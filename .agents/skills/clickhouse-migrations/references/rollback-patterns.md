# ClickHouse Migration Rollback Patterns

This document describes patterns for safely rolling back ClickHouse migrations in PostHog.

## Overview

ClickHouse migrations are generally **not reversible** due to the nature of columnar storage and distributed tables. However, there are patterns to minimize risk and handle rollbacks when necessary.

## Safe Migration Practices

### 1. Additive-Only Changes (Preferred)

Always prefer additive changes that don't break existing queries:

```sql
-- GOOD: Adding a nullable column with a default
ALTER TABLE sharded_events
    ADD COLUMN IF NOT EXISTS new_property Nullable(String);

-- GOOD: Adding a new materialized column
ALTER TABLE sharded_events
    ADD COLUMN IF NOT EXISTS $group_0 VARCHAR MATERIALIZED JSONExtractString(properties, '$group_0');
```

### 2. Two-Phase Column Removal

Never drop columns in a single migration. Use a two-phase approach:

**Phase 1 (current release):** Stop writing to the column in application code.

**Phase 2 (next release):** Drop the column after verifying no reads/writes.

```sql
-- Phase 2 migration
ALTER TABLE sharded_events
    DROP COLUMN IF EXISTS deprecated_column;

ALTER TABLE events
    DROP COLUMN IF EXISTS deprecated_column;
```

### 3. Rollback via Re-Migration

When a migration must be rolled back, create a new forward migration that undoes the change:

```python
from infi.clickhouse_orm import migrations

class Migration(migrations.Migration):
    """
    Rollback migration: removes the column added in migration 0042.
    This is a forward migration that acts as a rollback.
    """

    dependencies = [("posthog", "0043_rollback_new_column")]

    operations = [
        migrations.RunSQL(
            "ALTER TABLE sharded_events DROP COLUMN IF EXISTS problematic_column"
        ),
        migrations.RunSQL(
            "ALTER TABLE events DROP COLUMN IF EXISTS problematic_column"
        ),
    ]
```

## Table Type Considerations

### Distributed Tables vs Sharded Tables

Always apply schema changes to **both** the sharded (local) table and the distributed table:

| Table Name | Type | Notes |
|---|---|---|
| `sharded_events` | ReplicatedReplacingMergeTree | Local shard storage |
| `events` | Distributed | Query layer across shards |
| `sharded_person` | ReplicatedReplacingMergeTree | Local shard storage |
| `person` | Distributed | Query layer across shards |

```sql
-- Always update both tables
ALTER TABLE sharded_events ADD COLUMN IF NOT EXISTS my_col String DEFAULT '';
ALTER TABLE events ADD COLUMN IF NOT EXISTS my_col String DEFAULT '';
```

## Handling Failed Migrations

### Idempotent Migrations

All migrations should be idempotent using `IF EXISTS` / `IF NOT EXISTS`:

```sql
-- Safe to run multiple times
ALTER TABLE sharded_events
    ADD COLUMN IF NOT EXISTS my_column UInt8 DEFAULT 0;

ALTER TABLE sharded_events
    DROP COLUMN IF EXISTS old_column;
```

### Checking Migration State

```sql
-- Check if a column exists before proceeding
SELECT name, type
FROM system.columns
WHERE table = 'sharded_events'
  AND database = 'posthog'
  AND name = 'my_column';

-- Check if a table exists
SELECT name
FROM system.tables
WHERE database = 'posthog'
  AND name = 'my_new_table';
```

## Index and Materialized View Rollbacks

### Dropping Indexes

```sql
-- Drop a skipping index
ALTER TABLE sharded_events
    DROP INDEX IF EXISTS my_index;
```

### Dropping Materialized Views

```sql
-- Materialized views must be dropped explicitly
DROP VIEW IF EXISTS posthog.my_materialized_view;
```

> **Warning:** Dropping a materialized view does not remove the target table.
> Drop the target table separately if needed.

## Testing Rollback Procedures

Before deploying a migration to production:

1. Test the migration on a staging cluster
2. Verify the rollback migration works on staging
3. Document the rollback steps in the migration PR
4. Ensure the rollback migration is committed alongside the forward migration

## Emergency Procedures

For critical production issues:

1. **Do not** attempt to manually revert ClickHouse schema changes without a migration
2. Deploy the rollback migration as a hotfix
3. Monitor `system.mutations` for long-running ALTER operations:

```sql
SELECT database, table, command, create_time, is_done, parts_to_do
FROM system.mutations
WHERE is_done = 0
ORDER BY create_time DESC;
```
