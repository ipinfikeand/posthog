# ClickHouse Migration Performance Considerations

This document covers performance best practices when writing ClickHouse migrations in PostHog.

## Table of Contents
- [Mutation Operations](#mutation-operations)
- [Adding Columns](#adding-columns)
- [Dropping Columns](#dropping-columns)
- [Index Changes](#index-changes)
- [Data Backfills](#data-backfills)
- [Cluster-Wide Operations](#cluster-wide-operations)

---

## Mutation Operations

Mutations (`ALTER TABLE ... UPDATE`, `ALTER TABLE ... DELETE`) are expensive in ClickHouse because they rewrite data parts on disk.

### Avoid mutations where possible

```sql
-- BAD: triggers a heavyweight mutation
ALTER TABLE sharded_events UPDATE properties = '{}' WHERE properties = '' SETTINGS mutations_sync = 2;

-- GOOD: use a new column with a default value instead
ALTER TABLE sharded_events ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS properties_v2 String DEFAULT '{}';
```

### When mutations are unavoidable

- Always run mutations **asynchronously** (do not use `mutations_sync = 2` in production migrations).
- Monitor mutation progress via `system.mutations`.
- Never run more than one mutation concurrently on the same table.

```sql
-- Check pending mutations
SELECT database, table, mutation_id, command, is_done
FROM system.mutations
WHERE is_done = 0
ORDER BY create_time DESC;
```

---

## Adding Columns

Adding columns in ClickHouse is a **metadata-only** operation and is generally safe and fast.

```sql
-- Safe: metadata-only, instant
ALTER TABLE sharded_events ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS my_new_column UInt8 DEFAULT 0;
```

### Defaults and materialized columns

- `DEFAULT` expressions are evaluated **at read time** — no backfill needed.
- `MATERIALIZED` columns are computed for **new inserts only**; existing rows return the default (usually `0` or empty).
- Use `ALIAS` columns for computed views that should not be stored.

```sql
-- MATERIALIZED: stored for new data only
ALTER TABLE sharded_events ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS event_date Date MATERIALIZED toDate(timestamp);

-- ALIAS: never stored, computed on read
ALTER TABLE sharded_events ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS event_year UInt16 ALIAS toYear(timestamp);
```

---

## Dropping Columns

Dropping a column is also a metadata operation but frees disk space **lazily** (during future merges).

```sql
-- Safe to run, disk space freed during background merges
ALTER TABLE sharded_events ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS deprecated_column;
```

> **Warning:** Always ensure no application code reads the column before dropping it.

---

## Index Changes

Adding or dropping skipping indexes triggers a background mutation to rebuild affected parts.

```sql
-- Adding a skipping index — triggers background mutation
ALTER TABLE sharded_events ON CLUSTER '{cluster}'
    ADD INDEX IF NOT EXISTS idx_team_id team_id TYPE minmax GRANULARITY 1;

-- Materialize the index explicitly (optional, forces rebuild now)
ALTER TABLE sharded_events ON CLUSTER '{cluster}'
    MATERIALIZE INDEX idx_team_id;
```

### Granularity tuning

| Index Type | Recommended Granularity | Use Case |
|------------|------------------------|----------|
| `minmax`   | 1–4                    | Numeric range filters |
| `set(N)`   | 1                      | Low-cardinality string filters |
| `bloom_filter` | 1                  | High-cardinality string equality |

---

## Data Backfills

If you need to backfill data into a new column, prefer inserting into a new table and swapping, or use `INSERT INTO ... SELECT` in small batches.

```python
# In a migration, avoid large single-pass backfills.
# Instead, use batch processing:

BATCH_SIZE = 1_000_000

operations = [
    # Step 1: add column with a safe default
    operations.RunSQL(
        sql="ALTER TABLE sharded_events ON CLUSTER '{cluster}' ADD COLUMN IF NOT EXISTS enriched UInt8 DEFAULT 0",
        rollback="ALTER TABLE sharded_events ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS enriched",
    ),
    # Step 2: backfill is handled by a separate async job, not in the migration itself
]
```

> **Rule:** Never run a full-table `UPDATE` inside a migration. Schedule backfills as separate operational tasks.

---

## Cluster-Wide Operations

All DDL statements in PostHog's ClickHouse setup must include `ON CLUSTER '{cluster}'` to propagate changes to all shards and replicas.

```sql
-- Always use ON CLUSTER
ALTER TABLE sharded_events ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS new_col String DEFAULT '';

-- Distributed table may also need updating
ALTER TABLE events ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS new_col String DEFAULT '';
```

### Replication lag

- After a DDL on the cluster, allow time for replication before reading the new schema.
- Use `system.replicas` to check replication health:

```sql
SELECT host_name, table, is_leader, inserts_in_queue, queue_size
FROM system.replicas
WHERE table = 'sharded_events';
```

---

## Quick Reference Checklist

- [ ] Does the migration avoid mutations? If not, is there a safer alternative?
- [ ] Are all DDL statements using `ON CLUSTER '{cluster}'`?
- [ ] Does the migration use `IF NOT EXISTS` / `IF EXISTS` guards?
- [ ] Are large data backfills deferred to async jobs?
- [ ] Has the migration been tested on a staging cluster before production?
- [ ] Is there a rollback operation defined for each forward operation?
