# Common Pitfalls in ClickHouse Migrations

This document outlines frequent mistakes and how to avoid them when writing ClickHouse migrations for PostHog.

## 1. Mutating Data Without `FINAL`

ClickHouse uses a merge-tree engine that lazily merges parts. When querying or mutating data, rows may appear duplicated until parts are merged.

**Problem:**
```sql
-- This may return duplicate rows or miss updates
SELECT * FROM sharded_events WHERE team_id = 1;
```

**Solution:**
```sql
-- Use FINAL to force merge semantics (slower but correct)
SELECT * FROM sharded_events FINAL WHERE team_id = 1;
```

> ⚠️ Avoid `FINAL` in high-throughput paths — prefer it only in migrations or one-off queries.

---

## 2. Adding Columns Without Default Values

ClickHouse allows adding columns, but existing rows will return the type's zero-value unless a default is specified.

**Problem:**
```sql
ALTER TABLE events ADD COLUMN new_flag Boolean;
-- Existing rows return 0 (false), which may be misleading
```

**Solution:**
```sql
ALTER TABLE events ADD COLUMN new_flag Boolean DEFAULT 0;
-- Be explicit about the default to document intent
```

---

## 3. Forgetting Distributed Table Counterparts

PostHog uses both sharded (local) and distributed tables. A migration that only alters the local table will break reads/writes through the distributed layer.

**Problem:**
```python
operations = [
    run_sql_with_exceptions(
        "ALTER TABLE sharded_events ADD COLUMN extra String DEFAULT ''"
    )
]
# Missing: ALTER TABLE events (distributed) ADD COLUMN extra String DEFAULT ''
```

**Solution:**
```python
operations = [
    run_sql_with_exceptions(
        "ALTER TABLE sharded_events ADD COLUMN extra String DEFAULT ''"
    ),
    run_sql_with_exceptions(
        "ALTER TABLE events ADD COLUMN extra String DEFAULT ''"
    ),
]
```

---

## 4. Using `DROP COLUMN` Without Checking Dependencies

Materialized views, projections, or other tables may depend on a column. Dropping it without checking will cause silent failures or errors.

**Checklist before dropping a column:**
- Search for the column name in all materialized view definitions.
- Check any projection definitions on the table.
- Verify no application code references the column directly.

---

## 5. Running Heavy Mutations Synchronously

`ALTER TABLE ... UPDATE` and `ALTER TABLE ... DELETE` are asynchronous mutations in ClickHouse. They do not block, but they consume significant I/O.

**Problem:**
```sql
-- This returns immediately but runs in the background
ALTER TABLE events DELETE WHERE team_id = 999;
-- Assuming it's done after the migration completes is wrong
```

**Solution:**
- For migrations, prefer adding a new column and backfilling via a new materialized view.
- If a mutation is necessary, monitor `system.mutations` for completion.
- Never depend on mutation completion within the same migration script.

---

## 6. Ignoring Replication Lag on Replicated Tables

On `ReplicatedMergeTree` tables, DDL statements are replicated asynchronously. A migration may succeed on one replica but not yet be applied on others.

**Mitigation:**
- Use `ON CLUSTER` clauses when operating in a clustered environment.
- Add health checks in your deployment pipeline to verify schema consistency across replicas.

```sql
-- Preferred for clustered deployments
ALTER TABLE sharded_events ON CLUSTER '{cluster}' ADD COLUMN new_col String DEFAULT '';
```

---

## 7. Incorrect Order of Operations in Rollbacks

When writing rollback logic, the order must be the exact reverse of the forward migration.

**Problem:**
```python
# Forward: add column A, then add column B (B depends on A)
# Rollback: drop column A first — this will fail if B references A
```

**Solution:**
```python
# Rollback: drop column B first, then drop column A
```

See `rollback-patterns.md` for full rollback templates.

---

## 8. Using `String` Instead of `LowCardinality(String)`

For columns with a small number of distinct values (e.g., event types, status flags), `LowCardinality(String)` significantly reduces storage and improves query performance.

**When to use `LowCardinality`:**
- Fewer than ~10,000 distinct values
- Column is frequently used in `WHERE`, `GROUP BY`, or `ORDER BY`

```sql
-- Prefer this for low-cardinality string columns
ALTER TABLE events ADD COLUMN event_category LowCardinality(String) DEFAULT '';
```
