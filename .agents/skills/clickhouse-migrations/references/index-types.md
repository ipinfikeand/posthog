# ClickHouse Index Types Reference

This document covers the different index types available in ClickHouse and when to use each one in PostHog migrations.

## Primary Key vs ORDER BY

In ClickHouse, the `ORDER BY` clause defines the physical sort order of data on disk. The `PRIMARY KEY` is a prefix of `ORDER BY` and defines what gets stored in the sparse primary index.

```sql
-- Common pattern: ORDER BY includes more columns than PRIMARY KEY
CREATE TABLE posthog_db.my_table
(
    team_id Int64,
    created_at DateTime64(6, 'UTC'),
    uuid UUID,
    properties String
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (team_id, created_at, uuid)
PRIMARY KEY (team_id, created_at)
SETTINGS index_granularity = 8192;
```

## Skipping Indexes (Data Skipping Indexes)

Skipping indexes allow ClickHouse to skip granules that cannot contain matching data. They are defined with `INDEX` after column definitions.

### minmax Index

Stores min/max values for the expression within each granule. Best for monotonically increasing or range-query columns.

```sql
INDEX idx_timestamp created_at TYPE minmax GRANULARITY 1
```

**Use when:** Filtering on date ranges, numeric IDs, or any column with good range selectivity.

### set Index

Stores a set of unique values per granule. Effective when cardinality within a granule is low.

```sql
INDEX idx_event event TYPE set(100) GRANULARITY 1
```

**Use when:** Filtering on low-cardinality columns like event type, status, or boolean flags. The argument (100) limits the set size — use 0 for unlimited.

### bloom_filter Index

Probabilistic filter; trades false positives for compact size. Good for string equality checks and `IN` queries.

```sql
INDEX idx_distinct_id distinct_id TYPE bloom_filter(0.01) GRANULARITY 1
```

**Use when:** Filtering on high-cardinality string columns (UUIDs, distinct IDs, session IDs). The argument is the false positive rate.

### tokenbf_v1 Index

Bloom filter over tokens (whitespace/punctuation-split words). Supports `LIKE`, `hasToken`, and `IN` for tokenized strings.

```sql
INDEX idx_properties properties TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
```

Arguments: `(bloom_filter_size_bytes, hash_functions, seed)`

**Use when:** Searching within JSON properties or free-text fields.

### ngrambf_v1 Index

Bloom filter over n-grams. Supports substring searches with `LIKE '%substring%'`.

```sql
INDEX idx_url url TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1
```

Arguments: `(ngram_size, bloom_filter_size_bytes, hash_functions, seed)`

**Use when:** You need substring matching on URL or path columns.

## Adding Indexes in Migrations

Skipping indexes can be added to existing tables without rewriting data, but they only apply to new data written after the index is created. To index existing data, use `MATERIALIZE INDEX`.

```python
# In a PostHog migration file
from posthog.clickhouse.migrations.base import Migration

class Migration(Migration):
    operations = [
        # Add the index definition
        """
        ALTER TABLE sharded_events
        ON CLUSTER '{cluster}'
        ADD INDEX IF NOT EXISTS idx_session_id
            '$session_id' TYPE bloom_filter(0.01) GRANULARITY 1
        """,
        # Materialize to apply to existing data (expensive — consider skipping in prod)
        """
        ALTER TABLE sharded_events
        ON CLUSTER '{cluster}'
        MATERIALIZE INDEX idx_session_id
        """,
    ]
```

> ⚠️ **Warning:** `MATERIALIZE INDEX` on large tables is expensive and runs in the background. Monitor with `system.mutations`.

## Dropping Indexes

```sql
ALTER TABLE sharded_events
ON CLUSTER '{cluster}'
DROP INDEX IF EXISTS idx_session_id;
```

## Choosing the Right Granularity

`GRANULARITY` in a skipping index means "how many primary index granules are merged into one skipping index entry".

| Granularity | Effect |
|-------------|--------|
| 1 | Most precise, largest index size |
| 4 | Balanced (common default) |
| 8+ | Coarser, smaller index, less effective |

For PostHog tables with `index_granularity = 8192`, a skipping index `GRANULARITY 1` covers 8192 rows per entry.

## PostHog-Specific Patterns

- Always use `ON CLUSTER '{cluster}'` for distributed tables
- Prefer `bloom_filter` for `distinct_id`, `uuid`, and `session_id` columns
- Use `set` for `event` name filtering (low cardinality per granule)
- Avoid over-indexing — each index adds write overhead
