# ClickHouse Schema Patterns

Common schema patterns used in PostHog ClickHouse migrations.

## Table Engines

### ReplacingMergeTree
Use when you need deduplication by a version column or insertion order.

```sql
CREATE TABLE IF NOT EXISTS my_table ON CLUSTER '{cluster}'
(
    id UUID,
    team_id Int64,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    is_deleted UInt8 DEFAULT 0,
    _timestamp DateTime,
    _offset UInt64
) ENGINE = ReplacingMergeTree(_timestamp)
ORDER BY (team_id, id)
PARTITION BY toYYYYMM(created_at)
SETTINGS index_granularity = 512
```

### CollapsingMergeTree
Use when rows need to be collapsed by a sign column (for updates/deletes).

```sql
CREATE TABLE IF NOT EXISTS my_collapsing_table ON CLUSTER '{cluster}'
(
    id UUID,
    team_id Int64,
    value String,
    sign Int8,
    version UInt64
) ENGINE = CollapsingMergeTree(sign)
ORDER BY (team_id, id)
```

### Distributed Tables
Always create a Distributed table alongside the local shard table.

```sql
-- Local shard table
CREATE TABLE IF NOT EXISTS my_table_local ON CLUSTER '{cluster}'
(
    ...
) ENGINE = ReplacingMergeTree(...)
ORDER BY (...)

-- Distributed wrapper
CREATE TABLE IF NOT EXISTS my_table ON CLUSTER '{cluster}'
AS my_table_local
ENGINE = Distributed('{cluster}', 'posthog', 'my_table_local', rand())
```

## Column Types

### Common Type Mappings

| Use Case | ClickHouse Type |
|---|---|
| UUIDs | `UUID` |
| Short strings (enums) | `LowCardinality(String)` |
| Long text / JSON blobs | `String` |
| Timestamps with microseconds | `DateTime64(6, 'UTC')` |
| Timestamps (second precision) | `DateTime` |
| Boolean flags | `UInt8` |
| Integer IDs | `Int64` |
| Nullable optional fields | `Nullable(String)` |
| JSON properties | `String` (stored as JSON text) |

### Nullable Columns
Avoid `Nullable` where possible — it has performance implications. Prefer empty string `''` or `0` as defaults.

```sql
-- Prefer this
value String DEFAULT '',

-- Over this (unless truly nullable)
value Nullable(String),
```

## Ordering Keys

The `ORDER BY` clause defines the primary index. Choose columns that:
1. Are used in WHERE clauses frequently
2. Have low cardinality first (e.g., `team_id`)
3. Are monotonically increasing last (e.g., timestamps, UUIDs)

```sql
-- Good ordering: filters narrow by team first, then by time range
ORDER BY (team_id, toDate(timestamp), event, cityHash64(distinct_id))

-- Avoid high-cardinality columns first
-- Bad: ORDER BY (uuid, team_id)
```

## Partitioning

Partition by month for most event-like tables:

```sql
PARTITION BY toYYYYMM(timestamp)
```

For smaller tables with no time dimension, omit partitioning.

## Materialized Columns

Use materialized columns to pre-compute values for faster filtering:

```sql
CREATE TABLE IF NOT EXISTS events ON CLUSTER '{cluster}'
(
    uuid UUID,
    event String,
    properties String,
    -- Materialized from JSON for fast filtering
    `$session_id` VARCHAR MATERIALIZED JSONExtractString(properties, '$session_id'),
    `$window_id` VARCHAR MATERIALIZED JSONExtractString(properties, '$window_id')
) ENGINE = ...
```

## Adding Columns to Existing Tables

Always use `ADD COLUMN IF NOT EXISTS` and provide a DEFAULT:

```sql
ALTER TABLE my_table ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS new_column String DEFAULT ''
```

For Distributed + local table pairs, alter both:

```sql
ALTER TABLE my_table_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS new_column String DEFAULT ''

ALTER TABLE my_table ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS new_column String DEFAULT ''
```

## Indexes (Skip Indexes)

Bloom filter indexes speed up searches on high-cardinality string columns:

```sql
INDEX idx_properties_key (JSONExtractString(properties, 'key'))
    TYPE bloom_filter(0.01)
    GRANULARITY 1
```

See `database-indexes.md` for more detail on index strategies.
