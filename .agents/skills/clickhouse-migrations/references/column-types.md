# ClickHouse Column Types Reference

This reference covers the most commonly used ClickHouse column types in PostHog migrations, including when to use each type, gotchas, and examples.

## Numeric Types

### UInt8, UInt16, UInt32, UInt64
Use for non-negative integers. Prefer the smallest type that fits your range.

```sql
-- Good: use UInt8 for small enumerations (0-255)
status UInt8,

-- Good: use UInt64 for counts and IDs
event_count UInt64,
team_id UInt64,
```

### Int8, Int16, Int32, Int64
Use when negative values are possible.

```sql
-- Good: offset or delta values
delta Int32,
```

### Float32, Float64
Avoid for monetary or exact values — use `Decimal` instead.

```sql
-- Avoid for money:
price Float64,  -- BAD

-- Prefer:
price Decimal(18, 6),  -- GOOD
```

## String Types

### String
Variable-length byte string. Use for arbitrary text, JSON blobs, or binary data.

```sql
properties String,
distinct_id String,
```

### FixedString(N)
Fixed-length string padded with null bytes. Only use when the length is truly fixed (e.g., UUIDs stored as raw bytes).

```sql
-- UUIDs stored as 16-byte FixedString
uuid FixedString(16),
```

### LowCardinality(String)
Use when a String column has a small number of distinct values (< ~10,000). Dramatically reduces storage and speeds up GROUP BY.

```sql
-- Good candidates for LowCardinality:
event LowCardinality(String),
browser LowCardinality(String),
os LowCardinality(String),
```

**Pitfall:** Do NOT use `LowCardinality` on high-cardinality columns like `distinct_id` or `uuid` — it will hurt performance.

## Date and Time Types

### DateTime
Stores date+time as Unix timestamp (seconds). Timezone-naive by default.

```sql
created_at DateTime,
```

### DateTime64(precision[, timezone])
Supports sub-second precision. Use `DateTime64(6)` for microseconds.

```sql
-- Preferred for event timestamps:
timestamp DateTime64(6, 'UTC'),
```

### Date
Stores only the date (no time). Useful for partitioning keys.

```sql
-- Partitioning:
PARTITION BY toYYYYMM(toDate(timestamp))
```

## UUID

```sql
-- Native UUID type (stored as 16 bytes, displayed as string)
uuid UUID,

-- Alternatively, stored as String for compatibility:
uuid String,
```

PostHog generally uses `String` for UUIDs to avoid conversion overhead at the application layer.

## Boolean

ClickHouse does not have a native Boolean type. Use `UInt8` with values 0 and 1.

```sql
is_identified UInt8 DEFAULT 0,
```

## Nullable Types

Avoid `Nullable(T)` unless truly necessary. Nullable columns:
- Require extra storage (null map)
- Slow down aggregations
- Cannot be used in primary/sort keys

```sql
-- Avoid:
some_value Nullable(String),

-- Prefer a sentinel value:
some_value String DEFAULT '',
```

## Array Types

```sql
-- Array of strings:
tags Array(String),

-- Array of UInt64:
group_ids Array(UInt64),
```

Arrays work well with `arrayJoin`, `has`, and `indexOf` functions.

## Map Types

Available in ClickHouse 21.1+. Use sparingly — prefer JSON stored as `String` with JSONExtract functions for flexibility.

```sql
-- Map type:
attributes Map(String, String),
```

## Enum Types

Use `Enum8` or `Enum16` for columns with a known, fixed set of string values.

```sql
event_type Enum8('pageview' = 1, 'autocapture' = 2, 'identify' = 3),
```

**Pitfall:** Adding new Enum values requires an ALTER TABLE — plan ahead or use `LowCardinality(String)` for extensibility.

## Codec Recommendations

Add compression codecs to improve storage efficiency:

```sql
-- Delta codec for monotonically increasing values:
timestamp DateTime CODEC(Delta, ZSTD(1)),

-- ZSTD for general string compression:
properties String CODEC(ZSTD(3)),

-- DoubleDelta for slowly changing integers:
counter UInt64 CODEC(DoubleDelta, LZ4),
```

## Quick Reference Table

| Use Case              | Recommended Type                  |
|-----------------------|-----------------------------------|
| Event timestamp       | `DateTime64(6, 'UTC')`            |
| Team/project ID       | `UInt64`                          |
| Distinct ID           | `String`                          |
| Event name            | `LowCardinality(String)`          |
| JSON properties       | `String`                          |
| Boolean flag          | `UInt8 DEFAULT 0`                 |
| UUID                  | `String` or `UUID`                |
| Small enum            | `LowCardinality(String)` or `Enum8` |
| Monetary value        | `Decimal(18, 6)`                  |
