# ClickHouse Data Types Mapping

This reference covers common data type mappings between Python/Django and ClickHouse, helping you choose the right column types when writing migrations.

## Python to ClickHouse Type Mapping

| Python Type | Django Field | ClickHouse Type | Notes |
|-------------|-------------|-----------------|-------|
| `str` | `CharField` | `String` | Variable length, no max |
| `str` (UUID) | `UUIDField` | `UUID` | Use `UUID` not `String` for UUIDs |
| `int` | `IntegerField` | `Int64` | Default to 64-bit |
| `int` (small) | `SmallIntegerField` | `Int16` | For bounded small values |
| `int` (positive) | `PositiveIntegerField` | `UInt32` | For non-negative values |
| `float` | `FloatField` | `Float64` | Double precision |
| `Decimal` | `DecimalField` | `Decimal(P, S)` | Specify precision and scale |
| `bool` | `BooleanField` | `UInt8` | 0 or 1 |
| `datetime` | `DateTimeField` | `DateTime64(6, 'UTC')` | Always use UTC |
| `date` | `DateField` | `Date` | Date only, no time |
| `dict` | `JSONField` | `String` | Store as JSON string |
| `list` | `ArrayField` | `Array(T)` | Typed arrays |

## PostHog-Specific Conventions

### Team and Project IDs
```sql
-- Always use Int64 for team_id
team_id Int64,

-- Use UUID for project identifiers
project_id UUID,
```

### Timestamps
```sql
-- Event timestamps — use DateTime64 with microsecond precision
timestamp DateTime64(6, 'UTC'),

-- Created/updated at fields — second precision is usually fine
created_at DateTime DEFAULT now(),
updated_at DateTime DEFAULT now(),
```

### Person and Distinct IDs
```sql
-- Person UUIDs
person_id UUID,

-- Distinct IDs are arbitrary strings
distinct_id String,
```

### Properties
```sql
-- Raw properties blob (JSON)
properties String DEFAULT '{}',

-- Extracted property columns for filtering performance
property_value_email String MATERIALIZED JSONExtractString(properties, 'email'),
```

## Nullable vs Non-Nullable

Prefer non-nullable columns with sensible defaults over `Nullable(T)` — ClickHouse handles `Nullable` less efficiently.

```sql
-- Prefer this:
some_string String DEFAULT '',
some_int    Int64  DEFAULT 0,

-- Over this (only when NULL is semantically meaningful):
some_nullable_string Nullable(String),
some_nullable_int    Nullable(Int64),
```

### When to Use Nullable
- The absence of a value has distinct meaning from an empty/zero value
- Joining with external data where NULL indicates "no match"
- Optional foreign-key style references

## LowCardinality Optimization

Wrap `String` columns with few distinct values in `LowCardinality` to reduce storage and improve query performance:

```sql
-- Good candidates for LowCardinality
event_type   LowCardinality(String),
browser      LowCardinality(String),
os           LowCardinality(String),
country_code LowCardinality(String),  -- ~250 values

-- Bad candidates (too many distinct values)
distinct_id  String,   -- millions of unique values
properties   String,   -- arbitrary JSON
```

**Rule of thumb:** Use `LowCardinality` when cardinality is below ~10,000 distinct values.

## Array Types

```sql
-- Array of strings
tags Array(String) DEFAULT [],

-- Array of integers
group_ids Array(Int64) DEFAULT [],

-- Array of UUIDs
related_persons Array(UUID) DEFAULT [],
```

## Map Types (ClickHouse 21.1+)

```sql
-- For key-value pairs with known value type
string_properties Map(String, String) DEFAULT map(),
numeric_properties Map(String, Float64) DEFAULT map(),
```

## Enum Types

Use `Enum8` or `Enum16` for columns with a fixed, known set of values:

```sql
-- Enum8 supports up to 255 values
status Enum8(
    'pending'   = 1,
    'running'   = 2,
    'completed' = 3,
    'failed'    = 4
) DEFAULT 'pending',
```

> **Warning:** Adding new Enum values requires an `ALTER TABLE ... MODIFY COLUMN` migration. Consider `LowCardinality(String)` if the set of values may grow.

## Common Pitfalls

1. **Don't use `String` for UUIDs** — use the native `UUID` type for storage efficiency and proper sorting.
2. **Don't use `DateTime` for event timestamps** — use `DateTime64(6, 'UTC')` to preserve microsecond precision.
3. **Avoid deeply nested `Nullable`** — `Nullable(Array(String))` is not supported; use `Array(String)` with an empty default.
4. **`Int32` vs `UInt32`** — choose signed/unsigned based on whether negative values are possible.
