Run a web analytics overview query — high-level KPIs over a period: visitors, pageviews, sessions, average session duration, and bounce rate. Returns a small list of metric tuples with optional period-over-period comparison. Mirrors the in-product **Web analytics** scene.

# When to use this vs `query-trends`

Pick this tool only when the answer needs **session-level math**. Session aggregation is more expensive than per-event queries — only pay for it when needed.

Use `query-web-overview` when the question references: bounce rate, session duration, sessions as a count, or entry/initial values (entry page, initial channel, initial UTM).

Use `query-trends` instead for per-event counts — pageviews, sign-ups, button clicks. Faster.

# Inputs

- `dateRange` — defaults to last 7 days when omitted.
- `compareFilter: { compare: true }` — return prior-period values for change %.
- `properties` — event/person/session/HogQL filters. Same operator semantics as `query-trends` — see that prompt.
- `filterTestAccounts` — exclude internal/test users.
- `doPathCleaning` — apply team's path-cleaning rules.
- `conversionGoal` — action ID or custom event name; only set when the user asks about a conversion.

Use `read-data-schema` to validate property names/values when needed.

# Example

```json
{
  "kind": "WebOverviewQuery",
  "dateRange": { "date_from": "-7d" },
  "compareFilter": { "compare": true }
}
```

# Out of scope

Goals, web vitals, and external clicks are not exposed via MCP. Fall back to `execute-sql`.
