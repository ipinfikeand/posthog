Run a web analytics **overview** query — high-level KPIs over a period: unique visitors, page views, sessions, average session duration, and bounce rate. Returns a small list of metric tuples with optional period-over-period comparison.

Use this when the user asks "how is the site doing?", "what are the topline web numbers?", or wants a snapshot of overall traffic health. For breakdowns by page / UTM / device, use `query-web-stats`. For time-series of pageviews or other events, use the generic `query-trends`.

# Web analytics vs product analytics — pick by what's in the question

The deciding factor is **whether the answer needs session-level math**. Web analytics queries aggregate `$pageview` / `$screen` events into sessions before computing metrics; product analytics queries operate per-event without that step. Session aggregation is more expensive — only pay for it when you need it.

**Use `query-web-*` (this family) when the question references any of these:**

- **Bounce rate** or **bounces** (only meaningful per session)
- **Session duration** / time spent in session
- **Sessions** as a count (not pageviews)
- **Entry / initial / first-touch values** — entry page, exit page, initial channel, initial referring domain, initial UTM source/medium/campaign/term/content. These all require knowing which event was first in a session.
- "Top pages by visitors" / "top traffic sources" — the in-product Web analytics framing.

**Use `query-trends` / `query-paths` / `query-funnel` / `query-retention` / `query-stickiness` / `query-lifecycle` instead when the question is per-event:**

- Counting any event (pageviews, sign-ups, button clicks, custom events) without needing session boundaries.
- User-level math (`dau`, `mau`, sum/avg of an event property).
- Funnels, retention, lifecycle, paths between arbitrary events.

When a question could go either way, prefer the per-event tools — they're faster.

Examples:

- "Top pages by **bounce rate**" → `query-web-stats`. Needs sessions.
- "Pageviews **by initial UTM source**" → `query-web-stats`. Needs initial-touch.
- "Daily pageviews" → `query-trends` with the `$pageview` event. No session needed.
- "How many users clicked the upgrade button this week" → `query-trends`. Per-event.

CRITICAL: Be minimalist. Only set fields needed to answer the user's question. Default settings are usually sufficient.

# Time period

Use `dateRange` to set the period. If the user doesn't mention time, default to the last 7 days. To return a period-over-period comparison, set `compareFilter.compare=true`.

# Property filters

Apply via `properties`. Web analytics supports event, person, session, and HogQL filters. Only include filters when essential to the question. Operator and property-group conventions are the same as `query-trends` — see that prompt for the full list.

If you need to validate property names or values, use `read-data-schema` first.

# Conversion goal

`conversionGoal` accepts an action ID or custom event name and adds conversion-related fields to the response. Only set this when the user explicitly asks about a conversion metric.

# Example

Topline web KPIs for the last 7 days, compared to the prior 7 days:

```json
{
  "kind": "WebOverviewQuery",
  "dateRange": { "date_from": "-7d" },
  "compareFilter": { "compare": true }
}
```

# Out of scope

If the user asks for goals, web vitals, or external clicks, fall back to `execute-sql` — those runners are not yet exposed via MCP.
