Run a web analytics **breakdown table** query — top pages, UTMs, devices, browsers, countries, etc. — with visitors and views per row, plus optional bounce rate / average time on page columns. This is the right tool for "top pages with bounce rate" and for entry / exit / previous-page navigation analysis.

Use `query-web-overview` for KPIs (no breakdown). For time-series of pageviews or other events, use the generic `query-trends`.

# Web analytics vs product analytics — pick by what's in the question

The deciding factor is **whether the answer needs session-level math**. `query-web-stats` aggregates `$pageview` / `$screen` events into sessions before computing metrics; product analytics queries operate per-event without that step. Session aggregation is more expensive — only pay for it when you need it.

**Use `query-web-stats` when the breakdown or metric requires sessions:**

- **Bounce rate** (`includeBounceRate=true`) or **session-level avg time on page** (`includeAvgTimeOnPage=true`) — both are per-session.
- **Initial / first-touch breakdowns**: `InitialPage`, `InitialChannelType`, `InitialReferringDomain`, `InitialUTMSource`/`Medium`/`Campaign`/`Term`/`Content`. These all require knowing which event was first in a session.
- **Exit page** (`ExitPage`) — last event in a session.
- "Top pages by visitors" / "top traffic sources" — the in-product Web analytics framing.

**Use `query-trends` / `query-paths` instead when the question is per-event:**

- Counting any event (pageviews, sign-ups, button clicks, custom events) by an event property without needing session boundaries (`query-trends` with a breakdown).
- Navigation between arbitrary events — `query-paths` works on any event sequence and doesn't compute bounce rate.
- User-level math (`dau`, `mau`, sum/avg of an event property).

When a question could go either way, prefer the per-event tools — they're faster.

Examples:

- "Top pages by **bounce rate**" → `query-web-stats` (`Page` + `includeBounceRate`).
- "Pageviews **by initial UTM source**" → `query-web-stats` (`InitialUTMSource`).
- "Pageviews by current `utm_source` event property" → `query-trends` with the `$pageview` event broken down by `utm_source`. No session needed.
- "What events happen after sign-up" → `query-paths`.

CRITICAL: Be minimalist. Only set fields needed to answer the user's question. Default settings are usually sufficient.

# `breakdownBy` cheat-sheet

Path-style — pair these with `includeBounceRate` and/or `includeAvgTimeOnPage`:

- `Page` — pathname (the page being viewed)
- `InitialPage` — entry page of the session
- `ExitPage` — exit page of the session
- `PreviousPage` — the page the user came from

Marketing / source:

- `InitialChannelType`, `InitialReferringDomain`, `InitialReferringURL`
- `InitialUTMSource`, `InitialUTMMedium`, `InitialUTMCampaign`, `InitialUTMTerm`, `InitialUTMContent`, `InitialUTMSourceMediumCampaign`

Audience / device:

- `Browser`, `OS`, `Viewport`, `DeviceType`
- `Country`, `Region`, `City`, `Timezone`, `Language`

Other:

- `ScreenName` (mobile), `ExitClick`, `FrustrationMetrics`

# Quick mapping from user question to fields

| User asks for                  | `breakdownBy`                                    | Other fields                |
| ------------------------------ | ------------------------------------------------ | --------------------------- |
| Top pages                      | `Page`                                           | —                           |
| Top pages by bounce rate       | `Page`                                           | `includeBounceRate=true`    |
| Average time on page           | `Page`                                           | `includeAvgTimeOnPage=true` |
| Entry / exit pages with bounce | `InitialPage` or `ExitPage`                      | `includeBounceRate=true`    |
| Top traffic sources            | `InitialChannelType` or `InitialReferringDomain` | —                           |
| UTM campaign performance       | `InitialUTMCampaign`                             | —                           |
| Browser / OS / device split    | `Browser` / `OS` / `DeviceType`                  | —                           |
| Geography                      | `Country` / `Region` / `City`                    | —                           |

# Time period & filters

Same conventions as `query-web-overview`. Use `dateRange` (default to last 7 days when unspecified), `compareFilter`, and `properties` for filters. Use `read-data-schema` to validate property names/values when needed.

# Pagination

Use `limit` (default backend limit) and `offset` for pagination. Keep `limit` small (10–25) unless the user asks otherwise.

# Example

Top 20 pages by bounce rate over the last 7 days:

```json
{
  "kind": "WebStatsTableQuery",
  "breakdownBy": "Page",
  "includeBounceRate": true,
  "limit": 20,
  "dateRange": { "date_from": "-7d" }
}
```

# Out of scope

Goals (`WebGoalsQuery`), web vitals (`WebVitalsPathBreakdownQuery`), and external clicks (`WebExternalClicksTableQuery`) are not yet exposed via MCP. For those, fall back to `execute-sql`.
