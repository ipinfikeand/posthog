import { AssistantDateRangeFilter } from './schema-assistant-queries'
import {
    CompareFilter,
    NodeKind,
    WebAnalyticsConversionGoal,
    WebAnalyticsPropertyFilters,
    WebStatsBreakdown,
} from './schema-general'
import { integer } from './type-utils'

export interface WebAnalyticsAssistantFilters {
    date_from?: string | null
    date_to?: string | null
    properties: WebAnalyticsPropertyFilters
    doPathCleaning?: boolean
    compareFilter?: CompareFilter | null
}

/**
 * Shared filter set across all web-analytics assistant queries.
 *
 * Mirrors `WebAnalyticsQueryBase` but drops noisy fields agents shouldn't set
 * (`samplingFactor`, `aggregation_group_type_index`, `dataColorTheme`,
 * deprecated/legacy options).
 */
export interface AssistantWebAnalyticsQueryBase {
    /**
     * Date range for the query. Defaults to the last 7 days when omitted.
     */
    dateRange?: AssistantDateRangeFilter

    /**
     * Property filters applied to the query. Accepts event, person, session,
     * cohort, or HogQL filters. Default: [].
     */
    properties?: WebAnalyticsPropertyFilters

    /**
     * Compare the current period to a prior period.
     */
    compareFilter?: CompareFilter

    /**
     * Apply the team's path-cleaning rules to URL-style breakdowns.
     * @default false
     */
    doPathCleaning?: boolean

    /**
     * Exclude internal and test users by applying the team's test-account filter.
     * @default false
     */
    filterTestAccounts?: boolean

    /**
     * Conversion goal (action ID or custom event name) for goal-aware metrics.
     */
    conversionGoal?: WebAnalyticsConversionGoal | null
}

/**
 * High-level web-analytics KPIs over a period: visitors, views, sessions,
 * average session duration, and bounce rate. Returns one row of metrics with
 * optional period-over-period comparison.
 *
 * Use this when the user asks "how is the site doing?", "what are the topline
 * web numbers?", or wants a snapshot of overall traffic health.
 */
export interface AssistantWebOverviewQuery extends AssistantWebAnalyticsQueryBase {
    kind: NodeKind.WebOverviewQuery
}

/**
 * Tabular web-analytics breakdown — top pages, UTMs, devices, browsers,
 * countries, etc. — with visitors, views, and optional bounce rate / avg time
 * on page columns.
 *
 * This is the right query for "top pages with bounce rate" (set
 * `breakdownBy=Page` and `includeBounceRate=true`) and for entry/exit-page
 * navigation analysis (`breakdownBy=InitialPage|ExitPage|PreviousPage`).
 */
export interface AssistantWebStatsTableQuery extends AssistantWebAnalyticsQueryBase {
    kind: NodeKind.WebStatsTableQuery

    /**
     * Property to break down the table by. `Page`, `InitialPage`, `ExitPage`,
     * and `PreviousPage` are all path-style breakdowns and pair naturally with
     * `includeBounceRate` / `includeAvgTimeOnPage`.
     */
    breakdownBy: WebStatsBreakdown

    /**
     * Add a bounce-rate column. Most useful with a Page-style breakdown.
     * @default false
     */
    includeBounceRate?: boolean

    /**
     * Add an average-time-on-page column. Implies a Page-style breakdown.
     * @default false
     */
    includeAvgTimeOnPage?: boolean

    /**
     * When breaking down by Page, concatenate host + pathname so the same path
     * on different hosts is counted separately.
     * @default false
     */
    includeHost?: boolean

    /**
     * Maximum rows to return.
     */
    limit?: integer

    /**
     * Pagination offset.
     */
    offset?: integer
}

/**
 * Discriminated union of the assistant-facing web-analytics query types. The
 * `kind` literal selects which mode the agent is invoking.
 *
 * - `WebOverviewQuery`: KPIs (visitors, sessions, bounce rate, duration)
 * - `WebStatsTableQuery`: tabular breakdowns (top pages, UTMs, devices, …)
 *   with optional bounce rate / avg time on page
 *
 * Time-series of arbitrary events (including pageviews) are covered by the
 * generic `query-trends` tool — there is no `AssistantWebTrendsQuery`.
 */
export type AssistantWebAnalyticsQuery = AssistantWebOverviewQuery | AssistantWebStatsTableQuery
