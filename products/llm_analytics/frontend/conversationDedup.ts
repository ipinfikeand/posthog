import { LLMTrace, LLMTraceEvent } from '~/queries/schema/schema-general'

import { CompatMessage } from './types'
import { formatAiErrorForDisplay, normalizeMessage, normalizeMessages } from './utils'

// ─────────────────────────────────────────────────────────────────────────────
// Heuristics in this module mirror the LLMA agent skill scripts at
// `products/llm_analytics/skills/exploring-llm-traces/scripts/`:
//
//   * `extract_conversation.py` — iterates `$ai_generation` events sorted by
//     createdAt and reads role/content/tool_calls off `$ai_input` (the running
//     message history) plus `$ai_output_choices` (the model's reply). That's
//     the field set `messageSignature` keys on and the input shape this module
//     reads.
//
//   * `print_summary.py` — picks `generations[-1]` (the last `$ai_generation`
//     event in the trace) as "the final LLM output". That's the convention
//     `pickUserVisibleTurn` implements (named for the intent, not the mechanism).
//
// Keeping the UI and the skill aligned means agents and humans see the same
// conversation for a given trace. The Python is the existing reference; if we
// invented a different heuristic here, divergence between the two surfaces
// would be silent and confusing.
//
// Known limitation, same as the skill: for noisy LangGraph-style traces where
// the user-visible reply is structurally identified rather than positional,
// the "last generation" heuristic can pick the wrong event. The intended
// escape hatch is an opt-in `$ai_user_visible: true` convention on the SDK
// side; until that lands, the "Show reasoning" affordance is the user's fallback.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Provider-specific transport metadata that lives on typed content parts but
 * does NOT change what the user sees. Stripped before signing so that the
 * same user-visible message dedups across turns even if the caller changes
 * its cache hint, the SDK adds a verification signature, or routing metadata
 * differs between echoes.
 *
 *   - `cache_control` — Anthropic ephemeral-cache directives on text parts.
 *     Callers commonly add/remove these mid-session as the prefix stabilises.
 *   - `signature` — Anthropic cryptographic signature on `thinking` parts.
 *     Not user-visible; can vary between echoes of the same reasoning step.
 *   - `caller` — routing metadata observed on `tool_use` parts in production.
 *     Not user-visible.
 *
 * The replacer fires recursively for every key during serialisation, so these
 * fields are dropped at any nesting level.
 */
const TRANSPORT_METADATA_KEYS = new Set(['cache_control', 'signature', 'caller'])

function isPlainTextPart(part: unknown): boolean {
    return (
        typeof part === 'object' &&
        part !== null &&
        (part as Record<string, unknown>).type === 'text' &&
        typeof (part as Record<string, unknown>).text === 'string'
    )
}

function normalizeSignatureField(key: string, value: unknown): unknown {
    if (TRANSPORT_METADATA_KEYS.has(key)) {
        return undefined
    }
    // Converge text-only typed-parts arrays with their flat-string equivalent.
    // SDKs round-trip the same assistant reply between
    // `{role: 'assistant', content: 'Hello'}` (OpenAI flat-string output) and
    // `{role: 'assistant', content: [{type: 'text', text: 'Hello'}]}` (the app
    // stores history as typed parts and feeds it back as the next call's
    // input). Without this convergence the signature for the output differs
    // from the signature for the next turn's input copy of the same message,
    // and the assistant message re-renders. Only flatten when every part is a
    // plain text part — mixed content (text + tool_use, etc.) keeps its
    // typed-parts shape because it has no flat-string equivalent.
    if (key === 'content' && Array.isArray(value) && value.length > 0 && value.every(isPlainTextPart)) {
        return value.map((part) => (part as { text: string }).text).join('')
    }
    return value
}

/**
 * Stable string hash for a normalized message, used to detect when later turns
 * are re-sending an earlier turn's message in their `$ai_input` history.
 *
 * Two messages are considered "the same turn" iff their role, content,
 * tool_calls, tool_call_id, and (synthetic) tools list all match exactly,
 * **ignoring transport metadata** (see `TRANSPORT_METADATA_KEYS`) and
 * **after converging text-only typed-parts arrays with their flat-string
 * equivalent** (see `normalizeSignatureField`). The field set mirrors what
 * `extract_conversation.py` reads when rendering each turn — role/content/
 * tool_calls — plus tool_call_id (for distinguishing tool responses that
 * share content but answer different calls) and tools (for the synthetic
 * "available tools" pseudo-message `normalizeMessages` prepends).
 *
 * Reused on the Session detail view (cross-trace transcript dedup) and
 * intended to be picked up by any future agent-mode view that needs
 * "have we shown this already?" semantics.
 */
export function messageSignature(message: CompatMessage): string {
    // JSON.stringify preserves order
    return JSON.stringify(
        {
            role: message.role ?? '',
            content: message.content ?? '',
            tool_calls: message.tool_calls ?? null,
            tool_call_id: message.tool_call_id ?? '',
            tools: (message as { tools?: unknown }).tools ?? null,
        },
        // Call normalizeSignatureField for each field above:
        normalizeSignatureField
    )
}

/**
 * Pick the event in a trace that represents the user-visible conversational
 * turn — the thing we want to render as "what the model said" for this trace.
 *
 * The current heuristic is the latest `$ai_generation` event by `createdAt`,
 * matching the LLMA agent skill (`print_summary.py` → `generations[-1]`). That
 * mechanism is an implementation detail of this function: callers should treat
 * the return value as "the turn", not "the last generation". A future
 * `$ai_user_visible: true` convention or a non-positional selector for
 * LangGraph-style runs would change *how* this function picks but not what it
 * promises. Wrong for cases where the final generation is e.g. a
 * logging/cleanup call; the "Show reasoning" affordance is the user's
 * fallback.
 */
export function pickUserVisibleTurn(trace: LLMTrace | undefined): LLMTraceEvent | undefined {
    if (!trace?.events?.length) {
        return undefined
    }
    let latest: LLMTraceEvent | undefined
    let latestTs = -Infinity
    for (const event of trace.events) {
        if (event.event !== '$ai_generation') {
            continue
        }
        const ts = new Date(event.createdAt).getTime()
        if (ts > latestTs) {
            latest = event
            latestTs = ts
        }
    }
    return latest
}

/**
 * Maps each normalized message back to its index in the raw `$ai_input` array.
 * `normalizeMessage` may expand one raw entry into multiple `CompatMessage`s
 * (e.g. typed content parts split into text + tool_call bubbles), so this is
 * many-to-one. A leading `-1` is emitted when a synthetic "available tools"
 * message has been prepended.
 *
 * Why this matters: `ConversationMessagesDisplay` uses these indices to map
 * per-message sentiment back to the right rendered bubble (see its
 * `getMessageSentiment` path). After dedup, the surviving messages would lose
 * that mapping unless we tracked their original positions — which is what this
 * function does. Also called by `ConversationDisplay` (the per-event renderer
 * on the Trace page) so both surfaces share one implementation.
 */
export function buildInputSourceIndices(rawInput: unknown, tools: unknown): number[] {
    const indices: number[] = []
    if (tools) {
        indices.push(-1)
    }
    if (Array.isArray(rawInput)) {
        for (let i = 0; i < rawInput.length; i++) {
            const expanded = normalizeMessage(rawInput[i], 'user')
            for (let j = 0; j < expanded.length; j++) {
                indices.push(i)
            }
        }
    }
    return indices
}

export interface SessionTurnError {
    /** Human-readable label for *what* failed — `$ai_span_name`, `$ai_model`, or the raw event type as a last resort. */
    label: string
    /** Extracted error message. For `$ai_error` objects we surface `.message` directly; everything else is JSON-stringified. */
    message: string
}

export interface SessionTurn {
    /** The trace this turn corresponds to. */
    trace: LLMTrace
    /** True if `fullTraces[trace.id]` was available and we computed messages. */
    isLoaded: boolean
    /** Input messages new to this turn (already-seen messages from prior turns hidden). */
    newInputs: CompatMessage[]
    /** Output messages from this turn's last generation. */
    outputs: CompatMessage[]
    /**
     * The event in the trace that represents the user-visible turn — the
     * source of properties (model, error, latency, …) for the bubble stream
     * and the anchor event that "Show steps" uses to scroll the reasoning
     * panel. Currently this is the last `$ai_generation`, but consumers
     * should treat it as opaque; see `pickUserVisibleTurn` for the
     * (implementation-detail) selection rule.
     */
    userVisibleTurn?: LLMTraceEvent
    /**
     * Distinct tool names called in this turn, in first-appearance order. Pulled
     * from both `$ai_span_name` on instrumented `$ai_span` events and from
     * `tool_calls` / `tool_use` parts in generation outputs. Empty if the trace
     * is unloaded or if no tools were called.
     */
    tools: string[]
    /**
     * Distinct errors in the trace, deduped by `label + message` and ordered by
     * first chronological occurrence. Retries of the same failure collapse to a
     * single entry; truly distinct failures all appear. The trace-level
     * `errorCount` still reflects the total number of error EVENTS (including
     * retries) — `errors.length` is the count of UNIQUE kinds.
     */
    errors: SessionTurnError[]
}

/**
 * Pulls tool names out of an `$ai_output_choices` payload. Covers two shapes:
 * - OpenAI: `[{tool_calls: [{function: {name}}]}]` (also Chat Completions
 *   `{choices: [...]}` and LiteLLM `{message: {tool_calls: [...]}}`).
 * - Anthropic: typed content parts `[{type: 'tool_use', name, input}]`.
 *
 * Anything else is skipped silently — we just return what we can confidently
 * identify, so weird shapes degrade gracefully.
 */
function extractToolNamesFromOutput(rawOutput: unknown): string[] {
    const names: string[] = []
    const messages: unknown[] = Array.isArray(rawOutput)
        ? rawOutput
        : typeof rawOutput === 'object' &&
            rawOutput !== null &&
            'choices' in rawOutput &&
            Array.isArray((rawOutput as { choices: unknown[] }).choices)
          ? (rawOutput as { choices: unknown[] }).choices
          : []
    for (const raw of messages) {
        if (!raw || typeof raw !== 'object') {
            continue
        }
        const msg = 'message' in raw ? (raw as { message: unknown }).message : raw
        if (!msg || typeof msg !== 'object') {
            continue
        }
        const toolCalls = (msg as { tool_calls?: unknown }).tool_calls
        if (Array.isArray(toolCalls)) {
            for (const call of toolCalls) {
                const name = (call as { function?: { name?: unknown } })?.function?.name
                if (typeof name === 'string' && name) {
                    names.push(name)
                }
            }
        }
        const content = (msg as { content?: unknown }).content
        if (Array.isArray(content)) {
            for (const part of content) {
                if (part && typeof part === 'object' && (part as { type?: unknown }).type === 'tool_use') {
                    const name = (part as { name?: unknown }).name
                    if (typeof name === 'string' && name) {
                        names.push(name)
                    }
                }
            }
        }
    }
    return names
}

/**
 * Walks events chronologically and returns deduped error descriptors. Each entry
 * represents a UNIQUE failure (keyed by `label + message`) — retries of the same
 * operation collapse to one entry. Returns `[]` if no event has `$ai_is_error`
 * set or `$ai_error` populated.
 *
 * Why dedup: the chip already shows the raw event count (including retries).
 * The inline detail is more useful when it lists *distinct kinds* of failures —
 * "5 errors, all the same" reads as one item; "5 errors, 3 different kinds"
 * reads as three.
 */
function collectDistinctErrors(events: LLMTraceEvent[]): SessionTurnError[] {
    const sorted = [...events].sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())
    const seen = new Set<string>()
    const ordered: SessionTurnError[] = []
    for (const event of sorted) {
        // PostHog SDKs serialize booleans as strings, so `$ai_is_error: 'false'` is a
        // truthy non-empty string. Check both the canonical 'true' form and the raw
        // boolean to avoid surfacing non-errors as errors.
        const isError =
            event.properties.$ai_is_error === true ||
            event.properties.$ai_is_error === 'true' ||
            event.properties.$ai_error
        if (!isError) {
            continue
        }
        const label =
            (event.properties.$ai_span_name as string | undefined) ||
            (event.properties.$ai_model as string | undefined) ||
            event.event
        const rawError = event.properties.$ai_error
        const message =
            typeof rawError === 'object' &&
            rawError !== null &&
            'message' in rawError &&
            typeof (rawError as { message: unknown }).message === 'string'
                ? (rawError as { message: string }).message
                : formatAiErrorForDisplay(rawError)
        const key = `${label}::${message}`
        if (!seen.has(key)) {
            seen.add(key)
            ordered.push({ label, message })
        }
    }
    return ordered
}

/**
 * Walks all events in a trace once and collects distinct tool names called.
 * Pulls from `$ai_span_name` on instrumented `$ai_span` events and from
 * `tool_calls` / `tool_use` parts in `$ai_generation` outputs. Dedup preserves
 * first-appearance order.
 */
function collectToolsCalled(events: LLMTraceEvent[]): string[] {
    // Sort chronologically so "first-appearance order" is deterministic regardless
    // of whatever order ClickHouse returned the events in.
    const sorted = [...events].sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())
    const seen = new Set<string>()
    const ordered: string[] = []
    const add = (name: string): void => {
        if (!seen.has(name)) {
            seen.add(name)
            ordered.push(name)
        }
    }
    for (const event of sorted) {
        if (event.event === '$ai_span') {
            const spanName = event.properties.$ai_span_name
            if (typeof spanName === 'string' && spanName) {
                add(spanName)
            }
        } else if (event.event === '$ai_generation') {
            for (const name of extractToolNamesFromOutput(event.properties.$ai_output_choices)) {
                add(name)
            }
        }
    }
    return ordered
}

/**
 * Walk the traces in chronological order, normalize each turn's input/output,
 * and hide inputs whose signature already appeared in an earlier turn.
 *
 * Why this is necessary: each `$ai_generation` event's `$ai_input` carries the
 * **full running history** of the conversation up to that point — so without
 * dedup, turn N renders all of turns 1..N-1 again followed by its own new
 * messages.
 *
 * The dedup is computed on signatures of *normalized* `CompatMessage`s, not
 * raw payloads. That means correctness inherits from `normalizeMessage` —
 * shapes the parser recognizes dedup correctly; shapes that fall through to
 * raw-JSON wrapping also dedup correctly because two identical raw blobs
 * produce identical signatures.
 *
 * Pure: takes the loaded data, returns turn descriptors. No side effects.
 */
export function extractSessionTurns(traces: LLMTrace[], fullTraces: Record<string, LLMTrace>): SessionTurn[] {
    const seenSignatures = new Set<string>()
    return traces.map((trace) => {
        const fullTrace = fullTraces[trace.id]
        if (!fullTrace) {
            return {
                trace,
                isLoaded: false,
                newInputs: [],
                outputs: [],
                tools: [],
                errors: [],
            }
        }
        const tools = collectToolsCalled(fullTrace.events ?? [])
        const errors = collectDistinctErrors(fullTrace.events ?? [])
        const userVisibleTurn = pickUserVisibleTurn(fullTrace)
        if (!userVisibleTurn) {
            return {
                trace,
                isLoaded: true,
                newInputs: [],
                outputs: [],
                tools,
                errors,
            }
        }
        const { properties } = userVisibleTurn
        const rawInput = properties.$ai_input ?? properties.$ai_input_state
        const rawOutput = properties.$ai_output_choices ?? properties.$ai_output_state
        const aiTools = properties.$ai_tools

        const inputMessages = normalizeMessages(rawInput, 'user', aiTools)
        const outputMessages = normalizeMessages(rawOutput, 'assistant')

        const newInputs: CompatMessage[] = []
        for (const message of inputMessages) {
            const sig = messageSignature(message)
            if (!seenSignatures.has(sig)) {
                newInputs.push(message)
            }
            seenSignatures.add(sig)
        }
        for (const message of outputMessages) {
            seenSignatures.add(messageSignature(message))
        }

        return {
            trace,
            isLoaded: true,
            newInputs,
            outputs: outputMessages,
            userVisibleTurn,
            tools,
            errors,
        }
    })
}
