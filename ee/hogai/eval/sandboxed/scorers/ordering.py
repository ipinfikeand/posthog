"""Shared helpers for ordering- and tool-call-based scorers.

Two pieces every per-product scorer module ends up needing:

* ``extract_user_prompt`` — fish the original user prompt out of the
  task output. Used by relevancy / alignment scorers that ground their
  judgment in what the user actually asked.
* ``enumerate_tool_calls`` — chronological list of successful tool
  calls, with single-exec ``mcp__posthog__exec`` calls unwrapped to the
  inner tool name + parsed JSON args. Required for any scorer that
  needs to work in both ``mcp_mode=tools`` and ``mcp_mode=cli`` runs;
  without the unwrap, every cli-mode case looks like the agent never
  called the wrapped tool.

Promoted out of ``product_analytics/scorers.py`` so error-tracking and
future product modules can share the same logic instead of
re-implementing it (and silently breaking on cli-mode).
"""

from __future__ import annotations

import json
from typing import Any

from .deterministic import iter_successful_tool_calls, normalize_tool_name

__all__ = [
    "EXEC_TOOL_NAME",
    "INFO_SYNTHETIC_PREFIX",
    "enumerate_tool_calls",
    "extract_user_prompt",
    "parse_exec_command",
]


EXEC_TOOL_NAME = "exec"

# Synthetic prefix assigned to ``mcp__posthog__exec {command: "info <tool>"}``
# so scorers can treat the exec-wrapped ``info`` command and the per-tool
# ``ToolSearch(select:mcp__posthog__<tool>)`` as interchangeable
# "tool schema loaded" signals.
INFO_SYNTHETIC_PREFIX = "__info__:"


def extract_user_prompt(output: dict[str, Any] | None) -> str:
    """Return the original user prompt that drove this eval case.

    The eval harness doesn't surface the prompt on the task return dict,
    but ``parse_log(..., initial_prompt=eval_case.prompt)`` seeds it as
    the first user message in ``output["messages"]``. This helper checks
    a few common shapes (``output.prompt`` / ``output.input`` first) so
    scorers don't have to know where exactly the harness chose to put
    it. Returns an empty string when the prompt is unavailable; callers
    that need it should treat that as ``score=None``.
    """
    if not isinstance(output, dict):
        return ""
    for key in ("prompt", "input"):
        value = output.get(key)
        if isinstance(value, str) and value:
            return value
    messages = output.get("messages") or []
    for msg in messages:
        if msg.get("role") != "user":
            continue
        content = msg.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text", "")
                    if isinstance(text, str) and text:
                        return text
            # First user message may be tool_results in a multi-turn thread —
            # in that case keep scanning.
    return ""


def parse_exec_command(command: str) -> tuple[str, dict[str, Any]] | None:
    """Split a CLI-style ``exec`` command string into ``(virtual_name, input)``.

    Recognized shapes (produced by single-exec mode where the agent talks
    to the PostHog MCP through one ``exec`` tool):
      - ``"info <tool>"``                 → ``("__info__:<tool>", {})``
      - ``"call [--json] <tool> <json>"`` → ``("<tool>", parsed_json)``

    Anything else (``search``, ``tools``, ``schema``, malformed) returns
    ``None`` so the caller can fall back to emitting the raw ``exec``
    call — those commands aren't load-bearing for the ordering checks.
    """
    stripped = command.strip()
    if not stripped:
        return None

    head, _, rest = stripped.partition(" ")
    head = head.lower()

    if head == "info":
        tool = rest.strip().split(None, 1)[0] if rest.strip() else ""
        if tool:
            return (f"{INFO_SYNTHETIC_PREFIX}{tool}", {})
        return None

    if head == "call":
        rest = rest.strip()
        if rest.startswith("--json"):
            rest = rest[len("--json") :].lstrip()
        if not rest:
            return None
        tool, _, json_part = rest.partition(" ")
        tool = tool.strip()
        if not tool:
            return None
        json_part = json_part.strip()
        parsed: dict[str, Any] = {}
        if json_part:
            try:
                decoded = json.loads(json_part)
                if isinstance(decoded, dict):
                    parsed = decoded
            except json.JSONDecodeError:
                parsed = {}
        return (tool, parsed)

    return None


def enumerate_tool_calls(messages: list[dict[str, Any]]) -> list[tuple[int, str, dict[str, Any]]]:
    """Return chronological ``(position, normalized_name, tool_use)`` triples.

    Position is the index of the enclosing assistant message inside the
    flat ``messages`` list, which preserves execution order (``base.py``
    rebuilds the conversation in order, so message index ≈ time).
    Includes only successful calls — error results are skipped the same
    way ``iter_successful_tool_calls`` does.

    Unwraps single-exec ``mcp__posthog__exec`` calls: each
    ``call <tool> <json>`` becomes a synthetic
    ``(pos, <tool>, parsed_input)`` entry, and each ``info <tool>``
    becomes ``(pos, "__info__:<tool>", {})``. Ordering checks therefore
    don't care whether the agent talks to tools directly (per-tool MCP
    mode) or through the CLI wrapper (single-exec mode).
    """
    positions: dict[str, int] = {}
    for idx, msg in enumerate(messages):
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                call_id = block.get("id")
                if call_id and call_id not in positions:
                    positions[call_id] = idx

    ordered: list[tuple[int, str, dict[str, Any]]] = []
    for tool_use, _result in iter_successful_tool_calls(messages):
        call_id = tool_use.get("id", "")
        name = normalize_tool_name(tool_use.get("name"))
        pos = positions.get(call_id, -1)
        if name == EXEC_TOOL_NAME:
            tool_input = tool_use.get("input") or {}
            command = tool_input.get("command", "") if isinstance(tool_input, dict) else ""
            parsed = parse_exec_command(command)
            if parsed is not None:
                virtual_name, virtual_input = parsed
                synthetic_use = {
                    "id": tool_use.get("id"),
                    "name": virtual_name,
                    "input": virtual_input,
                }
                ordered.append((pos, virtual_name, synthetic_use))
                continue
        ordered.append((pos, name, tool_use))
    ordered.sort(key=lambda item: item[0])
    return ordered
