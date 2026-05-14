"""Base class for all Replay Vision lens types."""

import json
from typing import Any

from pydantic import BaseModel, Field

# TODO: replace with the real Signal side-mission prompt when Phase 4 lands.
_SIGNAL_SIDE_MISSION = ""

_PROMPT_TEMPLATE = """\
You are applying a configured lens to a recorded user session of {team_name}.

The video is the rasterized recording: 8x playback speed with inactive periods skipped. The events table below lists the analytics events captured during the session, in chronological order. Use both to ground your answer.

<lens_intent>
{user_prompt}
</lens_intent>

<task>
{task_instruction}
</task>

<events>
{events_json}
</events>
{signal_side_mission}
"""


class BaseLensOutput(BaseModel, frozen=True):
    """Final output shape persisted in `ReplayObservation.model_output` and emitted as `$replay_lens`."""

    confidence: float = Field(
        ge=0,
        le=1,
        description="Your confidence in this answer, 0 to 1. 0.5 means uncertain; 1.0 means absolutely sure.",
    )


class BaseLens(BaseModel, frozen=True):
    """Common shape for every concrete lens. Subclasses bind a `Literal` `lens_type` discriminator and override `task_instruction` + `llm_response_schema`."""

    prompt: str
    emits_signals: bool = False

    def task_instruction(self) -> str:
        """Lens-type-specific guidance — describes what to put in each output field."""
        raise NotImplementedError

    @property
    def llm_response_schema(self) -> type[BaseModel]:
        """Pydantic class the LLM emits — passed to Gemini's `response_json_schema`. May be dynamic per instance (scorer adds a numeric range; classifier could pin a tag set, etc.)."""
        raise NotImplementedError

    def finalize(self, llm_response: BaseModel) -> BaseLensOutput:
        """Build the final `BaseLensOutput` from the validated LLM response. Default: the LLM response IS the final output. Override to stamp config-derived fields the model shouldn't generate."""
        if not isinstance(llm_response, BaseLensOutput):
            raise TypeError(f"Expected BaseLensOutput, got {type(llm_response).__name__}")
        return llm_response

    def validate_semantics(self, output: BaseLensOutput) -> str | None:
        """Lens-specific checks beyond Pydantic schema validation (e.g. tag membership, score range). Return `None` when valid, otherwise a string suitable to feed back into a re-prompt."""
        return None

    def build_prompt(self, *, team_name: str, columns: list[str], events: list[list[Any]]) -> str:
        return _PROMPT_TEMPLATE.format(
            team_name=team_name,
            user_prompt=self.prompt,
            task_instruction=self.task_instruction(),
            events_json=_render_events(columns, events),
            signal_side_mission=f"\n{_SIGNAL_SIDE_MISSION}\n" if self.emits_signals and _SIGNAL_SIDE_MISSION else "",
        )


def _render_events(columns: list[str], events: list[list[Any]]) -> str:
    if not events:
        return "(no events captured during the session)"
    # Compact separators: Gemini parses fine without whitespace, and indent=2 burns thousands of prompt tokens.
    rendered = json.dumps([dict(zip(columns, row)) for row in events], separators=(",", ":"), default=str)
    # Escape `<` so a hostile event value can't forge the `</events>` closing tag and break out of the data block.
    return rendered.replace("<", "\\u003c")
