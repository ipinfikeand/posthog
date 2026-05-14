"""Single Gemini call per lens application; retries once on validation failure with the error fed back."""

from uuid import UUID

from django.conf import settings

import structlog
from asgiref.sync import sync_to_async
from google.genai import types
from posthoganalytics.ai.gemini import genai
from pydantic import BaseModel, ValidationError
from temporalio import activity
from temporalio.exceptions import ApplicationError

from posthog.models import Team

from products.replay_vision.backend.models.replay_lens import ReplayLens
from products.replay_vision.backend.temporal.lenses import lens_from_db
from products.replay_vision.backend.temporal.lenses.base import BaseLens
from products.replay_vision.backend.temporal.state import (
    StateActivitiesEnum,
    get_data_class_from_redis,
    get_redis_state_client,
)
from products.replay_vision.backend.temporal.types import CallLensProviderInputs, LensCallOutput, LensLlmInputs

logger = structlog.get_logger(__name__)

# One re-prompt with the validation error appended; matches the master plan ("one constrained re-prompt").
_MAX_LLM_ATTEMPTS = 2

_PROVIDER_FOR_GENAI = "google"


@activity.defn
async def call_lens_provider_activity(inputs: CallLensProviderInputs) -> LensCallOutput:
    """Run the lens against the uploaded video + cached events; validate, finalize, return the output."""
    lens = await sync_to_async(_load_lens)(inputs.lens_id, inputs.team_id)
    team_name = await sync_to_async(_load_team_name)(inputs.team_id)
    llm_inputs = await _load_llm_inputs(inputs.observation_id)

    prompt_text = lens.build_prompt(team_name=team_name, columns=llm_inputs.columns, events=llm_inputs.events)
    prompt_parts: list[types.Part] = [
        types.Part(file_data=types.FileData(file_uri=inputs.file_uri, mime_type=inputs.mime_type)),
        types.Part(text=prompt_text),
    ]

    finalized = await _call_with_retry(
        lens=lens, model=inputs.model.value, prompt_parts=prompt_parts, team_id=inputs.team_id
    )
    return LensCallOutput(
        model_output=finalized.model_dump(),
        model_used=inputs.model.value,
        provider_used=_PROVIDER_FOR_GENAI,
    )


def _load_lens(lens_id: UUID, team_id: int) -> BaseLens:
    row = ReplayLens.objects.filter(pk=lens_id, team_id=team_id).first()
    if row is None:
        raise ApplicationError(f"ReplayLens {lens_id} not found for team {team_id}", non_retryable=True)
    return lens_from_db(row)


def _load_team_name(team_id: int) -> str:
    return Team.objects.values_list("name", flat=True).get(pk=team_id)


async def _load_llm_inputs(observation_id: UUID) -> LensLlmInputs:
    redis_client, redis_key = get_redis_state_client(
        label=StateActivitiesEnum.SESSION_EVENTS,
        state_id=str(observation_id),
    )
    payload = await get_data_class_from_redis(redis_client, redis_key, target_class=LensLlmInputs)
    if payload is None:
        raise ApplicationError(f"LensLlmInputs missing in Redis for observation {observation_id}", non_retryable=True)
    return payload


async def _call_with_retry(*, lens: BaseLens, model: str, prompt_parts: list[types.Part], team_id: int) -> BaseModel:
    """One Gemini call, plus at most one retry that appends the validation error to the prompt."""
    client = genai.AsyncClient(api_key=settings.GEMINI_API_KEY)
    schema_class = lens.llm_response_schema
    parts = list(prompt_parts)
    last_error: str | None = None

    for attempt in range(_MAX_LLM_ATTEMPTS):
        response = await client.models.generate_content(
            model=f"models/{model}",
            contents=parts,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_json_schema=schema_class.model_json_schema(),
            ),
            posthog_distinct_id=f"replay-vision:{team_id}",
            posthog_groups={"project": str(team_id)},
        )
        response_text = (response.text or "").strip()
        if not response_text:
            last_error = "Empty response from model"
        else:
            try:
                parsed = schema_class.model_validate_json(response_text)
            except ValidationError as e:
                last_error = f"Schema validation failed: {e}"
            else:
                finalized = lens.finalize(parsed)
                semantic_error = lens.validate_semantics(finalized)
                if semantic_error is None:
                    return finalized
                last_error = f"Semantic validation failed: {semantic_error}"

        logger.warning(
            "replay_vision.call_lens_provider.invalid_response",
            attempt=attempt + 1,
            error=last_error,
            response_preview=response_text[:500] if response_text else None,
        )
        if attempt < _MAX_LLM_ATTEMPTS - 1:
            parts = [
                *parts,
                types.Part(text=f"\n\nYour previous attempt failed: {last_error}\nPlease fix your output."),
            ]

    raise ApplicationError(
        f"Lens call rejected after {_MAX_LLM_ATTEMPTS} attempts: {last_error}",
        non_retryable=True,
    )


# TODO: indexer's `emit_index_embeddings` side effect is not yet implemented (Phase 4 / embedding worker integration).
__all__ = ["call_lens_provider_activity"]
