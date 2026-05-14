"""Emit the `$replay_lens` event with the lens output to ClickHouse via posthog capture."""

from uuid import UUID

import structlog
from asgiref.sync import sync_to_async
from temporalio import activity

from posthog.ph_client import ph_scoped_capture

from products.replay_vision.backend.models.replay_lens import ReplayLens
from products.replay_vision.backend.models.replay_observation import ObservationTrigger, ReplayObservation
from products.replay_vision.backend.temporal.types import EmitLensEventInputs

logger = structlog.get_logger(__name__)


@activity.defn
async def emit_lens_event_activity(inputs: EmitLensEventInputs) -> None:
    """Capture the `$replay_lens` event; this is the single source of truth for lens output."""
    observation, lens = await sync_to_async(_load_observation_and_lens)(inputs.observation_id)

    properties: dict = {
        "lens_id": str(lens.id),
        "lens_name": lens.name,
        "lens_type": lens.lens_type,
        "lens_version": observation.lens_version,
        "session_id": observation.session_id,
        "triggered_by": observation.triggered_by,
        "triggered_by_user_id": observation.triggered_by_user_id,
        "model_used": inputs.model_used,
        "provider_used": inputs.provider_used,
        # Flatten so HogQL can query individual output fields without a JSON extract.
        **{f"lens_output_{k}": v for k, v in inputs.model_output.items()},
    }
    distinct_id = (
        str(observation.triggered_by_user_id)
        if observation.triggered_by_user_id is not None and observation.triggered_by == ObservationTrigger.ON_DEMAND
        else f"replay-vision:{observation.team_id}"
    )

    with ph_scoped_capture() as capture:
        capture(
            distinct_id=distinct_id,
            event="$replay_lens",
            properties=properties,
            groups={"project": str(observation.team_id)},
        )


def _load_observation_and_lens(observation_id: UUID) -> tuple[ReplayObservation, ReplayLens]:
    observation = ReplayObservation.objects.select_related("lens").get(pk=observation_id)
    return observation, observation.lens
