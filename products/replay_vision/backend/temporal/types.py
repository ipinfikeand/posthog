import datetime as dt
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from products.replay_vision.backend.models.replay_lens import LensModel, LensProvider
from products.replay_vision.backend.models.replay_observation import ObservationTrigger
from products.replay_vision.backend.temporal.constants import MAX_SESSION_ID_LENGTH


class ApplyLensInputs(BaseModel, frozen=True):
    """Input to ApplyLensWorkflow."""

    lens_id: UUID
    session_id: str = Field(min_length=1, max_length=MAX_SESSION_ID_LENGTH)
    team_id: int
    triggered_by: ObservationTrigger
    triggered_by_user_id: int | None = None
    # Snapshotted at workflow-start time so a lens-config edit mid-flight doesn't switch the model under us.
    model: LensModel
    provider: LensProvider


class CreateObservationInputs(BaseModel, frozen=True):
    lens_id: UUID
    team_id: int
    session_id: str = Field(min_length=1, max_length=MAX_SESSION_ID_LENGTH)
    triggered_by: ObservationTrigger
    triggered_by_user_id: int | None
    workflow_id: str


class CreateObservationOutput(BaseModel, frozen=True):
    """`was_created=False` means the row already existed; the caller should no-op."""

    observation_id: UUID
    was_created: bool


class MarkObservationRunningInputs(BaseModel, frozen=True):
    observation_id: UUID


class MarkObservationFailedInputs(BaseModel, frozen=True):
    observation_id: UUID
    error_reason: str


class FetchSessionEventsInputs(BaseModel, frozen=True):
    observation_id: UUID
    team_id: int
    session_id: str


class LensLlmInputs(BaseModel, frozen=True):
    """Per-session analytics events + recording metadata, stashed in Redis between activities."""

    session_id: str
    team_id: int
    session_start_time: dt.datetime
    session_end_time: dt.datetime
    duration_seconds: float
    columns: list[str]
    events: list[list[Any]]

    @model_validator(mode="after")
    def _events_match_columns(self) -> "LensLlmInputs":
        column_count = len(self.columns)
        for index, row in enumerate(self.events):
            if len(row) != column_count:
                raise ValueError(f"events[{index}] has {len(row)} values but columns has {column_count}")
        return self


class EnsureSessionAssetInputs(BaseModel, frozen=True):
    team_id: int
    session_id: str


class EnsureSessionAssetOutput(BaseModel, frozen=True):
    asset_id: int


class UploadVideoToGeminiInputs(BaseModel, frozen=True):
    asset_id: int
    observation_id: UUID  # used as the Gemini file's display_name for cleanup-sweep tracking


class UploadedVideo(BaseModel, frozen=True):
    file_uri: str
    mime_type: str
    gemini_file_name: str  # opaque ID for `files.delete`


class CallLensProviderInputs(BaseModel, frozen=True):
    lens_id: UUID
    team_id: int
    observation_id: UUID  # locates the LensLlmInputs blob in Redis
    file_uri: str
    mime_type: str
    model: LensModel


class LensCallOutput(BaseModel, frozen=True):
    """Result of one `call_lens_provider` invocation; `model_output` is the lens-specific dict (shape varies per `LensType`)."""

    model_output: dict[str, Any]
    model_used: str
    provider_used: str


class CleanupGeminiFileInputs(BaseModel, frozen=True):
    gemini_file_name: str


class MarkObservationSucceededInputs(BaseModel, frozen=True):
    observation_id: UUID
    model_used: str
    provider_used: str


class EmitLensEventInputs(BaseModel, frozen=True):
    """Payload for the `$replay_lens` capture; this is the only place lens output lives outside of ClickHouse."""

    observation_id: UUID
    model_output: dict[str, Any]
    model_used: str
    provider_used: str
