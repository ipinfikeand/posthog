import asyncio
import datetime as dt
from uuid import UUID

import temporalio.workflow as wf
from temporalio import common
from temporalio.common import SearchAttributePair, TypedSearchAttributes, WorkflowIDReusePolicy

from posthog.temporal.common.base import PostHogWorkflow
from posthog.temporal.common.search_attributes import POSTHOG_SESSION_RECORDING_ID_KEY, POSTHOG_TEAM_ID_KEY
from posthog.temporal.session_replay.rasterize_recording.types import RasterizeRecordingInputs

with wf.unsafe.imports_passed_through():
    from django.conf import settings

from products.replay_vision.backend.temporal.activities import (
    call_lens_provider_activity,
    cleanup_gemini_file_activity,
    create_observation_activity,
    emit_lens_event_activity,
    ensure_session_asset_activity,
    fetch_session_events_activity,
    mark_observation_failed_activity,
    mark_observation_running_activity,
    mark_observation_succeeded_activity,
    upload_video_to_gemini_activity,
)
from products.replay_vision.backend.temporal.constants import APPLY_LENS_WORKFLOW_NAME
from products.replay_vision.backend.temporal.types import (
    ApplyLensInputs,
    CallLensProviderInputs,
    CleanupGeminiFileInputs,
    CreateObservationInputs,
    CreateObservationOutput,
    EmitLensEventInputs,
    EnsureSessionAssetInputs,
    EnsureSessionAssetOutput,
    FetchSessionEventsInputs,
    LensCallOutput,
    MarkObservationFailedInputs,
    MarkObservationRunningInputs,
    MarkObservationSucceededInputs,
    UploadedVideo,
    UploadVideoToGeminiInputs,
)

_STATE_ACTIVITY_RETRY = common.RetryPolicy(
    initial_interval=dt.timedelta(seconds=1),
    maximum_interval=dt.timedelta(seconds=10),
    maximum_attempts=5,
)

# Create's `ValueError` paths (lens missing, user not in org) won't recover on retry.
_CREATE_OBSERVATION_RETRY = common.RetryPolicy(
    initial_interval=dt.timedelta(seconds=1),
    maximum_interval=dt.timedelta(seconds=10),
    maximum_attempts=5,
    non_retryable_error_types=["ValueError"],
)

_FETCH_RETRY = common.RetryPolicy(
    initial_interval=dt.timedelta(seconds=2),
    maximum_interval=dt.timedelta(seconds=30),
    maximum_attempts=3,
)

# Asset get-or-create has no transient failure modes worth retrying.
_ENSURE_ASSET_RETRY = common.RetryPolicy(maximum_attempts=1)

# Upload + provider call: short retries; both are external-IO sensitive but in-activity logic also handles re-prompts.
_UPLOAD_RETRY = common.RetryPolicy(
    initial_interval=dt.timedelta(seconds=2),
    maximum_interval=dt.timedelta(seconds=30),
    maximum_attempts=3,
)

# Cleanup is best-effort; the cleanup sweep handles persistent failures.
_CLEANUP_RETRY = common.RetryPolicy(maximum_attempts=2)


@wf.defn(name=APPLY_LENS_WORKFLOW_NAME)
class ApplyLensWorkflow(PostHogWorkflow):
    """Apply one lens to one session: create row → fetch+rasterize → upload → call provider → emit event → mark succeeded."""

    inputs_cls = ApplyLensInputs

    @wf.run
    async def run(self, inputs: ApplyLensInputs) -> None:
        workflow_id = wf.info().workflow_id

        create_result: CreateObservationOutput = await wf.execute_activity(
            create_observation_activity,
            CreateObservationInputs(
                lens_id=inputs.lens_id,
                team_id=inputs.team_id,
                session_id=inputs.session_id,
                triggered_by=inputs.triggered_by,
                triggered_by_user_id=inputs.triggered_by_user_id,
                workflow_id=workflow_id,
            ),
            start_to_close_timeout=dt.timedelta(seconds=30),
            retry_policy=_CREATE_OBSERVATION_RETRY,
        )
        if not create_result.was_created:
            return  # Existing observation owns this (lens, session_id); its workflow drives it.

        observation_id = create_result.observation_id
        await wf.execute_activity(
            mark_observation_running_activity,
            MarkObservationRunningInputs(observation_id=observation_id),
            start_to_close_timeout=dt.timedelta(seconds=30),
            retry_policy=_STATE_ACTIVITY_RETRY,
        )

        uploaded: UploadedVideo | None = None
        try:
            asset_result = await self._fetch_and_ensure_asset(inputs, observation_id)
            await self._run_rasterize_child(inputs, asset_result.asset_id)
            uploaded = await wf.execute_activity(
                upload_video_to_gemini_activity,
                UploadVideoToGeminiInputs(asset_id=asset_result.asset_id, observation_id=observation_id),
                start_to_close_timeout=dt.timedelta(minutes=10),
                retry_policy=_UPLOAD_RETRY,
            )
            call_output: LensCallOutput = await wf.execute_activity(
                call_lens_provider_activity,
                CallLensProviderInputs(
                    lens_id=inputs.lens_id,
                    team_id=inputs.team_id,
                    observation_id=observation_id,
                    file_uri=uploaded.file_uri,
                    mime_type=uploaded.mime_type,
                    model=inputs.model,
                ),
                start_to_close_timeout=dt.timedelta(minutes=5),
                retry_policy=_UPLOAD_RETRY,
            )
            await wf.execute_activity(
                emit_lens_event_activity,
                EmitLensEventInputs(
                    observation_id=observation_id,
                    model_output=call_output.model_output,
                    model_used=call_output.model_used,
                    provider_used=call_output.provider_used,
                ),
                start_to_close_timeout=dt.timedelta(seconds=30),
                retry_policy=_STATE_ACTIVITY_RETRY,
            )
            await wf.execute_activity(
                mark_observation_succeeded_activity,
                MarkObservationSucceededInputs(
                    observation_id=observation_id,
                    model_used=call_output.model_used,
                    provider_used=call_output.provider_used,
                ),
                start_to_close_timeout=dt.timedelta(seconds=30),
                retry_policy=_STATE_ACTIVITY_RETRY,
            )
        except Exception as e:
            await self._mark_failed(observation_id, f"{type(e).__name__}: {e}")
            raise
        finally:
            if uploaded is not None:
                await wf.execute_activity(
                    cleanup_gemini_file_activity,
                    CleanupGeminiFileInputs(gemini_file_name=uploaded.gemini_file_name),
                    start_to_close_timeout=dt.timedelta(seconds=30),
                    retry_policy=_CLEANUP_RETRY,
                )

    async def _fetch_and_ensure_asset(self, inputs: ApplyLensInputs, observation_id: UUID) -> EnsureSessionAssetOutput:
        fetch_task = wf.execute_activity(
            fetch_session_events_activity,
            FetchSessionEventsInputs(
                observation_id=observation_id,
                team_id=inputs.team_id,
                session_id=inputs.session_id,
            ),
            start_to_close_timeout=dt.timedelta(minutes=2),
            retry_policy=_FETCH_RETRY,
        )
        asset_task = wf.execute_activity(
            ensure_session_asset_activity,
            EnsureSessionAssetInputs(team_id=inputs.team_id, session_id=inputs.session_id),
            start_to_close_timeout=dt.timedelta(seconds=30),
            retry_policy=_ENSURE_ASSET_RETRY,
        )
        _, asset_result = await asyncio.gather(fetch_task, asset_task)
        return asset_result

    async def _run_rasterize_child(self, inputs: ApplyLensInputs, asset_id: int) -> None:
        # Per-lens child id so concurrent observations of the same session don't collide on WorkflowAlreadyStartedError.
        await wf.execute_child_workflow(
            "rasterize-recording",
            RasterizeRecordingInputs(exported_asset_id=asset_id),
            id=f"replay-vision-rasterize-{inputs.team_id}-{inputs.session_id}-{inputs.lens_id}",
            task_queue=settings.SESSION_REPLAY_TASK_QUEUE,
            retry_policy=common.RetryPolicy(maximum_attempts=int(settings.TEMPORAL_WORKFLOW_MAX_ATTEMPTS)),
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            execution_timeout=dt.timedelta(minutes=30),
            search_attributes=TypedSearchAttributes(
                search_attributes=[
                    SearchAttributePair(key=POSTHOG_TEAM_ID_KEY, value=inputs.team_id),
                    SearchAttributePair(key=POSTHOG_SESSION_RECORDING_ID_KEY, value=inputs.session_id),
                ]
            ),
        )

    async def _mark_failed(self, observation_id: UUID, error_reason: str) -> None:
        await wf.execute_activity(
            mark_observation_failed_activity,
            MarkObservationFailedInputs(observation_id=observation_id, error_reason=error_reason),
            start_to_close_timeout=dt.timedelta(seconds=30),
            retry_policy=_STATE_ACTIVITY_RETRY,
        )
