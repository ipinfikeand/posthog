import json
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Optional

from django.conf import settings

import temporalio
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ParentClosePolicy

from posthog.temporal.common.base import PostHogWorkflow
from posthog.temporal.oauth import PosthogMcpScopes

from products.tasks.backend.services.sandbox import is_public_sandbox_repo
from products.tasks.backend.temporal.babysit_pr.activities.send_heartbeat_to_babysit import (
    SendHeartbeatToBabysitInput,
    send_heartbeat_to_babysit,
)
from products.tasks.backend.temporal.babysit_pr.activities.start_babysit_for_task import (
    StartBabysitForTaskInput,
    start_babysit_for_task,
)
from products.tasks.backend.temporal.create_snapshot.workflow import CreateSnapshotForRepositoryInput

from .activities.cleanup_sandbox import CleanupSandboxInput, cleanup_sandbox
from .activities.create_resume_snapshot import CreateResumeSnapshotInput, create_resume_snapshot
from .activities.emit_progress_activity import EmitProgressInput, emit_progress_activity
from .activities.execute_task_in_sandbox import ExecuteTaskOutput
from .activities.forward_pending_message import forward_pending_user_message
from .activities.get_sandbox_for_repository import GetSandboxForRepositoryOutput
from .activities.get_task_processing_context import (
    GetTaskProcessingContextInput,
    TaskProcessingContext,
    get_task_processing_context,
)
from .activities.post_slack_update import PostSlackUpdateInput, post_slack_update
from .activities.provision_sandbox import (
    CheckoutBranchInSandboxInput,
    CloneRepositoryInSandboxInput,
    CreateSandboxForRepositoryInput,
    InjectFreshTokensOnResumeInput,
    PrepareSandboxForRepositoryInput,
    checkout_branch_in_sandbox,
    clone_repository_in_sandbox,
    create_sandbox_for_repository,
    inject_fresh_tokens_on_resume,
    prepare_sandbox_for_repository,
)
from .activities.read_sandbox_logs import ReadSandboxLogsInput, read_sandbox_logs
from .activities.relay_sandbox_events import RelaySandboxEventsInput, relay_sandbox_events
from .activities.send_followup_to_sandbox import SendFollowupToSandboxInput, send_followup_to_sandbox
from .activities.start_agent_server import StartAgentServerInput, StartAgentServerOutput, start_agent_server
from .activities.track_workflow_event import TrackWorkflowEventInput, track_workflow_event
from .activities.update_task_run_status import UpdateTaskRunStatusInput, update_task_run_status


@dataclass
class ProcessTaskInput:
    run_id: str
    create_pr: bool = True
    slack_thread_context: Optional[dict[str, Any]] = None
    posthog_mcp_scopes: PosthogMcpScopes = "read_only"


@dataclass
class PendingFollowup:
    message: str | None
    artifact_ids: list[str]


@dataclass
class ProcessTaskOutput:
    success: bool
    task_result: Optional[ExecuteTaskOutput] = None
    error: Optional[str] = None
    sandbox_id: Optional[str] = None


class TaskEvent(StrEnum):
    SIGNAL_RECEIVED = "signal_received"
    TIMEOUT_REACHED = "timeout_reached"


# Default 2 hours in production. Override via TASKS_INACTIVITY_TIMEOUT_SECONDS
# for local testing (e.g. `TASKS_INACTIVITY_TIMEOUT_SECONDS=30` to force a fast
# shutdown for resume-flow testing).
INACTIVITY_TIMEOUT = timedelta(seconds=settings.TASKS_INACTIVITY_TIMEOUT_SECONDS or 2 * 60 * 60)
RELAY_SANDBOX_EVENTS_START_TO_CLOSE_TIMEOUT = timedelta(hours=24)
PENDING_MESSAGE_FORWARD_TIMEOUT_SECONDS = 180
# Throttle: at most one heartbeat is forwarded to the babysit workflow per this
# interval. Without throttling, every agent-active heartbeat (which can fire
# many times per minute during normal log activity) would schedule its own
# activity and signal RPC.
BABYSIT_HEARTBEAT_FORWARD_INTERVAL = timedelta(seconds=60)

_PATCH_ID_FOLLOWUP_QUEUE = "tasks-follow-up-message-queue"


@temporalio.workflow.defn(name="process-task")
class ProcessTaskWorkflow(PostHogWorkflow):
    def __init__(self) -> None:
        self._context: Optional[TaskProcessingContext] = None
        self._slack_thread_context: Optional[dict[str, Any]] = None
        self._posthog_mcp_scopes: PosthogMcpScopes = "read_only"
        self._sandbox_id_for_cleanup: Optional[str] = None
        self._task_completed: bool = False
        self._completion_status: str = "completed"
        self._completion_error: Optional[str] = None
        self._heartbeat_received: bool = False
        self._agent_active_since_last_forward: bool = False
        self._last_heartbeat_forward_at: Optional[datetime] = None
        self._pending_followup: PendingFollowup | None = None
        self._pending_followups: list[PendingFollowup] = []
        # Tracks which progress step is currently in-progress (step, label,
        # group) so we can emit a "failed" transition from the workflow-level
        # exception handler onto the right card.
        self._current_progress_step: Optional[tuple[str, str, str]] = None

    @property
    def context(self) -> TaskProcessingContext:
        if self._context is None:
            raise RuntimeError("context accessed before being set")
        return self._context

    @staticmethod
    def _should_skip_followup(message: str | None, artifact_ids: list[str]) -> bool:
        return not message and not artifact_ids

    @staticmethod
    def parse_inputs(inputs: list[str]) -> ProcessTaskInput:
        loaded = json.loads(inputs[0])
        return ProcessTaskInput(
            run_id=loaded["run_id"],
            create_pr=loaded.get("create_pr", True),
            slack_thread_context=loaded.get("slack_thread_context"),
            posthog_mcp_scopes=loaded.get("posthog_mcp_scopes", "read_only"),
        )

    @staticmethod
    def _activity_error_properties(error: Exception) -> dict[str, Any]:
        if not isinstance(error, temporalio.exceptions.ActivityError):
            return {}

        retry_state = error.retry_state
        properties: dict[str, Any] = {
            "temporal_activity_id": error.activity_id,
            "temporal_activity_type": error.activity_type,
            "temporal_activity_identity": error.identity,
            "temporal_activity_retry_state": retry_state.name if retry_state else None,
            "temporal_activity_scheduled_event_id": error.scheduled_event_id,
            "temporal_activity_started_event_id": error.started_event_id,
        }

        if error.cause:
            properties.update(
                {
                    "cause_error_type": type(error.cause).__name__,
                    "cause_error_message": str(error.cause)[:500],
                }
            )

        return properties

    async def _wait_for_task_external_event(self):
        await workflow.wait_condition(
            lambda: (
                self._task_completed
                or self._heartbeat_received
                or self._pending_followup is not None
                or len(self._pending_followups) > 0
            )
        )
        return TaskEvent.SIGNAL_RECEIVED

    async def _wait_for_inactivity(self, timeout: timedelta = INACTIVITY_TIMEOUT):
        await workflow.sleep(timeout.total_seconds())
        return TaskEvent.TIMEOUT_REACHED

    async def _wait_for_event(self) -> TaskEvent:
        possible_events: list[asyncio.Task[TaskEvent]] = [
            asyncio.create_task(self._wait_for_task_external_event()),
            asyncio.create_task(self._wait_for_inactivity(INACTIVITY_TIMEOUT)),
        ]
        done, pending = await workflow.wait(possible_events, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        pending_tasks_results = await asyncio.gather(
            *pending, return_exceptions=True
        )  # Ensure all pending tasks are cancelled
        for task in done:
            if task.exception():
                workflow.logger.warning(
                    "Event wait task failed",
                    run_id=self.context.run_id,
                    error=str(task.exception()),
                )
                continue
            return task.result()
        for task_result in pending_tasks_results:
            if isinstance(task_result, Exception):
                workflow.logger.warning(
                    "Pending event wait task failed during cancellation",
                    run_id=self.context.run_id,
                    error=str(task_result),
                )
            if isinstance(task_result, TaskEvent):
                workflow.logger.info(
                    "Pending event wait task completed during cancellation",
                    run_id=self.context.run_id,
                    event=task_result.value,
                )
                return task_result
        raise RuntimeError("No event was completed successfully")

    @workflow.run
    async def run(self, input: ProcessTaskInput) -> ProcessTaskOutput:
        sandbox_id = None
        sandbox_cleaned = False
        timed_out = False
        run_id = input.run_id
        self._sandbox_id_for_cleanup = None
        self._slack_thread_context = input.slack_thread_context
        try:
            self._context = await self._get_task_processing_context(input)
            self._posthog_mcp_scopes = input.posthog_mcp_scopes
            await self._update_task_run_status("in_progress")

            # Spawn (or signal) the per-Task PR babysit workflow as soon as we
            # know the context. Done before sandbox setup so a misbehaving
            # provisioning step doesn't prevent babysitting from starting.
            await self._start_babysit_for_task()

            # Announce the first progress step immediately so the desktop card
            # shows up before any provisioning log lines arrive.
            await self._emit_progress("sandbox", "in_progress", "Setting up sandbox", "setup")

            await self._track_workflow_event(
                "task_run_started",
                {
                    "run_id": run_id,
                    "task_id": self.context.task_id,
                    "repository": self.context.repository,
                    "team_id": self.context.team_id,
                },
            )

            await self._post_slack_update()

            sandbox_output = await self._get_sandbox_for_repository()
            sandbox_id = sandbox_output.sandbox_id

            # TODO(tasks): Re-enable snapshot creation
            # if sandbox_output.should_create_snapshot and self.context.repository and self.context.github_integration_id:
            #     await self._trigger_snapshot_workflow()

            await self._post_slack_update()

            # Start agent-server for direct connection from PostHog Code
            await self._emit_progress("agent", "in_progress", "Starting agent", "setup")
            agent_server_output = await self._start_agent_server(sandbox_output)
            await self._emit_progress("agent", "completed", "Started agent", "setup")

            await self._track_workflow_event(
                "sandbox_started",
                {
                    "run_id": run_id,
                    "task_id": self.context.task_id,
                    "sandbox_id": sandbox_id,
                    "sandbox_url": agent_server_output.sandbox_url,
                    "used_snapshot": sandbox_output.used_snapshot,
                    "repository": self.context.repository,
                },
            )

            relay_task = asyncio.ensure_future(self._relay_sandbox_events(agent_server_output, sandbox_id=sandbox_id))

            if self._should_forward_pending_user_message():
                await self._forward_pending_user_message()

            # Wait for completion signal or inactivity timeout.
            # Heartbeat signals reset the inactivity timer, keeping the workflow alive
            # as long as the agent is actively producing logs.
            while not self._task_completed:
                event = await self._wait_for_event()
                match event:
                    case TaskEvent.TIMEOUT_REACHED:
                        timed_out = True
                        break
                    case TaskEvent.SIGNAL_RECEIVED:
                        pending_followup_count = len(self._pending_followups) + (
                            1 if self._pending_followup is not None else 0
                        )
                        if pending_followup_count > 0:
                            workflow.logger.info(
                                "Pending follow-up message received, sending to sandbox",
                                run_id=self.context.run_id,
                                pending_followup_count=pending_followup_count,
                            )
                            if self._pending_followup is not None:
                                pending_followup = self._pending_followup
                                self._pending_followup = None
                            else:
                                pending_followup = self._pending_followups.pop(0)
                            message = pending_followup.message
                            artifact_ids = pending_followup.artifact_ids
                            if self._should_skip_followup(message, artifact_ids):
                                workflow.logger.warning(
                                    "empty_followup_skipped",
                                    run_id=self.context.run_id,
                                )
                                continue

                            await self._send_followup_to_sandbox(
                                message=message,
                                artifact_ids=artifact_ids,
                            )
                            continue

                        if self._heartbeat_received and not self._task_completed:
                            workflow.logger.info(
                                "Heartbeat received, resetting inactivity timer", run_id=self.context.run_id
                            )
                            self._heartbeat_received = False
                            if self._agent_active_since_last_forward:
                                self._agent_active_since_last_forward = False
                                if self._should_forward_heartbeat_to_babysit():
                                    self._last_heartbeat_forward_at = workflow.now()
                                    await self._forward_heartbeat_to_babysit()
                            continue
                    case _:
                        raise ValueError(f"Unknown event type: {event}")

            # Stop the relay now that the main loop is done
            await self._cancel_relay(relay_task)

            if self._task_completed:
                await self._update_task_run_status(self._completion_status, error_message=self._completion_error)
            elif timed_out:
                await self._update_task_run_status("completed", error_message="Run timed out due to inactivity")

            await self._post_slack_update()

            return ProcessTaskOutput(
                success=True,
                task_result=None,
                error=None,
                sandbox_id=sandbox_id,
            )

        except asyncio.CancelledError:
            current_sandbox_id = sandbox_id or self._sandbox_id_for_cleanup
            if self._context:
                if self._current_progress_step is not None:
                    failed_step, failed_label, failed_group = self._current_progress_step
                    await self._emit_progress(
                        failed_step,
                        "failed",
                        failed_label,
                        failed_group,
                        detail="Cancelled",
                    )
                await self._track_workflow_event(
                    "task_run_cancelled",
                    {
                        "run_id": run_id,
                        "task_id": self.context.task_id,
                        "repository": self.context.repository,
                        "team_id": self.context.team_id,
                    },
                )
            await self._update_task_run_status("cancelled", run_id=run_id)
            if current_sandbox_id:
                await self._cleanup_sandbox(current_sandbox_id)
                sandbox_id = None
                self._sandbox_id_for_cleanup = None
            raise

        except Exception as e:
            current_sandbox_id = sandbox_id or self._sandbox_id_for_cleanup
            error_message = str(e)[:500]
            if self._context:
                if self._current_progress_step is not None:
                    failed_step, failed_label, failed_group = self._current_progress_step
                    await self._emit_progress(
                        failed_step,
                        "failed",
                        failed_label,
                        failed_group,
                        detail=error_message[:200],
                    )
                await self._track_workflow_event(
                    "task_run_failed",
                    {
                        "run_id": run_id,
                        "task_id": self.context.task_id,
                        "repository": self.context.repository,
                        "origin_product": self.context.origin_product,
                        "environment": self.context.environment,
                        "mode": self.context.mode,
                        "run_source": self.context.run_source,
                        "runtime_adapter": self.context.runtime_adapter,
                        "provider": self.context.provider,
                        "model": self.context.model,
                        "reasoning_effort": self.context.reasoning_effort,
                        "error_type": type(e).__name__,
                        "error_message": error_message,
                        "sandbox_id": current_sandbox_id,
                        **self._activity_error_properties(e),
                    },
                )
            await self._update_task_run_status("failed", error_message=error_message, run_id=run_id)
            if self._context:
                await self._post_slack_update()

            return ProcessTaskOutput(
                success=False,
                task_result=None,
                error=error_message,
                sandbox_id=current_sandbox_id,
            )

        finally:
            cleanup_sandbox_id = sandbox_id or self._sandbox_id_for_cleanup
            if cleanup_sandbox_id:
                # When `use_modal_resume_snapshots` is off, resume relies on the
                # agent server's git-checkpoint mechanism instead. Read from
                # context (captured at workflow start) so replay is deterministic
                # against env-var flips.
                if self._context and self._context.mode == "interactive" and self._context.use_modal_resume_snapshots:
                    await self._create_resume_snapshot(cleanup_sandbox_id)

                await self._read_sandbox_logs(cleanup_sandbox_id)
                await self._cleanup_sandbox(cleanup_sandbox_id)
                sandbox_cleaned = True
                self._sandbox_id_for_cleanup = None

            if sandbox_cleaned and self._slack_thread_context and self._context:
                await self._post_slack_update(sandbox_cleaned=True)

    async def _get_task_processing_context(self, input: ProcessTaskInput) -> TaskProcessingContext:
        return await workflow.execute_activity(
            get_task_processing_context,
            GetTaskProcessingContextInput(run_id=input.run_id, create_pr=input.create_pr),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    async def _get_sandbox_for_repository(self) -> GetSandboxForRepositoryOutput:
        prepared = await workflow.execute_activity(
            prepare_sandbox_for_repository,
            PrepareSandboxForRepositoryInput(context=self.context),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        created = await workflow.execute_activity(
            create_sandbox_for_repository,
            CreateSandboxForRepositoryInput(context=self.context, prepared=prepared),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        self._sandbox_id_for_cleanup = created.sandbox_id
        if prepared.used_snapshot:
            await self._emit_progress(
                "sandbox",
                "completed",
                "Restored sandbox",
                "setup",
                detail="Resumed from a previous snapshot",
            )
        else:
            await self._emit_progress("sandbox", "completed", "Set up sandbox", "setup")

        # Resuming from a filesystem snapshot carries the previous run's
        # credentials baked into .git/config and any agentsh env file — refresh
        # them before any sandbox command (diagnostics, fetch, checkout) runs.
        if prepared.snapshot_external_id:
            await workflow.execute_activity(
                inject_fresh_tokens_on_resume,
                InjectFreshTokensOnResumeInput(
                    context=self.context,
                    sandbox_id=created.sandbox_id,
                    repository=prepared.repository,
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )

        can_clone_without_integration = is_public_sandbox_repo(prepared.repository)
        has_clone_credentials = self.context.has_github_credentials or can_clone_without_integration

        will_clone = bool(prepared.repository and not prepared.used_snapshot and has_clone_credentials)
        will_checkout = bool(prepared.repository and prepared.branch and has_clone_credentials)

        if will_clone:
            await self._emit_progress("clone", "in_progress", "Cloning repository", "setup")
            await workflow.execute_activity(
                clone_repository_in_sandbox,
                CloneRepositoryInSandboxInput(
                    context=self.context,
                    sandbox_id=created.sandbox_id,
                    repository=prepared.repository,
                    github_token=prepared.github_token,
                    shallow_clone=prepared.shallow_clone,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            await self._emit_progress("clone", "completed", "Cloned repository", "setup")

        state = self.context.state or {}
        is_resume = bool(state.get("resume_from_run_id") or state.get("handoff_resumed"))
        if will_checkout and not is_resume:
            branch_label_active = f"Checking out branch {prepared.branch}"
            branch_label_done = f"Checked out branch {prepared.branch}"
            await self._emit_progress("checkout", "in_progress", branch_label_active, "setup")
            await workflow.execute_activity(
                checkout_branch_in_sandbox,
                CheckoutBranchInSandboxInput(
                    context=self.context,
                    sandbox_id=created.sandbox_id,
                    repository=prepared.repository,
                    branch=prepared.branch,
                    github_token=prepared.github_token,
                    shallow_clone=prepared.shallow_clone,
                    used_snapshot=prepared.used_snapshot,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            await self._emit_progress("checkout", "completed", branch_label_done, "setup")

        return GetSandboxForRepositoryOutput(
            sandbox_id=created.sandbox_id,
            sandbox_url=created.sandbox_url,
            connect_token=created.connect_token,
            used_snapshot=prepared.used_snapshot,
            should_create_snapshot=prepared.should_create_snapshot,
        )

    async def _cleanup_sandbox(self, sandbox_id: str) -> None:
        cleanup_input = CleanupSandboxInput(sandbox_id=sandbox_id)
        await workflow.execute_activity(
            cleanup_sandbox,
            cleanup_input,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    async def _read_sandbox_logs(self, sandbox_id: str) -> None:
        try:
            logs = await workflow.execute_activity(
                read_sandbox_logs,
                ReadSandboxLogsInput(sandbox_id=sandbox_id, run_id=self.context.run_id),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            if logs:
                workflow.logger.info(f"Agent-server logs from sandbox {sandbox_id}:\n{logs}")
        except Exception as e:
            workflow.logger.warning(f"Failed to read sandbox logs: {e}")

    async def _start_agent_server(self, sandbox_output: GetSandboxForRepositoryOutput) -> StartAgentServerOutput:
        return await workflow.execute_activity(
            start_agent_server,
            StartAgentServerInput(
                context=self.context,
                sandbox_id=sandbox_output.sandbox_id,
                sandbox_url=sandbox_output.sandbox_url,
                sandbox_connect_token=sandbox_output.connect_token,
                posthog_mcp_scopes=self._posthog_mcp_scopes,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    async def _forward_pending_user_message(self) -> None:
        await workflow.execute_activity(
            forward_pending_user_message,
            self.context.run_id,
            start_to_close_timeout=timedelta(seconds=PENDING_MESSAGE_FORWARD_TIMEOUT_SECONDS),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

    def _should_forward_pending_user_message(self) -> bool:
        if not self._context:
            return False

        state = self.context.state or {}
        is_resume = bool(state.get("resume_from_run_id") or state.get("handoff_resumed"))
        return self.context.mode != "interactive" and not is_resume

    async def _track_workflow_event(self, event_name: str, properties: dict) -> None:
        track_input = TrackWorkflowEventInput(
            event_name=event_name,
            distinct_id=self.context.distinct_id,
            properties=properties,
            groups={
                "organization": self.context.organization_id,
                "project": self.context.team_uuid,
            },
        )
        await workflow.execute_activity(
            track_workflow_event,
            track_input,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

    async def _emit_progress(
        self,
        step: str,
        status: str,
        label: str,
        group: str,
        detail: Optional[str] = None,
    ) -> None:
        """Emit a structured progress notification. Best-effort.

        The caller-supplied `group` is scoped with the workflow's run id so
        cards never collide across workflow executions (retries, resumes). The
        scoped id is what actually goes on the wire — callers don't need to
        think about uniqueness.
        """
        scoped_group = f"{group}:{self.context.run_id}"
        try:
            await workflow.execute_activity(
                emit_progress_activity,
                EmitProgressInput(
                    run_id=self.context.run_id,
                    step=step,
                    status=status,
                    label=label,
                    group=scoped_group,
                    detail=detail,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            if status == "in_progress":
                self._current_progress_step = (step, label, group)
            elif status in {"completed", "failed"}:
                if self._current_progress_step and self._current_progress_step[0] == step:
                    self._current_progress_step = None
        except Exception as e:
            workflow.logger.warning(
                "emit_progress_failed",
                run_id=self.context.run_id,
                step=step,
                status=status,
                error=str(e),
            )

    async def _update_task_run_status(
        self, status: str, error_message: Optional[str] = None, run_id: Optional[str] = None
    ) -> None:
        await workflow.execute_activity(
            update_task_run_status,
            UpdateTaskRunStatusInput(
                run_id=run_id if run_id is not None else self.context.run_id,
                status=status,
                error_message=error_message,
            ),
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    async def _relay_sandbox_events(
        self, agent_server_output: StartAgentServerOutput, sandbox_id: str | None = None
    ) -> None:
        """Start the SSE relay activity as a concurrent task (best-effort)."""
        try:
            relay_input = RelaySandboxEventsInput(
                run_id=self.context.run_id,
                task_id=self.context.task_id,
                sandbox_url=agent_server_output.sandbox_url,
                sandbox_connect_token=agent_server_output.connect_token,
                team_id=self.context.team_id,
                distinct_id=self.context.distinct_id,
                sandbox_id=sandbox_id,
            )
            await workflow.execute_activity(
                relay_sandbox_events,
                relay_input,
                start_to_close_timeout=RELAY_SANDBOX_EVENTS_START_TO_CLOSE_TIMEOUT,
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=1),
                cancellation_type=workflow.ActivityCancellationType.TRY_CANCEL,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            workflow.logger.warning(
                "relay_sandbox_events_failed_non_fatal",
                run_id=self.context.run_id,
                error=str(e),
            )

    @staticmethod
    async def _cancel_relay(relay_task: "asyncio.Task[None]") -> None:
        """Cancel the relay task if still running."""
        if relay_task.done():
            return
        relay_task.cancel()
        try:
            await relay_task
        except (asyncio.CancelledError, Exception):
            pass

    async def _create_resume_snapshot(self, sandbox_id: str) -> None:
        """Create a filesystem snapshot for interactive sandbox resume."""
        try:
            result = await workflow.execute_activity(
                create_resume_snapshot,
                CreateResumeSnapshotInput(sandbox_id=sandbox_id, run_id=self.context.run_id),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            if result.external_id:
                workflow.logger.info(f"Resume snapshot created: {result.external_id} for sandbox {sandbox_id}")
            elif result.error:
                workflow.logger.warning(f"Resume snapshot skipped: {result.error}")
        except Exception as e:
            workflow.logger.warning(f"Resume snapshot failed (non-fatal): {e}")

    async def _trigger_snapshot_workflow(self) -> None:
        github_integration_id = self.context.github_integration_id
        repository = self.context.repository
        if github_integration_id is None or repository is None:
            workflow.logger.info("Skipping snapshot workflow — no repository configured")
            return

        workflow_id = f"create-snapshot-for-repository-{github_integration_id}-{repository.replace('/', '-')}"

        await workflow.start_child_workflow(
            workflow="create-snapshot-for-repository",
            arg=CreateSnapshotForRepositoryInput(
                github_integration_id=github_integration_id,
                repository=repository,
                team_id=self.context.team_id,
            ),
            id=workflow_id,
            task_queue=settings.TASKS_TASK_QUEUE,
            parent_close_policy=ParentClosePolicy.ABANDON,  # This will allow the snapshot workflow to continue even if the task workflow fails or closes
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

    async def _post_slack_update(self, sandbox_cleaned: bool = False) -> None:
        if not self._slack_thread_context:
            return
        await workflow.execute_activity(
            post_slack_update,
            PostSlackUpdateInput(
                run_id=self.context.run_id,
                slack_thread_context=self._slack_thread_context,
                sandbox_cleaned=sandbox_cleaned,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

    @temporalio.workflow.signal
    async def complete_task(self, status: str = "completed", error_message: Optional[str] = None) -> None:
        self._completion_status = status
        self._completion_error = error_message
        self._task_completed = True

    @temporalio.workflow.signal
    async def heartbeat(self, agent_active: bool = False) -> None:
        self._heartbeat_received = True
        if agent_active:
            # Latched until the main loop forwards (throttled) to babysit.
            self._agent_active_since_last_forward = True

    @temporalio.workflow.signal
    async def send_followup_message(self, message: str | None = None, artifact_ids: Optional[list[str]] = None) -> None:
        # Log signal arrival so we can correlate it with the adapter's "begin dispatch"
        # log below — gaps between the two point at workflow-loop backpressure.
        context = self._context
        workflow.logger.info(
            "send_followup_signal_received",
            run_id=context.run_id if context is not None else None,
            message_length=len(message or ""),
            artifact_count=len(artifact_ids or []),
        )
        pending_followup = PendingFollowup(message=message, artifact_ids=artifact_ids or [])
        if workflow.patched(_PATCH_ID_FOLLOWUP_QUEUE):
            self._pending_followups.append(pending_followup)
        else:
            self._pending_followup = pending_followup

    async def _send_followup_to_sandbox(self, message: str | None, artifact_ids: list[str]) -> None:
        workflow.logger.info(
            "send_followup_dispatch_begin",
            run_id=self.context.run_id,
            message_length=len(message or ""),
            artifact_count=len(artifact_ids),
        )
        try:
            await workflow.execute_activity(
                send_followup_to_sandbox,
                SendFollowupToSandboxInput(
                    run_id=self.context.run_id,
                    message=message,
                    posthog_mcp_scopes=self._posthog_mcp_scopes,
                    artifact_ids=artifact_ids,
                ),
                start_to_close_timeout=timedelta(minutes=35),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        except Exception as e:
            workflow.logger.warning(
                "send_followup_to_sandbox_failed",
                run_id=self.context.run_id,
                error=str(e),
            )
            # Mark the run as failed so poll_for_turn sees a terminal status
            # immediately instead of waiting for the inactivity timeout.
            self._completion_status = "failed"
            self._completion_error = f"Follow-up delivery failed: {e}"
            self._task_completed = True

    async def _start_babysit_for_task(self) -> None:
        try:
            await workflow.execute_activity(
                start_babysit_for_task,
                StartBabysitForTaskInput(context=self.context),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except Exception as e:
            # Babysitting is best-effort; failing to start it shouldn't take
            # down the run. Log loudly so we notice in metrics.
            workflow.logger.warning(
                "start_babysit_for_task_failed",
                run_id=self.context.run_id,
                error=str(e),
            )

    def _should_forward_heartbeat_to_babysit(self) -> bool:
        ctx = self._context
        if ctx is None:
            return False
        # Mirrors the gating in start_babysit_for_task: if babysit wasn't
        # started, no signal target exists and we'd just be generating noise.
        if not ctx.create_pr or not ctx.pr_loop_enabled or not ctx.has_github_credentials:
            return False
        if self._last_heartbeat_forward_at is None:
            return True
        elapsed = workflow.now() - self._last_heartbeat_forward_at
        return elapsed >= BABYSIT_HEARTBEAT_FORWARD_INTERVAL

    async def _forward_heartbeat_to_babysit(self) -> None:
        try:
            await workflow.execute_activity(
                send_heartbeat_to_babysit,
                SendHeartbeatToBabysitInput(task_id=self.context.task_id),
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        except Exception as e:
            workflow.logger.info(
                "send_heartbeat_to_babysit_failed",
                run_id=self.context.run_id,
                error=str(e),
            )
