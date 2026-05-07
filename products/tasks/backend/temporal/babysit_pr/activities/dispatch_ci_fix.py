from dataclasses import dataclass
from datetime import timedelta

from django.conf import settings

from temporalio import activity
from temporalio.common import RetryPolicy, WorkflowIDReusePolicy

from posthog.temporal.common.client import async_connect
from posthog.temporal.oauth import PosthogMcpScopes

from products.tasks.backend.models import TaskRun


@dataclass
class DispatchCiFixInput:
    task_id: str
    run_id: str
    ci_message: str
    create_pr: bool
    posthog_mcp_scopes: PosthogMcpScopes = "read_only"


@activity.defn
async def dispatch_ci_fix(input: DispatchCiFixInput) -> None:
    """Signal-with-start the ProcessTaskWorkflow for the latest run, queueing a CI fix follow-up.

    If the workflow is already running for the given run_id, the
    `send_followup_message` signal is delivered; otherwise a fresh
    ProcessTaskWorkflow is started which re-provisions the sandbox and processes
    the queued follow-up. Uses the same JSON-shaped input as ProcessTaskInput so
    we can avoid a circular import on `ProcessTaskInput` itself.
    """
    client = await async_connect()
    workflow_id = TaskRun.get_workflow_id(input.task_id, input.run_id)
    process_task_input = {
        "run_id": input.run_id,
        "create_pr": input.create_pr,
        "posthog_mcp_scopes": input.posthog_mcp_scopes,
    }
    await client.start_workflow(
        "process-task",
        process_task_input,
        id=workflow_id,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
        task_queue=settings.TASKS_TASK_QUEUE,
        retry_policy=RetryPolicy(maximum_attempts=3),
        execution_timeout=timedelta(hours=4),
        start_signal="send_followup_message",
        start_signal_args=[input.ci_message, []],
    )
    activity.logger.info(
        "babysit_dispatch_ci_fix",
        task_id=input.task_id,
        run_id=input.run_id,
        workflow_id=workflow_id,
    )
