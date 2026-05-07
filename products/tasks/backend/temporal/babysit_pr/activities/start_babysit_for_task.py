from dataclasses import dataclass
from datetime import timedelta

from django.conf import settings

from temporalio import activity
from temporalio.common import WorkflowIDReusePolicy

from posthog.temporal.common.client import async_connect

from products.tasks.backend.temporal.process_task.activities.get_task_processing_context import TaskProcessingContext

# Hard ceiling on how long a babysit instance can poll a PR. Most PRs merge
# within hours; abandoned PRs that never merge would otherwise leak workflow
# history forever. Easily extended by re-signalling: a future run signals the
# same workflow ID and resets the timer.
BABYSIT_RUN_TIMEOUT = timedelta(days=7)


@dataclass
class StartBabysitForTaskInput:
    context: TaskProcessingContext


@activity.defn
async def start_babysit_for_task(input: StartBabysitForTaskInput) -> None:
    """Signal-with-start the BabysitPullRequestWorkflow for this task.

    The babysit workflow is per-Task (workflow ID `pr-babysit-{task_id}`), so
    repeated calls during CI-fix re-runs land as `track_run` signals on the
    same instance. Skips entirely when no PR is going to exist (e.g. create_pr
    is off, the feature flag is disabled, or there are no GitHub credentials).
    """
    ctx = input.context

    if not ctx.create_pr:
        activity.logger.info("babysit_skipped_no_pr", task_id=ctx.task_id, run_id=ctx.run_id)
        return
    if not ctx.pr_loop_enabled:
        activity.logger.info("babysit_skipped_pr_loop_disabled", task_id=ctx.task_id, run_id=ctx.run_id)
        return
    if not ctx.has_github_credentials:
        activity.logger.info("babysit_skipped_no_github_credentials", task_id=ctx.task_id, run_id=ctx.run_id)
        return

    client = await async_connect()
    workflow_id = f"pr-babysit-{ctx.task_id}"
    babysit_input = {"context": ctx}
    await client.start_workflow(
        "babysit-pull-request",
        babysit_input,
        id=workflow_id,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
        task_queue=settings.TASKS_TASK_QUEUE,
        run_timeout=BABYSIT_RUN_TIMEOUT,
        start_signal="track_run",
        start_signal_args=[ctx],
    )
    activity.logger.info(
        "babysit_started_or_signaled",
        task_id=ctx.task_id,
        run_id=ctx.run_id,
        workflow_id=workflow_id,
    )
