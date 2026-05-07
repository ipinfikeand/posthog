import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest

from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from products.tasks.backend.temporal.babysit_pr.activities.dispatch_ci_fix import DispatchCiFixInput
from products.tasks.backend.temporal.babysit_pr.workflow import (
    CI_FOLLOW_UP_DELAY,
    DEFAULT_CI_MESSAGE,
    MAX_CI_REPETITIONS,
    BabysitPullRequestInput,
    BabysitPullRequestWorkflow,
)
from products.tasks.backend.temporal.process_task.activities.get_pr_context import GetPrContextOutput
from products.tasks.backend.temporal.process_task.activities.get_task_processing_context import TaskProcessingContext

_pr_context_overrides: dict = {}
_dispatch_calls: list[DispatchCiFixInput] = []


def _build_context(*, ci_prompt: str | None = None) -> TaskProcessingContext:
    return TaskProcessingContext(
        task_id="task-1",
        run_id="run-1",
        team_id=1,
        team_uuid=str(uuid.uuid4()),
        organization_id=str(uuid.uuid4()),
        github_integration_id=1,
        repository="org/repo",
        distinct_id="user-1",
        create_pr=True,
        pr_loop_enabled=True,
        ci_prompt=ci_prompt,
    )


@activity.defn(name="get_pr_context")
def _mock_get_pr_context(_input) -> GetPrContextOutput | None:
    behavior = _pr_context_overrides.get("behavior", "changing")
    _pr_context_overrides["_call_count"] = _pr_context_overrides.get("_call_count", 0) + 1
    if behavior == "missing":
        return None
    if behavior == "merged":
        return GetPrContextOutput(
            pr_url="https://github.com/org/repo/pull/1",
            pr_state="closed",
            fingerprint="merged-fp",
            is_merged=True,
        )
    if behavior == "closed_unmerged":
        return GetPrContextOutput(
            pr_url="https://github.com/org/repo/pull/1",
            pr_state="closed",
            fingerprint="closed-fp",
            is_merged=False,
        )
    if behavior == "unchanged":
        return GetPrContextOutput(
            pr_url="https://github.com/org/repo/pull/1",
            pr_state="open",
            fingerprint="stable-fp",
            is_merged=False,
        )
    if behavior == "sequence":
        sequence: list[str] = _pr_context_overrides["sequence"]
        idx = min(_pr_context_overrides["_call_count"] - 1, len(sequence) - 1)
        return GetPrContextOutput(
            pr_url="https://github.com/org/repo/pull/1",
            pr_state="open",
            fingerprint=sequence[idx],
            is_merged=False,
        )
    return GetPrContextOutput(
        pr_url="https://github.com/org/repo/pull/1",
        pr_state="open",
        fingerprint=f"fp-{_pr_context_overrides['_call_count']}",
        is_merged=False,
    )


@activity.defn(name="dispatch_ci_fix")
def _mock_dispatch_ci_fix(input: DispatchCiFixInput) -> None:
    _dispatch_calls.append(input)


def _make_worker(env, task_queue: str) -> Worker:
    return Worker(
        env.client,
        task_queue=task_queue,
        workflows=[BabysitPullRequestWorkflow],
        activities=[
            _mock_get_pr_context,
            _mock_dispatch_ci_fix,
        ],
        workflow_runner=UnsandboxedWorkflowRunner(),
        activity_executor=ThreadPoolExecutor(max_workers=5),
    )


pytestmark = [pytest.mark.asyncio, pytest.mark.django_db]


class TestBabysitPullRequestWorkflow:
    @pytest.fixture(autouse=True)
    def _reset_state(self):
        _pr_context_overrides.clear()
        _dispatch_calls.clear()
        yield
        _pr_context_overrides.clear()
        _dispatch_calls.clear()

    @pytest.mark.timeout(60)
    async def test_exits_when_no_pr_after_first_check(self):
        _pr_context_overrides["behavior"] = "missing"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                result = await env.client.execute_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=_build_context()),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=2),
                )

        assert result.terminal_reason == "no_pr"
        assert result.ci_repetitions == 0
        assert _dispatch_calls == []
        assert _pr_context_overrides.get("_call_count") == 1

    @pytest.mark.timeout(60)
    async def test_exits_cleanly_when_pr_merged(self):
        _pr_context_overrides["behavior"] = "merged"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                result = await env.client.execute_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=_build_context()),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=2),
                )

        assert result.terminal_reason == "merged"
        assert _dispatch_calls == []

    @pytest.mark.timeout(60)
    async def test_exits_cleanly_when_pr_closed_unmerged(self):
        _pr_context_overrides["behavior"] = "closed_unmerged"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                result = await env.client.execute_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=_build_context()),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=2),
                )

        assert result.terminal_reason == "closed_unmerged"
        assert _dispatch_calls == []

    @pytest.mark.timeout(60)
    async def test_dispatches_ci_fix_when_pr_changed(self):
        _pr_context_overrides["behavior"] = "changing"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                result = await env.client.execute_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=_build_context()),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=4),
                )

        assert result.ci_repetitions == MAX_CI_REPETITIONS
        assert len(_dispatch_calls) == MAX_CI_REPETITIONS
        for call in _dispatch_calls:
            assert call.task_id == "task-1"
            assert call.run_id == "run-1"
            assert call.ci_message == DEFAULT_CI_MESSAGE
            assert call.create_pr is True

    @pytest.mark.timeout(60)
    async def test_uses_ci_prompt_override_when_set(self):
        custom_prompt = "Custom CI prompt for the babysit dispatch path."
        _pr_context_overrides["behavior"] = "changing"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                await env.client.execute_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=_build_context(ci_prompt=custom_prompt)),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=4),
                )

        assert _dispatch_calls
        assert all(call.ci_message == custom_prompt for call in _dispatch_calls)

    @pytest.mark.timeout(90)
    async def test_skips_dispatch_when_fingerprint_unchanged(self):
        # The first poll persists the fingerprint and FIREs once. Subsequent
        # polls observe the same fingerprint and SKIP — guarded by the
        # _last_active_time bump in _handle_terminal_or_skip so we don't
        # tight-loop the GitHub API.
        _pr_context_overrides["behavior"] = "unchanged"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                handle = await env.client.start_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=_build_context()),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=4),
                )
                # Skip past three CI polling cycles and assert only one fire.
                await env.sleep(CI_FOLLOW_UP_DELAY.total_seconds() * 3 + 60)
                # Simulate a PR merge to terminate the workflow before time-skipping forever.
                _pr_context_overrides["behavior"] = "merged"
                result = await handle.result()

        assert len(_dispatch_calls) == 1, (
            f"only the first changed-fingerprint poll should fire; got {len(_dispatch_calls)} dispatches"
        )
        assert result.terminal_reason == "merged"

    @pytest.mark.timeout(90)
    async def test_heartbeat_signal_resets_idle_timer(self):
        # Send a heartbeat near the CI deadline; the workflow should sleep
        # ANOTHER full CI_FOLLOW_UP_DELAY before polling.
        _pr_context_overrides["behavior"] = "changing"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                handle = await env.client.start_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=_build_context()),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=4),
                )

                # Just before the deadline, send a heartbeat to push the timer.
                await env.sleep(CI_FOLLOW_UP_DELAY.total_seconds() - 30)
                await handle.signal(BabysitPullRequestWorkflow.agent_heartbeat)
                # Cross the original deadline; no dispatch should have happened.
                await env.sleep(60)
                pre_dispatch_count = len(_dispatch_calls)

                # Cleanly end via a merge result.
                _pr_context_overrides["behavior"] = "merged"
                # Advance enough for the next idle period to elapse.
                await env.sleep(CI_FOLLOW_UP_DELAY.total_seconds() + 60)
                await handle.result()

        assert pre_dispatch_count == 0, "heartbeat should have prevented the original deadline from firing"

    @pytest.mark.timeout(60)
    async def test_track_run_signal_updates_active_run(self):
        # The track_run signal updates the workflow's active_context. The
        # subsequent dispatch should target the new run_id.
        _pr_context_overrides["behavior"] = "changing"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                handle = await env.client.start_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=_build_context()),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=4),
                )

                new_context = _build_context()
                new_context.run_id = "run-2"
                await handle.signal(BabysitPullRequestWorkflow.track_run, new_context)

                # The track_run signal resets the idle timer; advance past it
                # so a poll fires with the new context.
                await env.sleep(CI_FOLLOW_UP_DELAY.total_seconds() + 60)
                _pr_context_overrides["behavior"] = "merged"
                await handle.result()

        assert _dispatch_calls, "expected at least one dispatch using the updated run_id"
        assert _dispatch_calls[0].run_id == "run-2"

    @pytest.mark.timeout(60)
    async def test_signal_with_start_starts_workflow_when_not_running(self):
        # Validates the pattern start_babysit_for_task uses: passing the
        # context as the start_signal_args so a fresh workflow gets the run
        # via track_run before its run() body proceeds.
        _pr_context_overrides["behavior"] = "merged"

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-{uuid.uuid4()}"
            async with _make_worker(env, task_queue):
                ctx = _build_context()
                handle = await env.client.start_workflow(
                    BabysitPullRequestWorkflow.run,
                    BabysitPullRequestInput(context=ctx),
                    id=f"test-{uuid.uuid4()}",
                    task_queue=task_queue,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    execution_timeout=timedelta(hours=2),
                    start_signal="track_run",
                    start_signal_args=[ctx],
                )
                result = await handle.result()

        assert result.terminal_reason == "merged"
