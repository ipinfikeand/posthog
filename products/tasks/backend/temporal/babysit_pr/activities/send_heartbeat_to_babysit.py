from dataclasses import dataclass

from temporalio import activity

from posthog.temporal.common.client import async_connect


@dataclass
class SendHeartbeatToBabysitInput:
    task_id: str


@activity.defn
async def send_heartbeat_to_babysit(input: SendHeartbeatToBabysitInput) -> None:
    """Forward an `agent_heartbeat` signal to the babysit workflow for this task.

    Best-effort: if the babysit workflow has already exited (PR merged, max
    repetitions, etc.) the signal target won't exist and we swallow the error.
    """
    client = await async_connect()
    workflow_id = f"pr-babysit-{input.task_id}"
    handle = client.get_workflow_handle(workflow_id)
    try:
        await handle.signal("agent_heartbeat")
    except Exception as e:
        activity.logger.info(
            "send_heartbeat_to_babysit_no_target",
            task_id=input.task_id,
            workflow_id=workflow_id,
            error=str(e),
        )
