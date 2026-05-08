import logging
from dataclasses import dataclass

from temporalio import activity

from posthog.temporal.common.utils import asyncify

from products.tasks.backend.services.sandbox import Sandbox, SandboxBase
from products.tasks.backend.stream.redis_stream import publish_task_run_stream_complete
from products.tasks.backend.temporal.exceptions import SandboxNotFoundError
from products.tasks.backend.temporal.observability import log_activity_execution

logger = logging.getLogger(__name__)
AGENT_SERVER_GRACEFUL_SHUTDOWN_WAIT_SECONDS = 30
AGENT_SERVER_GRACEFUL_SHUTDOWN_COMMAND = (
    "pkill -TERM -f '[a]gent-server' || true; "
    f"for i in $(seq 1 {AGENT_SERVER_GRACEFUL_SHUTDOWN_WAIT_SECONDS}); do "
    "pgrep -f '[a]gent-server' >/dev/null || exit 0; "
    "sleep 1; "
    "done; "
    "pgrep -f '[a]gent-server' >/dev/null && exit 1 || exit 0"
)
AGENT_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = AGENT_SERVER_GRACEFUL_SHUTDOWN_WAIT_SECONDS + 5


@dataclass
class CleanupSandboxInput:
    sandbox_id: str
    run_id: str | None = None
    complete_stream_on_cleanup: bool = False


def _request_agent_server_shutdown(sandbox: SandboxBase) -> bool:
    try:
        result = sandbox.execute(
            AGENT_SERVER_GRACEFUL_SHUTDOWN_COMMAND,
            timeout_seconds=AGENT_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.warning("cleanup_sandbox_agent_server_shutdown_failed", extra={"sandbox_id": sandbox.id}, exc_info=True)
        return False

    if result.exit_code != 0:
        logger.warning(
            "cleanup_sandbox_agent_server_shutdown_nonzero",
            extra={"sandbox_id": sandbox.id, "exit_code": result.exit_code, "stderr": result.stderr},
        )
        return False

    return True


@activity.defn
@asyncify
def cleanup_sandbox(input: CleanupSandboxInput) -> None:
    with log_activity_execution(
        "cleanup_sandbox",
        sandbox_id=input.sandbox_id,
    ):
        stream_completion_safe = False
        try:
            sandbox = Sandbox.get_by_id(input.sandbox_id)
        except SandboxNotFoundError:
            stream_completion_safe = True
            sandbox = None
        except Exception:
            logger.warning("cleanup_sandbox_get_by_id_failed", extra={"sandbox_id": input.sandbox_id}, exc_info=True)
            sandbox = None

        if sandbox is not None:
            agent_server_stopped = False
            if input.complete_stream_on_cleanup:
                agent_server_stopped = _request_agent_server_shutdown(sandbox)
            sandbox_destroyed = False
            try:
                sandbox.destroy()
                sandbox_destroyed = True
            except Exception:
                # The sandbox has a timeout, and it will eventually terminate if we failed to cleanup.
                logger.warning("cleanup_sandbox_destroy_failed", extra={"sandbox_id": input.sandbox_id}, exc_info=True)

            stream_completion_safe = agent_server_stopped or sandbox_destroyed

        if input.complete_stream_on_cleanup and input.run_id and stream_completion_safe:
            publish_task_run_stream_complete(input.run_id)
            logger.info(
                "cleanup_sandbox_stream_completion_published",
                extra={"sandbox_id": input.sandbox_id, "run_id": input.run_id},
            )
        elif input.complete_stream_on_cleanup and input.run_id:
            logger.warning(
                "cleanup_sandbox_stream_completion_skipped",
                extra={"sandbox_id": input.sandbox_id, "run_id": input.run_id},
            )
