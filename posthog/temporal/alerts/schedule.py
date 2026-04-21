import datetime as dt

from django.conf import settings

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)

from posthog.temporal.common.schedule import a_create_schedule, a_schedule_exists, a_update_schedule

SCHEDULE_ID = "schedule-due-alert-checks-schedule"
CLEANUP_ALERT_CHECKS_SCHEDULE_ID = "cleanup-alert-checks-schedule"
ALERTS_BACKLOG_SCHEDULE_ID = "report-alerts-backlog-schedule"


async def create_schedule_due_alert_checks_schedule(client: Client) -> None:
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            "schedule-due-alert-checks",
            id=SCHEDULE_ID,
            task_queue=settings.ANALYTICS_PLATFORM_TASK_QUEUE,
            execution_timeout=dt.timedelta(minutes=10),
        ),
        spec=ScheduleSpec(cron_expressions=["*/2 * * * *"]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.ALLOW_ALL),
    )

    if await a_schedule_exists(client, SCHEDULE_ID):
        await a_update_schedule(client, SCHEDULE_ID, schedule)
    else:
        await a_create_schedule(client, SCHEDULE_ID, schedule, trigger_immediately=False)


async def create_cleanup_alert_checks_schedule(client: Client) -> None:
    """Daily at 8:00 UTC — matches the prior Celery crontab."""
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            "cleanup-alert-checks",
            id=CLEANUP_ALERT_CHECKS_SCHEDULE_ID,
            task_queue=settings.ANALYTICS_PLATFORM_TASK_QUEUE,
            execution_timeout=dt.timedelta(minutes=30),
        ),
        spec=ScheduleSpec(cron_expressions=["0 8 * * *"]),
        # SKIP — if yesterday's cleanup is still running, don't start a second one.
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )

    if await a_schedule_exists(client, CLEANUP_ALERT_CHECKS_SCHEDULE_ID):
        await a_update_schedule(client, CLEANUP_ALERT_CHECKS_SCHEDULE_ID, schedule)
    else:
        await a_create_schedule(client, CLEANUP_ALERT_CHECKS_SCHEDULE_ID, schedule, trigger_immediately=False)


async def create_alerts_backlog_schedule(client: Client) -> None:
    """Every 12 minutes — matches the prior Celery crontab."""
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            "report-alerts-backlog",
            id=ALERTS_BACKLOG_SCHEDULE_ID,
            task_queue=settings.ANALYTICS_PLATFORM_TASK_QUEUE,
            execution_timeout=dt.timedelta(minutes=5),
        ),
        spec=ScheduleSpec(cron_expressions=["*/12 * * * *"]),
        # SKIP — if the previous run is still going, don't stack up stale reports.
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )

    if await a_schedule_exists(client, ALERTS_BACKLOG_SCHEDULE_ID):
        await a_update_schedule(client, ALERTS_BACKLOG_SCHEDULE_ID, schedule)
    else:
        await a_create_schedule(client, ALERTS_BACKLOG_SCHEDULE_ID, schedule, trigger_immediately=False)
