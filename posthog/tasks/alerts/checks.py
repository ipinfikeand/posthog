from datetime import UTC, datetime

from django.db.models import Q

import structlog
from celery import shared_task
from dateutil.relativedelta import relativedelta

from posthog.schema import AlertCalculationInterval, AlertState, TrendsQuery

from posthog.models import AlertConfiguration
from posthog.models.alert import AlertCheck
from posthog.ph_client import ph_scoped_capture
from posthog.schema_migrations.upgrade_manager import upgrade_query
from posthog.tasks.alerts.detector import check_trends_alert_with_detector
from posthog.tasks.alerts.trends import check_trends_alert
from posthog.tasks.alerts.utils import WRAPPER_NODE_KINDS, AlertEvaluationResult, next_check_time
from posthog.utils import get_from_dict_or_attr

logger = structlog.get_logger(__name__)


ANIRUDH_DISTINCT_ID = "wcPbDRs08GtNzrNIXfzHvYAkwUaekW7UrAo4y3coznT"


@shared_task(ignore_result=True)
def checks_cleanup_task() -> None:
    AlertCheck.clean_up_old_checks()


@shared_task(
    ignore_result=True,
    expires=60 * 60,
)
def alerts_backlog_task() -> None:
    """
    This runs every 5min to check backlog for alerts
    - hourly alerts - alerts that haven't been checked in the last hour + 5min
    - daily alerts - alerts that haven't been checked in the last hour + 15min
    """
    now = datetime.now(UTC)

    hourly_alerts_breaching_sla = AlertConfiguration.objects.filter(
        Q(
            enabled=True,
            calculation_interval=AlertCalculationInterval.HOURLY,
            last_checked_at__lte=now - relativedelta(hours=1, minutes=5),
        ),
        insight__deleted=False,
    ).count()

    now = datetime.now(UTC)

    daily_alerts_breaching_sla = AlertConfiguration.objects.filter(
        Q(
            enabled=True,
            calculation_interval=AlertCalculationInterval.HOURLY,
            last_checked_at__lte=now - relativedelta(days=1, minutes=15),
        ),
        insight__deleted=False,
    ).count()

    with ph_scoped_capture() as capture_ph_event:
        capture_ph_event(
            distinct_id=ANIRUDH_DISTINCT_ID,
            event="alert check backlog",
            properties={
                "calculation_interval": AlertCalculationInterval.DAILY,
                "backlog": daily_alerts_breaching_sla,
            },
        )

        capture_ph_event(
            distinct_id=ANIRUDH_DISTINCT_ID,
            event="alert check backlog",
            properties={
                "calculation_interval": AlertCalculationInterval.HOURLY,
                "backlog": hourly_alerts_breaching_sla,
            },
        )


def check_alert_for_insight(alert: AlertConfiguration) -> AlertEvaluationResult:
    """
    Matches insight type with alert checking logic.

    If detector_config is set, uses the detector abstraction.
    Otherwise falls back to threshold-based checking.
    """
    insight = alert.insight

    with upgrade_query(insight):
        query = insight.query
        kind = get_from_dict_or_attr(query, "kind")

        if kind in WRAPPER_NODE_KINDS:
            query = get_from_dict_or_attr(query, "source")
            kind = get_from_dict_or_attr(query, "kind")

        match kind:
            case "TrendsQuery":
                query = TrendsQuery.model_validate(query)
                # Use detector-based checking if detector_config is set
                if alert.detector_config:
                    return check_trends_alert_with_detector(alert, insight, query, alert.detector_config)
                return check_trends_alert(alert, insight, query)
            case _:
                raise NotImplementedError(f"AlertCheckError: Alerts for {kind} are not supported yet")


def add_alert_check(
    alert: AlertConfiguration,
    value: float | None,
    breaches: list[str] | None,
    error: dict | None,
    anomaly_scores: list[float | None] | None = None,
    triggered_points: list[int] | None = None,
    triggered_dates: list[str] | None = None,
    interval: str | None = None,
    triggered_metadata: dict | None = None,
) -> tuple[AlertCheck, bool]:
    """Persist an AlertCheck row and return it plus a decision on whether notification is needed.

    `targets_notified` is always created empty; `notify_alert_activity` fills it on
    successful delivery and treats a non-empty value as the idempotency sentinel on retry.
    `last_notified_at` is likewise set by the notify activity on success, not here.
    """
    should_notify = False

    if error:
        alert.state = AlertState.ERRORED
        should_notify = True
    elif breaches:
        alert.state = AlertState.FIRING
        should_notify = True
    else:
        alert.state = AlertState.NOT_FIRING  # Set the Alert to not firing if the threshold is no longer met
        # TODO: Optionally send a resolved notification when alert goes from firing to not_firing?

    alert.last_checked_at = datetime.now(UTC)

    # IMPORTANT: update next_check_at according to interval
    # ensure we don't recheck alert until the next interval is due
    alert.next_check_at = next_check_time(alert)

    alert_check = AlertCheck.objects.create(
        alert_configuration=alert,
        calculated_value=value,
        condition=alert.condition,
        targets_notified={},
        state=alert.state,
        triggered_metadata=triggered_metadata,
        error=error,
        anomaly_scores=anomaly_scores,
        triggered_points=triggered_points,
        triggered_dates=triggered_dates,
        interval=interval,
    )

    alert.save(update_fields=["state", "last_checked_at", "next_check_at"])

    return alert_check, should_notify
