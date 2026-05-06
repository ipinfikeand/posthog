"""
Shared utilities for proxying flag evaluation requests to the Rust flags service.

All flag evaluation (decide, toolbar, local eval API, etc.) now goes through the Rust
flags service. This module provides a shared HTTP client and proxy function.
"""

from typing import Any

from django.conf import settings

from posthog.security.outbound_proxy import internal_requests_session

# Reusable session for proxying to the flags service with connection pooling
_FLAGS_SERVICE_SESSION = internal_requests_session()

# Header used by `/internal/flags` to authenticate calls from other PostHog services.
# Must match `INTERNAL_SECRET_HEADER` in `rust/feature-flags/src/api/endpoint.rs`.
_INTERNAL_SECRET_HEADER = "X-PostHog-Internal-Secret"


def get_flags_from_service(
    token: str,
    distinct_id: str,
    groups: dict[str, Any] | None = None,
    *,
    internal: bool = False,
) -> dict[str, Any]:
    """
    Proxy a request to the Rust feature flags service.

    Args:
        token: The project API token (the public token) for the team
        distinct_id: The distinct ID for the user
        groups: Optional groups for group-based flags (default: None)
        internal: When True, route via `/internal/flags` instead of `/flags`. The
            internal route is only reachable from inside the cluster; it bypasses
            the per-team billing limiter and does not increment the team's
            billable flag-request counter. Use this only for service-to-service
            calls that should not bill the customer (e.g. cohort evaluation,
            internal UI handlers). Default: False.

    Returns:
        The full response from the flags service as a dict, typically containing:
        - "flags": dict of flag key -> value/boolean
        - "featureFlagPayloads": dict of flag key -> payload (if requested)
        - Other metadata depending on API version

    Raises:
        requests.RequestException: If the HTTP request fails (timeout, connection error, etc.)
        requests.HTTPError: If the service returns a non-2xx status code

    Example:
        >>> response = get_flags_from_service(
        ...     token="phc_abc123",
        ...     distinct_id="user_123",
        ...     groups={"company": "acme"}
        ... )
        >>> flags_data = response.get("flags", {})
        >>> if flags_data.get("new-feature", {}).get("enabled"):
        ...     # Feature is enabled
    """
    if internal:
        base_url = getattr(settings, "INTERNAL_FLAGS_SERVICE_URL", None) or getattr(
            settings, "FEATURE_FLAGS_SERVICE_URL", "http://localhost:3001"
        )
        path = "/internal/flags"
    else:
        base_url = getattr(settings, "FEATURE_FLAGS_SERVICE_URL", "http://localhost:3001")
        path = "/flags"

    proxy_timeout = getattr(settings, "FEATURE_FLAGS_SERVICE_PROXY_TIMEOUT", 3)

    payload: dict[str, Any] = {
        "token": token,
        "distinct_id": distinct_id,
    }

    if groups:
        payload["groups"] = groups

    params: dict[str, str] = {"v": "2"}

    headers: dict[str, str] = {}
    if internal:
        secret = getattr(settings, "INTERNAL_FLAGS_SHARED_SECRET", "")
        if secret:
            headers[_INTERNAL_SECRET_HEADER] = secret

    response = _FLAGS_SERVICE_SESSION.post(
        f"{base_url}{path}",
        params=params,
        json=payload,
        headers=headers or None,
        timeout=proxy_timeout,
    )
    response.raise_for_status()
    return response.json()
