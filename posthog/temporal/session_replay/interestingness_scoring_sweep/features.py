"""Feature schema + parity validation for the session interestingness model.

The model is trained on the CTE-derived `replay_features` query in `sql.py`.
This module pins:

    * `FEATURE_NAMES`: the exact column order the booster was trained against.
    * `FEATURE_RANGES`: per-feature dtype kind + value-range bounds.

`validate_features(df)` runs once per chunk just before predict and is a
hard gate: any column-set / order / dtype / range / non-finite mismatch
raises and the chunk activity fails (marked non_retryable in the workflow,
so a schema bug fails fast).

When the model is retrained:
    1. Re-run training; freeze the new `FEATURE_NAMES` order.
    2. Update `FEATURE_NAMES` and `FEATURE_RANGES` here in lockstep with
       `sql.py`'s SELECT column order.
    3. Bump `MODEL_FEATURE_SCHEMA_VERSION` so deploys with mismatched workers
       are visible in logs.

Notes on dtypes:

    * Rates / ratios / stats are CH `Float64` divided by counts, returned
      as Python `float`. ClickHouse's `nullIf` produces NULL on zero
      denominators; pandas surfaces this as `NaN`, which XGBoost handles
      natively. Validation accepts NaN (but not +/-inf — that's a feature
      engineering bug).
    * Pass-through counts (`*_path_visit_count`, `unique_urls`,
      `viewport_resize_count`, `selection_copy_count`, `long_idle_gap_count`,
      `page_revisit_count`) come back as Python `int`. Pandas will infer
      either int64 or float64 depending on whether the chunk has any NULLs.
      Validation accepts both kinds for these columns.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# Bump on every breaking feature-set change. Logged per chunk so distribution
# shifts can be correlated with deploys.
MODEL_FEATURE_SCHEMA_VERSION = 1

# Order MUST match the SELECT in `sql.fetch_features_sql`. XGBoost predict is
# positional unless feature_names is passed; we pass feature_names defensively
# to make name mismatches a hard fail rather than silent reordering.
FEATURE_NAMES: tuple[str, ...] = (
    "event_rate",
    "click_rate",
    "keypress_rate",
    "mouse_activity_rate",
    "rage_click_rate",
    "dead_click_rate",
    "quick_back_rate",
    "page_visit_rate",
    "text_selection_rate",
    "scroll_event_rate",
    "console_error_rate",
    "console_error_after_click_rate",
    "network_request_rate",
    "network_failed_request_rate",
    "mouse_mean_x",
    "mouse_mean_y",
    "mouse_stddev_x",
    "mouse_stddev_y",
    "mouse_distance_per_s",
    "mouse_direction_change_rate",
    "mouse_velocity_mean",
    "mouse_velocity_stddev",
    "scroll_magnitude_per_s",
    "scroll_magnitude_per_event",
    "scroll_direction_reversal_rate",
    "rapid_scroll_reversal_rate",
    "max_scroll_y",
    "inter_action_gap_mean_ms",
    "inter_action_gap_stddev_ms",
    "max_idle_gap_ms",
    "network_request_duration_mean_ms",
    "network_request_duration_stddev_ms",
    "network_failure_ratio",
    "network_4xx_ratio",
    "network_5xx_ratio",
    "scroll_to_top_rate",
    "backspace_ratio",
    "long_idle_gap_count",
    "console_warn_rate",
    "mutation_rate",
    "viewport_resize_count",
    "touch_event_rate",
    "selection_copy_count",
    "login_path_visit_count",
    "signup_path_visit_count",
    "checkout_path_visit_count",
    "cart_path_visit_count",
    "billing_path_visit_count",
    "settings_path_visit_count",
    "account_path_visit_count",
    "error_path_visit_count",
    "not_found_path_visit_count",
    "admin_path_visit_count",
    "dashboard_path_visit_count",
    "onboarding_path_visit_count",
    "cancel_path_visit_count",
    "refund_path_visit_count",
    "unique_urls",
    "unique_click_targets",
    "unique_form_fields",
    "page_revisit_count",
)

# Columns that identify the row but are NOT model features. Stripped before
# predict; re-attached for the INSERT.
ID_COLUMNS: tuple[str, ...] = ("team_id", "session_id_v7", "session_timestamp")

# Per-dtype-kind groupings used in `FEATURE_RANGES` below.
# `'iuf'` accepts int / unsigned int / float — useful for pass-through counts
# whose pandas dtype depends on whether NULLs are present in a given chunk.
_NUMERIC = "iuf"
_FLOAT = "f"


@dataclass(frozen=True)
class FeatureSpec:
    """Allowed dtype family + value range for a single feature.

    `dtype_kind` is one or more of numpy's dtype.kind tags concatenated
    ('i' int, 'u' unsigned int, 'f' float, 'b' bool). A range of `None` on
    either end disables that side of the bounds check.
    """

    dtype_kind: str
    min_value: float | None = None
    max_value: float | None = None


# Per-feature contracts. Bounds aren't statistical — they're the universe
# of values the model has ever been trained on. Anything outside is a wiring
# bug (negative count, infinity from a bad division), not just a distribution
# shift. Generous upper bounds are deliberate: the goal is to catch wiring
# bugs without flagging legitimate outliers.
#
# Rates: 0+, no upper bound (1k events/sec is unusual but possible).
# Ratios: 0..1 with a tiny epsilon for FP error.
# Mouse mean coords: any float (mouse can be off-screen).
# Counts: non-negative.
_RATE = FeatureSpec(_FLOAT, 0.0, None)
_RATIO = FeatureSpec(_FLOAT, 0.0, 1.0 + 1e-6)
_NONNEG_FLOAT = FeatureSpec(_FLOAT, 0.0, None)
_ANY_FLOAT = FeatureSpec(_FLOAT, None, None)
_NONNEG_COUNT = FeatureSpec(_NUMERIC, 0, None)


FEATURE_RANGES: dict[str, FeatureSpec] = {
    "event_rate": _RATE,
    "click_rate": _RATE,
    "keypress_rate": _RATE,
    "mouse_activity_rate": _RATE,
    "rage_click_rate": _RATE,
    "dead_click_rate": _RATE,
    "quick_back_rate": _RATE,
    "page_visit_rate": _RATE,
    "text_selection_rate": _RATE,
    "scroll_event_rate": _RATE,
    "console_error_rate": _RATE,
    "console_error_after_click_rate": _RATE,
    "network_request_rate": _RATE,
    "network_failed_request_rate": _RATE,
    "mouse_mean_x": _ANY_FLOAT,
    "mouse_mean_y": _ANY_FLOAT,
    "mouse_stddev_x": _NONNEG_FLOAT,
    "mouse_stddev_y": _NONNEG_FLOAT,
    "mouse_distance_per_s": _NONNEG_FLOAT,
    "mouse_direction_change_rate": _NONNEG_FLOAT,
    "mouse_velocity_mean": _NONNEG_FLOAT,
    "mouse_velocity_stddev": _NONNEG_FLOAT,
    "scroll_magnitude_per_s": _NONNEG_FLOAT,
    "scroll_magnitude_per_event": _NONNEG_FLOAT,
    "scroll_direction_reversal_rate": _RATE,
    "rapid_scroll_reversal_rate": _RATE,
    "max_scroll_y": _NONNEG_FLOAT,
    "inter_action_gap_mean_ms": _NONNEG_FLOAT,
    "inter_action_gap_stddev_ms": _NONNEG_FLOAT,
    "max_idle_gap_ms": _NONNEG_FLOAT,
    "network_request_duration_mean_ms": _NONNEG_FLOAT,
    "network_request_duration_stddev_ms": _NONNEG_FLOAT,
    "network_failure_ratio": _RATIO,
    "network_4xx_ratio": _RATIO,
    "network_5xx_ratio": _RATIO,
    "scroll_to_top_rate": _RATE,
    "backspace_ratio": _RATIO,
    "long_idle_gap_count": _NONNEG_COUNT,
    "console_warn_rate": _RATE,
    "mutation_rate": _RATE,
    "viewport_resize_count": _NONNEG_COUNT,
    "touch_event_rate": _RATE,
    "selection_copy_count": _NONNEG_COUNT,
    "login_path_visit_count": _NONNEG_COUNT,
    "signup_path_visit_count": _NONNEG_COUNT,
    "checkout_path_visit_count": _NONNEG_COUNT,
    "cart_path_visit_count": _NONNEG_COUNT,
    "billing_path_visit_count": _NONNEG_COUNT,
    "settings_path_visit_count": _NONNEG_COUNT,
    "account_path_visit_count": _NONNEG_COUNT,
    "error_path_visit_count": _NONNEG_COUNT,
    "not_found_path_visit_count": _NONNEG_COUNT,
    "admin_path_visit_count": _NONNEG_COUNT,
    "dashboard_path_visit_count": _NONNEG_COUNT,
    "onboarding_path_visit_count": _NONNEG_COUNT,
    "cancel_path_visit_count": _NONNEG_COUNT,
    "refund_path_visit_count": _NONNEG_COUNT,
    "unique_urls": _NONNEG_COUNT,
    "unique_click_targets": _NONNEG_COUNT,
    "unique_form_fields": _NONNEG_COUNT,
    "page_revisit_count": _NONNEG_COUNT,
}


# Cheap consistency check at import time — guarantees the two tables stay in
# lockstep without a separate test.
assert set(FEATURE_RANGES.keys()) == set(FEATURE_NAMES), (
    "FEATURE_RANGES and FEATURE_NAMES are out of sync; "
    f"missing={set(FEATURE_NAMES) - set(FEATURE_RANGES.keys())}, "
    f"extra={set(FEATURE_RANGES.keys()) - set(FEATURE_NAMES)}"
)


class FeatureValidationError(Exception):
    """Raised when a chunk's feature DataFrame doesn't match the trained schema."""


def _check_columns(df: pd.DataFrame) -> None:
    """Hard check on column set + order. Order matters for DMatrix construction."""
    expected = list(FEATURE_NAMES)
    actual_features = [c for c in df.columns if c not in ID_COLUMNS]

    missing = set(expected) - set(actual_features)
    extra = set(actual_features) - set(expected)
    if missing or extra:
        raise FeatureValidationError(
            f"Feature column set mismatch: missing={sorted(missing)}, extra={sorted(extra)}. "
            f"Expected (in order): {expected}. Actual (in order): {actual_features}."
        )

    if actual_features != expected:
        raise FeatureValidationError(f"Feature column order mismatch. Expected: {expected}. Actual: {actual_features}.")


def _check_dtype(name: str, series: pd.Series, allowed_kinds: str) -> None:
    """Validate a column's dtype.kind matches one of `allowed_kinds`."""
    kind = series.dtype.kind
    if kind not in allowed_kinds:
        raise FeatureValidationError(
            f"Feature {name!r} has dtype {series.dtype} (kind={kind!r}); expected one of kinds {list(allowed_kinds)!r}."
        )


def _check_finite(name: str, series: pd.Series) -> None:
    """+/-inf is never a value the model has seen and almost always means a
    feature-engineering bug (division returning inf, etc.). Fail loud.

    NaN is fine — XGBoost handles it natively, and our SQL deliberately
    produces NULL (→ NaN) when denominators are zero.
    """
    if series.dtype.kind != "f":
        return
    arr = series.to_numpy()
    if not np.all(np.isfinite(arr) | np.isnan(arr)):
        bad = series[~(np.isfinite(arr) | np.isnan(arr))]
        raise FeatureValidationError(
            f"Feature {name!r} contains non-finite values (excluding NaN): first 5 = {bad.head(5).tolist()}."
        )


def _check_range(name: str, series: pd.Series, spec: FeatureSpec) -> None:
    """Reject any value outside the trained range. NaN passes (XGBoost handles it)."""
    finite = series.dropna()
    if finite.empty:
        return
    if spec.min_value is not None:
        below = finite[finite < spec.min_value]
        if not below.empty:
            raise FeatureValidationError(
                f"Feature {name!r} has {len(below)} value(s) below min={spec.min_value}: e.g. {below.head(5).tolist()}."
            )
    if spec.max_value is not None:
        above = finite[finite > spec.max_value]
        if not above.empty:
            raise FeatureValidationError(
                f"Feature {name!r} has {len(above)} value(s) above max={spec.max_value}: e.g. {above.head(5).tolist()}."
            )


def validate_features(df: pd.DataFrame) -> None:
    """Hard-fail if `df` doesn't match the trained model's expected schema.

    O(rows × features). On a 10k-row chunk × 61 features this is single-digit ms.

    Raises:
        FeatureValidationError: any column / dtype / range / finiteness mismatch.
    """
    if df.empty:
        return

    _check_columns(df)
    for name in FEATURE_NAMES:
        series = df[name]
        spec = FEATURE_RANGES[name]
        _check_dtype(name, series, spec.dtype_kind)
        _check_finite(name, series)
        _check_range(name, series, spec)


def feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Strip ID columns; return a DataFrame with feature columns in trained order.

    Preserves row order so callers can re-attach scores positionally.
    """
    return df.loc[:, list(FEATURE_NAMES)]
