"""Feature schema + parity validation for the session interestingness model.

Why this exists: the XGBoost model was trained against a frozen set of feature
columns, in a specific order, with specific dtypes and value ranges. The CH
SELECT in `sql.FEATURE_SELECT_FRAGMENT` projects those features at serve time.
If the two drift — a feature is renamed, a column is added in one place but
not the other, a dtype changes, a value falls outside the trained domain —
predictions silently degrade with no error.

`validate_features(df)` is the cheap runtime guard that fails loud rather
than silently scoring on garbage. It runs once per chunk just before predict
and is intended to be a hard gate: any mismatch raises and the activity
fails (Temporal will retry; if the schema genuinely changed, all retries
fail, which is the right escalation signal).

When updating the model:
    1. Train the new model with a frozen `FEATURE_NAMES` order.
    2. Update `FEATURE_NAMES` and `FEATURE_RANGES` here in lockstep with
       `sql.FEATURE_SELECT_FRAGMENT`.
    3. Bump `MODEL_FEATURE_SCHEMA_VERSION`.
    4. Re-deploy. The schema version is logged on every chunk; mismatches
       between worker pods running different versions are visible in logs.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# Bump on every breaking feature-set change. Logged per chunk so downstream
# observers can correlate score-distribution shifts with deploys.
MODEL_FEATURE_SCHEMA_VERSION = 1

# Order MUST match `sql.FEATURE_SELECT_FRAGMENT`'s column order. XGBoost's
# DMatrix is positional unless feature_names is passed; we pass feature_names
# defensively to make name mismatches a hard fail rather than silent reordering.
FEATURE_NAMES: tuple[str, ...] = (
    "pageview_count",
    "autocapture_count",
    "screen_count",
    "duration_ms",
    "has_replay_events",
    "has_autocapture",
    "unique_url_count",
    "unique_event_count",
    "unique_host_count",
)

# Columns that identify the row but are NOT features for the model.
# Stripped before predict; re-attached for the INSERT.
ID_COLUMNS: tuple[str, ...] = ("team_id", "session_id_v7", "session_timestamp")


@dataclass(frozen=True)
class FeatureSpec:
    """Allowed dtype family + value range for a single feature.

    `dtype_kind` follows numpy's dtype.kind taxonomy:
        'i' int, 'u' unsigned int, 'f' float, 'b' bool.
    A range of `None` on either end disables that side of the bounds check.
    """

    dtype_kind: str
    min_value: float | None = None
    max_value: float | None = None


# Per-feature contracts. min/max here aren't statistical bounds — they're the
# universe of values the model has ever been trained against. Anything outside
# is a wiring bug (negative count, infinity from a bad division), not just a
# distribution shift.
FEATURE_RANGES: dict[str, FeatureSpec] = {
    "pageview_count": FeatureSpec("iu", 0, None),
    "autocapture_count": FeatureSpec("iu", 0, None),
    "screen_count": FeatureSpec("iu", 0, None),
    # Up to ~30 days in ms — anything bigger is a clock skew bug, not a real session.
    "duration_ms": FeatureSpec("iu", 0, 30 * 24 * 60 * 60 * 1000),
    "has_replay_events": FeatureSpec("iub", 0, 1),
    "has_autocapture": FeatureSpec("iub", 0, 1),
    "unique_url_count": FeatureSpec("iu", 0, None),
    "unique_event_count": FeatureSpec("iu", 0, None),
    "unique_host_count": FeatureSpec("iu", 0, None),
}


class FeatureValidationError(Exception):
    """Raised when a chunk's feature DataFrame doesn't match the trained schema.

    Carries enough detail for Temporal's failure surface to be debuggable
    without re-running. Always include the offending column and the failing
    rows / values.
    """


def _check_columns(df: pd.DataFrame) -> None:
    """Hard check on column set + order. Order matters because we rely on it
    in DMatrix construction; reordering features silently changes predictions.
    """
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
    """XGBoost handles NaN, but +/-inf is not a value the model has ever seen
    and almost always indicates a feature-engineering bug (division by zero
    in a derived feature, etc.). Fail loud.
    """
    if series.dtype.kind != "f":
        return
    if not np.isfinite(series.replace({np.nan: 0}).to_numpy()).all():
        bad = series[~np.isfinite(series.fillna(0))]
        raise FeatureValidationError(f"Feature {name!r} contains non-finite values: first 5 = {bad.head(5).tolist()}.")


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

    Runs in O(rows × features). Keep `df` to a single chunk (~10k rows) and
    this stays at single-digit ms.

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

    Preserves the row order of `df` so callers can re-attach scores positionally.
    """
    return df.loc[:, list(FEATURE_NAMES)]
