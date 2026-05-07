"""XGBoost booster lifecycle for the session interestingness scorer.

The booster is loaded **once per worker process** and held as a module-level
singleton. XGBoost predict releases the GIL and is parallelized internally
by libomp; we want libomp to use the worker pod's full CPU budget on a
single chunk at a time, not split it across many concurrent activities (see
`README.md` for the OMP_NUM_THREADS guidance).

Loading from disk is paid on first use; pin the model file in the worker
container image so the load is local + fast.

xgboost is lazy-imported on first use so workers that don't pull this task
queue (most of them) don't pay the import cost or require xgboost to be
installed at all.
"""

from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING, Any

import numpy as np
import structlog

from posthog.temporal.session_scoring.features import FEATURE_NAMES, feature_matrix

if TYPE_CHECKING:
    import pandas as pd

logger = structlog.get_logger(__name__)


# Path on disk where the trained booster is mounted/baked. Override with
# `SESSION_INTERESTINGNESS_MODEL_PATH` in an environment-specific settings file
# or container spec — keeps this module portable across local / staging / prod.
_MODEL_PATH_ENV_VAR = "SESSION_INTERESTINGNESS_MODEL_PATH"
_DEFAULT_MODEL_PATH = "/models/session_interestingness/model.ubj"

# Held as Any (not `xgb.Booster | None`) so workers without xgboost installed
# can still import the module cleanly.
_BOOSTER: Any = None
_BOOSTER_LOCK = threading.Lock()


def _model_path() -> str:
    return os.environ.get(_MODEL_PATH_ENV_VAR, _DEFAULT_MODEL_PATH)


def _load_booster() -> Any:
    """Lazy load + cache the booster; thread-safe under high `max_concurrent_activities`.

    Held under a lock only on first load. Subsequent calls hit the fast path
    (one global-not-None check), so the lock isn't on the per-predict path.
    """
    global _BOOSTER
    if _BOOSTER is not None:
        return _BOOSTER

    with _BOOSTER_LOCK:
        if _BOOSTER is not None:
            return _BOOSTER

        import xgboost as xgb  # noqa: PLC0415  (intentional: lazy import, see module docstring)

        path = _model_path()
        booster = xgb.Booster()
        booster.load_model(path)
        logger.info("session_scoring.model_loaded", path=path, num_features=booster.num_features())
        _BOOSTER = booster
        return _BOOSTER


def warmup() -> None:
    """Eagerly load the booster on worker startup.

    Call from the worker bootstrap so the first activity doesn't pay the
    load cost (typically tens of ms but spikes badly if the model file is
    on a slow mount).
    """
    _load_booster()


class FeatureCountMismatchError(Exception):
    """Booster's expected feature count != FEATURE_NAMES."""


class ScoreRangeError(Exception):
    """Booster returned scores outside [0, 1] — model is likely misconfigured."""


def predict(df: pd.DataFrame) -> np.ndarray:
    """Score a chunk's feature DataFrame and return a 1-D float32 array in [0, 1].

    `df` must already have passed `validate_features` — predict is the hot
    path and skips re-validation. Returned array is positionally aligned
    with `df.index`.
    """
    import xgboost as xgb  # noqa: PLC0415  (intentional: lazy import, see module docstring)

    booster = _load_booster()
    if booster.num_features() != len(FEATURE_NAMES):
        raise FeatureCountMismatchError(
            f"Booster expects {booster.num_features()} features but FEATURE_NAMES has "
            f"{len(FEATURE_NAMES)}. Either the model was trained against a different "
            "feature set or features.py is out of sync with sql.FEATURE_SELECT_FRAGMENT."
        )

    features = feature_matrix(df)
    dmat = xgb.DMatrix(features, feature_names=list(FEATURE_NAMES))
    raw = booster.predict(dmat)

    scores = np.asarray(raw, dtype=np.float32).reshape(-1)
    # Scores below 0 or above 1 indicate a model mismatch (e.g. trained as
    # regression when it should be probability) — easier to debug here than
    # downstream in CH.
    if scores.size and (scores.min() < 0.0 or scores.max() > 1.0):
        raise ScoreRangeError(
            f"Booster returned scores outside [0, 1]: min={scores.min()}, max={scores.max()}. "
            "Model is likely not configured for probability output (objective should be "
            "binary:logistic / reg:logistic, or the booster needs an inverse_link wrapper)."
        )
    return scores
