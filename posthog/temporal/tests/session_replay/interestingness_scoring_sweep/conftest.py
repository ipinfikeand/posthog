"""Shared fixtures for the interestingness_scoring_sweep tests.

The interesting fixture is `trained_model_path` — it trains a real XGBoost
booster against synthetic data with the production `FEATURE_NAMES` schema,
saves it to a tmp `.ubj` file, points `SESSION_INTERESTINGNESS_MODEL_PATH`
at it, and resets the scorer module's singleton. Every test that exercises
`scorer.predict` runs against an actual booster loaded from disk — the same
code path the worker hits at runtime.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

import numpy as np
import pandas as pd
import xgboost as xgb

from posthog.temporal.session_replay.interestingness_scoring_sweep import scorer as scorer_mod
from posthog.temporal.session_replay.interestingness_scoring_sweep.features import FEATURE_NAMES


def _synthetic_training_frame(rows: int, *, seed: int) -> pd.DataFrame:
    """Return a DataFrame with FEATURE_NAMES columns and uniform-random values.

    Values are deliberately kept in [0, 1] so the same frame works for any
    feature, including the strict-ratio columns. Labels are a simple linear
    rule on the first feature so the trained booster has a real signal and
    won't degenerate to a constant prediction.
    """
    rng = np.random.default_rng(seed)
    data = rng.random((rows, len(FEATURE_NAMES))).astype(np.float32)
    df = pd.DataFrame(data, columns=list(FEATURE_NAMES))
    df["__label__"] = (df[FEATURE_NAMES[0]] > 0.5).astype(np.int32)
    return df


def _train_booster(df: pd.DataFrame, *, objective: str = "binary:logistic") -> xgb.Booster:
    """Train a tiny 2-tree booster on `df` with `FEATURE_NAMES` features and column `__label__`."""
    features = df[list(FEATURE_NAMES)]
    labels = df["__label__"].to_numpy()
    dmat = xgb.DMatrix(features, label=labels, feature_names=list(FEATURE_NAMES))
    return xgb.train(
        {"objective": objective, "max_depth": 2, "eta": 0.5, "verbosity": 0},
        dmat,
        num_boost_round=2,
    )


@pytest.fixture
def trained_model_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Train + persist a real binary:logistic booster, point env var at it, reset singleton.

    Test functions that need scoring should depend on this fixture. Cleanup:
    the booster cache in `scorer` is reset both before and after so test
    ordering doesn't leak a model trained for a different test.
    """
    df = _synthetic_training_frame(rows=128, seed=42)
    booster = _train_booster(df)
    model_path = tmp_path / "model.ubj"
    booster.save_model(str(model_path))

    monkeypatch.setenv("SESSION_INTERESTINGNESS_MODEL_PATH", str(model_path))
    scorer_mod._BOOSTER = None
    yield model_path
    scorer_mod._BOOSTER = None


@pytest.fixture
def regression_model_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Train a `reg:squarederror` booster — used to exercise the out-of-range guard.

    Regression objective emits raw scores whose magnitude depends on the
    label range; we deliberately train with labels well outside [0, 1] so
    the model produces predictions that should trigger `ScoreRangeError`.
    """
    df = _synthetic_training_frame(rows=128, seed=7)
    df["__label__"] = (df[FEATURE_NAMES[0]] * 100).astype(np.float32)  # labels in [0, 100]
    booster = _train_booster(df, objective="reg:squarederror")
    model_path = tmp_path / "regression_model.ubj"
    booster.save_model(str(model_path))

    monkeypatch.setenv("SESSION_INTERESTINGNESS_MODEL_PATH", str(model_path))
    scorer_mod._BOOSTER = None
    yield model_path
    scorer_mod._BOOSTER = None


@pytest.fixture
def feature_frame() -> pd.DataFrame:
    """A small, valid feature DataFrame with FEATURE_NAMES columns."""
    rng = np.random.default_rng(123)
    rows = 16
    data = rng.random((rows, len(FEATURE_NAMES))).astype(np.float32)
    return pd.DataFrame(data, columns=list(FEATURE_NAMES))
