"""Integration tests for `interestingness_scoring_sweep.scorer`.

These tests use a real XGBoost booster trained in-memory by the
`trained_model_path` fixture, saved to disk, and loaded through the same
code path the production worker hits. No mocking of xgboost itself —
the goal is to catch regressions in the actual load + predict path.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

import numpy as np
import pandas as pd

from posthog.temporal.session_replay.interestingness_scoring_sweep import scorer as scorer_mod
from posthog.temporal.session_replay.interestingness_scoring_sweep.features import FEATURE_NAMES
from posthog.temporal.session_replay.interestingness_scoring_sweep.scorer import (
    FeatureCountMismatchError,
    ScoreRangeError,
    _load_booster,
    predict,
    warmup,
)


class TestModelLoading:
    def test_load_booster_loads_from_env_var_path(self, trained_model_path: Path) -> None:
        booster = _load_booster()
        # If load failed, _load_booster would raise; getting a Booster back is
        # the main assertion. num_features is a cheap sanity probe.
        assert booster.num_features() == len(FEATURE_NAMES)

    def test_load_booster_caches_singleton(self, trained_model_path: Path) -> None:
        # The hot path (every chunk's predict) hits this. It must hand back
        # the same Booster object on every call, not re-load from disk.
        first = _load_booster()
        second = _load_booster()
        third = _load_booster()
        assert first is second is third

    def test_load_booster_thread_safe(self, trained_model_path: Path) -> None:
        # If `max_concurrent_activities > 1`, the first chunks contend on
        # _load_booster simultaneously. The double-checked lock must produce
        # exactly one booster. We probe for that by snapshotting the cache
        # mid-flight from many threads and asserting they all converge.
        boosters = []
        barrier = threading.Barrier(8)

        def worker() -> None:
            barrier.wait()
            boosters.append(_load_booster())

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(boosters) == 8
        assert all(b is boosters[0] for b in boosters)

    def test_warmup_loads_eagerly(self, trained_model_path: Path) -> None:
        assert scorer_mod._BOOSTER is None
        warmup()
        assert scorer_mod._BOOSTER is not None

    def test_load_booster_raises_when_path_missing(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        # Operator misconfiguration (model file not mounted) should fail loud
        # at the first chunk, not silently return a default booster.
        nonexistent = tmp_path / "does_not_exist.ubj"
        monkeypatch.setenv("SESSION_INTERESTINGNESS_MODEL_PATH", str(nonexistent))
        scorer_mod._BOOSTER = None
        with pytest.raises(Exception):
            _load_booster()


class TestPredict:
    def test_predict_returns_scores_in_unit_interval(
        self, trained_model_path: Path, feature_frame: pd.DataFrame
    ) -> None:
        # Booster was trained binary:logistic — every prediction must land in [0, 1].
        scores = predict(feature_frame)

        assert isinstance(scores, np.ndarray)
        assert scores.dtype == np.float32
        assert scores.shape == (len(feature_frame),)
        assert scores.min() >= 0.0
        assert scores.max() <= 1.0

    def test_predict_handles_nan_features(self, trained_model_path: Path, feature_frame: pd.DataFrame) -> None:
        # Our SQL produces NaN when denominators are zero (nullIf(...)).
        # XGBoost handles NaN natively; predict must not raise or crash.
        feature_frame.loc[0, "event_rate"] = float("nan")
        feature_frame.loc[1, "mouse_velocity_mean"] = float("nan")

        scores = predict(feature_frame)

        assert np.isfinite(scores).all()

    def test_predict_with_id_columns_alongside_features(
        self, trained_model_path: Path, feature_frame: pd.DataFrame
    ) -> None:
        # The CH SELECT returns id columns + features mixed together. predict
        # delegates to feature_matrix, which strips ids; this end-to-end test
        # protects that contract.
        df = feature_frame.copy()
        df["team_id"] = 42
        df["session_id_v7"] = "00000000-0000-7000-0000-000000000000"
        df["session_timestamp"] = pd.Timestamp("2026-01-01")

        scores = predict(df)

        assert scores.shape == (len(feature_frame),)

    def test_predict_is_invariant_to_input_column_order(
        self, trained_model_path: Path, feature_frame: pd.DataFrame
    ) -> None:
        # We pass feature_names into DMatrix so XGBoost reorders by name, not
        # position. That means we get the same scores regardless of how the
        # caller orders the columns — a regression here would silently mis-score.
        scores_a = predict(feature_frame)
        shuffled = feature_frame.loc[:, list(reversed(FEATURE_NAMES))]
        scores_b = predict(shuffled)

        np.testing.assert_array_equal(scores_a, scores_b)

    def test_predict_empty_dataframe(self, trained_model_path: Path) -> None:
        df = pd.DataFrame(columns=list(FEATURE_NAMES))
        scores = predict(df)
        assert scores.shape == (0,)


class TestPredictGuards:
    def test_score_out_of_range_raises(self, regression_model_path: Path, feature_frame: pd.DataFrame) -> None:
        # Training a regression model with labels in [0, 100] → predictions
        # well above 1. predict must reject this loudly rather than write
        # garbage scores into ClickHouse.
        with pytest.raises(ScoreRangeError, match=r"outside \[0, 1\]"):
            predict(feature_frame)

    def test_feature_count_mismatch_raises(
        self,
        feature_frame: pd.DataFrame,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Simulate deploying a model trained against a different feature set —
        # train a booster with 3 features, point env var at it, and call predict
        # with the production 61-feature frame. Booster.num_features() (3) won't
        # match len(FEATURE_NAMES) (61), so the guard must fire.
        import xgboost as xgb  # noqa: PLC0415  (test-local; matches scorer's lazy-import policy)

        rng = np.random.default_rng(0)
        small_features = pd.DataFrame(rng.random((32, 3)), columns=["a", "b", "c"])
        labels = (small_features["a"] > 0.5).astype(np.int32).to_numpy()
        dmat = xgb.DMatrix(small_features, label=labels, feature_names=list(small_features.columns))
        small_booster = xgb.train(
            {"objective": "binary:logistic", "max_depth": 2, "eta": 0.5, "verbosity": 0},
            dmat,
            num_boost_round=2,
        )
        path = tmp_path / "small_model.ubj"
        small_booster.save_model(str(path))

        monkeypatch.setenv("SESSION_INTERESTINGNESS_MODEL_PATH", str(path))
        scorer_mod._BOOSTER = None

        try:
            with pytest.raises(FeatureCountMismatchError, match="Booster expects"):
                predict(feature_frame)
        finally:
            scorer_mod._BOOSTER = None
