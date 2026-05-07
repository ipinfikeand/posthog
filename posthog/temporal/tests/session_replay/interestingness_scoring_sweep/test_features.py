"""Unit tests for `interestingness_scoring_sweep.features.validate_features`.

The function is the runtime gate between the CH SELECT and the XGBoost
predict — every drift mode that could mis-score sessions in production is
covered here. Tests are pure pandas, no CH or xgboost in the picture.
"""

from __future__ import annotations

from typing import Any

import pytest

import numpy as np
import pandas as pd

from posthog.temporal.session_replay.interestingness_scoring_sweep.features import (
    FEATURE_NAMES,
    FEATURE_RANGES,
    FeatureValidationError,
    feature_matrix,
    validate_features,
)


class TestValidateFeaturesHappyPaths:
    def test_zero_row_dataframe_passes(self) -> None:
        row = dict.fromkeys(FEATURE_NAMES, 0.0)
        validate_features(pd.DataFrame([row]))

    def test_empty_dataframe_passes(self) -> None:
        validate_features(pd.DataFrame(columns=list(FEATURE_NAMES)))

    def test_nan_passes_for_float_features(self, feature_frame: pd.DataFrame) -> None:
        # XGBoost handles NaN natively; our SQL deliberately produces NULL → NaN
        # when denominators are zero. NaN must not be rejected.
        feature_frame.loc[0, "event_rate"] = float("nan")
        feature_frame.loc[1, "mouse_mean_x"] = float("nan")
        validate_features(feature_frame)

    def test_id_columns_alongside_features_pass(self, feature_frame: pd.DataFrame) -> None:
        # The CH SELECT returns id columns + features in one frame. validate_features
        # must accept that layout, not just bare features.
        df = feature_frame.copy()
        df["team_id"] = 42
        df["session_id_v7"] = "00000000-0000-7000-0000-000000000000"
        df["session_timestamp"] = pd.Timestamp("2026-01-01")
        validate_features(df)


class TestValidateFeaturesColumnSet:
    def test_missing_column_raises(self, feature_frame: pd.DataFrame) -> None:
        df = feature_frame.drop(columns=["click_rate"])
        with pytest.raises(FeatureValidationError, match=r"missing=\['click_rate'\]"):
            validate_features(df)

    def test_extra_feature_column_raises(self, feature_frame: pd.DataFrame) -> None:
        df = feature_frame.copy()
        df["bogus_extra_feature"] = 0.0
        with pytest.raises(FeatureValidationError, match=r"extra=\['bogus_extra_feature'\]"):
            validate_features(df)

    def test_reordered_columns_raise(self, feature_frame: pd.DataFrame) -> None:
        # Ordering matters because we pass feature_names to DMatrix positionally.
        # Silent reordering would change predictions without any error surface.
        cols = list(feature_frame.columns)
        cols[0], cols[1] = cols[1], cols[0]
        with pytest.raises(FeatureValidationError, match="order mismatch"):
            validate_features(feature_frame.loc[:, cols])


class TestValidateFeaturesRanges:
    @pytest.mark.parametrize(
        ("column", "bad_value"),
        [
            ("event_rate", -0.5),
            ("network_failure_ratio", 1.5),
            ("backspace_ratio", -0.01),
            ("mouse_stddev_x", -1.0),
            ("login_path_visit_count", -3),
        ],
    )
    def test_out_of_range_value_raises(
        self,
        column: str,
        bad_value: Any,
        feature_frame: pd.DataFrame,
    ) -> None:
        feature_frame.loc[0, column] = bad_value
        with pytest.raises(FeatureValidationError, match=column):
            validate_features(feature_frame)

    @pytest.mark.parametrize("bad_value", [float("inf"), float("-inf")])
    def test_inf_rejected(self, bad_value: float, feature_frame: pd.DataFrame) -> None:
        feature_frame.loc[0, "event_rate"] = bad_value
        with pytest.raises(FeatureValidationError, match="non-finite"):
            validate_features(feature_frame)

    def test_unbounded_column_accepts_large_value(self, feature_frame: pd.DataFrame) -> None:
        # Rates have no upper bound — high counts on short sessions can produce
        # arbitrarily large rates. Don't false-positive on legitimate data.
        feature_frame.loc[0, "event_rate"] = 1e6
        validate_features(feature_frame)

    def test_any_float_column_accepts_negative(self, feature_frame: pd.DataFrame) -> None:
        # mouse_mean_x has no lower bound — mouse can be off-screen.
        feature_frame.loc[0, "mouse_mean_x"] = -1234.5
        validate_features(feature_frame)


class TestValidateFeaturesDtypes:
    def test_string_dtype_rejected_for_numeric_feature(self, feature_frame: pd.DataFrame) -> None:
        df = feature_frame.copy()
        df["event_rate"] = df["event_rate"].astype(str)
        with pytest.raises(FeatureValidationError, match="dtype"):
            validate_features(df)

    def test_int_dtype_accepted_for_count_column(self, feature_frame: pd.DataFrame) -> None:
        # Count columns can come back as int64 (no NULLs) or float64 (some NULLs)
        # depending on what's in the chunk — both must pass.
        df = feature_frame.copy()
        df["login_path_visit_count"] = df["login_path_visit_count"].astype(np.int64)
        validate_features(df)

    def test_float_dtype_accepted_for_count_column(self, feature_frame: pd.DataFrame) -> None:
        df = feature_frame.copy()
        df["login_path_visit_count"] = df["login_path_visit_count"].astype(np.float64)
        validate_features(df)


class TestFeatureMatrix:
    def test_strips_id_columns(self, feature_frame: pd.DataFrame) -> None:
        df = feature_frame.copy()
        df["team_id"] = 1
        df["session_id_v7"] = "00000000-0000-7000-0000-000000000000"
        df["session_timestamp"] = pd.Timestamp("2026-01-01")

        out = feature_matrix(df)

        assert list(out.columns) == list(FEATURE_NAMES)
        assert "team_id" not in out.columns
        assert len(out) == len(feature_frame)

    def test_preserves_row_order(self, feature_frame: pd.DataFrame) -> None:
        # Predict re-attaches scores positionally — a row reorder here would
        # cross-write scores to the wrong sessions.
        out = feature_matrix(feature_frame)
        pd.testing.assert_series_equal(out[FEATURE_NAMES[0]], feature_frame[FEATURE_NAMES[0]])

    def test_reorders_to_trained_order(self, feature_frame: pd.DataFrame) -> None:
        # Even if the input frame has features in a different order (e.g. CH
        # returns them in a different sequence after a refactor), feature_matrix
        # must hand back columns in FEATURE_NAMES order so DMatrix construction
        # is consistent.
        shuffled_cols = list(reversed(FEATURE_NAMES))
        out = feature_matrix(feature_frame.loc[:, shuffled_cols])
        assert list(out.columns) == list(FEATURE_NAMES)


class TestSchemaConsistency:
    def test_feature_names_and_ranges_in_lockstep(self) -> None:
        # The module has an import-time assert too, but a unit test makes the
        # invariant visible in test failure output if the import-time guard
        # is ever weakened.
        assert set(FEATURE_RANGES.keys()) == set(FEATURE_NAMES)
