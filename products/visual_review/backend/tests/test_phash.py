"""Tests for perceptual-hash tolerance matching."""

import io

import pytest

from PIL import Image, ImageDraw

from products.visual_review.backend import logic
from products.visual_review.backend.diffing import _diff_snapshot
from products.visual_review.backend.facade.enums import ClassificationReason, RunType, SnapshotResult
from products.visual_review.backend.models import Artifact, Run, RunSnapshot, ToleratedHash
from products.visual_review.backend.phash import (
    HAMMING_TOLERANCE_BITS,
    compute_phash,
    hamming_distance,
    is_within_tolerance,
)
from products.visual_review.backend.tests.conftest import PRODUCT_DATABASES


def _render_card(background: tuple[int, int, int] = (245, 245, 245), shift: int = 0) -> bytes:
    """A stable synthetic render with optional per-pixel shift.

    shift=0 baseline; shift=1-2 mimics antialiasing drift (phash should
    tolerate); shift=40+ is a genuine visual change (phash should reject).
    """
    img = Image.new("RGB", (200, 200), background)
    draw = ImageDraw.Draw(img)
    draw.rectangle((20, 20, 180, 180), fill=(255, 255, 255), outline=(220, 220, 220))
    draw.rectangle((30, 30, 170, 40), fill=(80 + shift, 80 + shift, 80 + shift))
    draw.rectangle((30, 50, 140, 60), fill=(80 + shift, 80 + shift, 80 + shift))
    draw.rectangle((140, 160, 175, 175), fill=(50, 100 + shift, 200))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class TestComputePhash:
    def test_deterministic(self):
        img = _render_card()
        assert compute_phash(img) == compute_phash(img)

    def test_returns_16_hex_chars(self):
        h = compute_phash(_render_card())
        assert len(h) == 16
        int(h, 16)  # parses as hex

    def test_identical_images_match(self):
        a = _render_card()
        b = _render_card()
        assert hamming_distance(compute_phash(a), compute_phash(b)) == 0

    def test_tiny_drift_within_tolerance(self):
        baseline = compute_phash(_render_card(shift=0))
        drifted = compute_phash(_render_card(shift=2))
        assert is_within_tolerance(baseline, drifted)

    def test_large_change_outside_tolerance(self):
        baseline = compute_phash(_render_card(shift=0))
        changed = compute_phash(_render_card(background=(20, 20, 180), shift=40))
        assert not is_within_tolerance(baseline, changed)


class TestHammingDistance:
    @pytest.mark.parametrize(
        "a,b,expected",
        [
            ("0000000000000000", "0000000000000000", 0),
            ("0000000000000000", "0000000000000001", 1),
            ("0000000000000000", "ffffffffffffffff", 64),
            ("ff00ff00ff00ff00", "00ff00ff00ff00ff", 64),
        ],
    )
    def test_known_distances(self, a, b, expected):
        assert hamming_distance(a, b) == expected

    @pytest.mark.parametrize("a,b", [("", "abcd"), ("abcd", ""), ("abc", "abcd"), ("zzzz", "abcd")])
    def test_malformed_inputs_return_max(self, a, b):
        assert hamming_distance(a, b) == 64

    def test_tolerance_threshold_sanity(self):
        # Ensure the default threshold is conservative (well below half)
        assert 0 < HAMMING_TOLERANCE_BITS < 32


@pytest.mark.django_db(databases=PRODUCT_DATABASES)
class TestDiffSnapshotPhashShortCircuit:
    """Integration: a CHANGED snapshot whose bytes are within Hamming
    tolerance of a stored alternate_phash gets reclassified as UNCHANGED
    without invoking compute_diff or compute_ssim.
    """

    @pytest.fixture
    def repo(self, team):
        return logic.create_repo(team_id=team.id, repo_external_id=88888, repo_full_name="org/phash-test")

    def _setup_snapshot(self, repo) -> tuple[RunSnapshot, bytes, bytes]:
        baseline_bytes = _render_card(shift=0)
        drifted_bytes = _render_card(shift=2)

        baseline_artifact = Artifact.objects.create(
            repo=repo, team_id=repo.team_id, content_hash="baseline_sha", storage_path="p/baseline"
        )
        current_artifact = Artifact.objects.create(
            repo=repo, team_id=repo.team_id, content_hash="current_sha", storage_path="p/current"
        )
        run = Run.objects.create(
            repo=repo,
            team_id=repo.team_id,
            run_type=RunType.STORYBOOK,
            commit_sha="abc",
            branch="main",
        )
        snapshot = RunSnapshot.objects.create(
            run=run,
            team_id=repo.team_id,
            identifier="Button",
            current_hash="current_sha",
            baseline_hash="baseline_sha",
            current_artifact=current_artifact,
            baseline_artifact=baseline_artifact,
            result=SnapshotResult.CHANGED,
        )
        return snapshot, baseline_bytes, drifted_bytes

    def test_short_circuits_when_phash_matches(self, repo, mocker):
        snapshot, baseline_bytes, drifted_bytes = self._setup_snapshot(repo)

        # Pre-seed a tolerated row with the phash of the drifted render
        tolerated = ToleratedHash.objects.create(
            repo=repo,
            team_id=repo.team_id,
            identifier=snapshot.identifier,
            baseline_hash=snapshot.baseline_hash,
            alternate_hash="some_prior_run_sha",  # deliberately not equal to current_hash
            alternate_phash=compute_phash(drifted_bytes),
            reason="auto_threshold",
        )

        mocker.patch(
            "products.visual_review.backend.logic.read_artifact_bytes",
            side_effect=lambda _repo_id, content_hash: (
                baseline_bytes if content_hash == "baseline_sha" else drifted_bytes
            ),
        )
        compute_diff_mock = mocker.patch("products.visual_review.backend.diffing.compute_diff")

        _diff_snapshot(snapshot)

        compute_diff_mock.assert_not_called()
        snapshot.refresh_from_db()
        assert snapshot.result == SnapshotResult.UNCHANGED
        assert snapshot.classification_reason == ClassificationReason.TOLERATED_HASH
        assert snapshot.tolerated_hash_match_id == tolerated.id

    def test_short_circuits_on_baseline_phash_match_without_stored_alternate(self, repo, mocker):
        """First-encounter drift: no tolerated row exists yet, but current is
        within Hamming tolerance of the baseline itself. Snapshot should be
        reclassified UNCHANGED with reason=BASELINE_PHASH, compute_diff should
        not run, and a tolerated row should be seeded for future runs."""
        snapshot, baseline_bytes, drifted_bytes = self._setup_snapshot(repo)

        assert not ToleratedHash.objects.filter(
            repo=repo, identifier=snapshot.identifier, baseline_hash=snapshot.baseline_hash
        ).exists()

        mocker.patch(
            "products.visual_review.backend.logic.read_artifact_bytes",
            side_effect=lambda _repo_id, content_hash: (
                baseline_bytes if content_hash == "baseline_sha" else drifted_bytes
            ),
        )
        compute_diff_mock = mocker.patch("products.visual_review.backend.diffing.compute_diff")

        _diff_snapshot(snapshot)

        compute_diff_mock.assert_not_called()
        snapshot.refresh_from_db()
        assert snapshot.result == SnapshotResult.UNCHANGED
        assert snapshot.classification_reason == ClassificationReason.BASELINE_PHASH
        assert snapshot.tolerated_hash_match_id is None

        seeded = ToleratedHash.objects.get(
            repo=repo,
            identifier=snapshot.identifier,
            baseline_hash=snapshot.baseline_hash,
            alternate_hash=snapshot.current_hash,
        )
        assert seeded.alternate_phash == compute_phash(drifted_bytes)

    def test_baseline_phash_match_skipped_when_distant(self, repo, mocker):
        """Distant renders should not trigger the baseline-phash short-circuit
        and should fall through to the normal diff path."""
        snapshot, baseline_bytes, _ = self._setup_snapshot(repo)
        distant_bytes = _render_card(background=(20, 20, 180), shift=40)

        mocker.patch(
            "products.visual_review.backend.logic.read_artifact_bytes",
            side_effect=lambda _repo_id, content_hash: (
                baseline_bytes if content_hash == "baseline_sha" else distant_bytes
            ),
        )
        mocker.patch(
            "products.visual_review.backend.diffing.compute_diff",
            return_value=mocker.Mock(diff_percentage=50.0, diff_pixel_count=1000),
        )
        mocker.patch("products.visual_review.backend.diffing._store_diff")

        _diff_snapshot(snapshot)

        snapshot.refresh_from_db()
        assert snapshot.result == SnapshotResult.CHANGED
        assert snapshot.classification_reason != ClassificationReason.BASELINE_PHASH

    def test_falls_through_when_phash_outside_tolerance(self, repo, mocker):
        snapshot, baseline_bytes, _ = self._setup_snapshot(repo)
        distant_bytes = _render_card(background=(20, 20, 180), shift=40)

        ToleratedHash.objects.create(
            repo=repo,
            team_id=repo.team_id,
            identifier=snapshot.identifier,
            baseline_hash=snapshot.baseline_hash,
            alternate_hash="prior",
            alternate_phash=compute_phash(baseline_bytes),
            reason="auto_threshold",
        )

        mocker.patch(
            "products.visual_review.backend.logic.read_artifact_bytes",
            side_effect=lambda _repo_id, content_hash: (
                baseline_bytes if content_hash == "baseline_sha" else distant_bytes
            ),
        )
        # Short-circuit compute_diff with an "obvious change" result so
        # the test doesn't depend on pixelhog internals — we only care
        # that we fell through into the normal diff path.
        mocker.patch(
            "products.visual_review.backend.diffing.compute_diff",
            return_value=mocker.Mock(diff_percentage=50.0, diff_pixel_count=1000),
        )
        mocker.patch("products.visual_review.backend.diffing._store_diff")

        _diff_snapshot(snapshot)

        snapshot.refresh_from_db()
        # Still CHANGED — phash didn't rescue it, and the stored diff path ran
        assert snapshot.result == SnapshotResult.CHANGED
