from dataclasses import dataclass, field
from typing import Optional

from django.db import models
from django.utils import timezone

from posthog.models.utils import RootTeamMixin, UUIDModel

UNMATCHED_SAMPLE_CAP = 50


class CohortCSVImport(RootTeamMixin, UUIDModel):
    """
    Track CSV import attempts for static cohorts.

    Populated in two stages:
      1. Synchronously in the API request after parsing the CSV (rows_total,
         rows_skipped, ids_submitted, id_type, email_property_key, filename).
      2. Asynchronously by `calculate_cohort_from_list` after person matching
         and insertion completes (persons_matched, persons_added,
         persons_already_in_cohort, unmatched_count, unmatched_sample,
         finished_at, error).
    """

    ID_TYPE_DISTINCT_ID = "distinct_id"
    ID_TYPE_PERSON_ID = "person_id"
    ID_TYPE_EMAIL = "email"
    ID_TYPE_CHOICES = [
        (ID_TYPE_DISTINCT_ID, "Distinct ID"),
        (ID_TYPE_PERSON_ID, "Person ID"),
        (ID_TYPE_EMAIL, "Email"),
    ]

    team = models.ForeignKey("posthog.Team", on_delete=models.CASCADE)
    cohort = models.ForeignKey("posthog.Cohort", on_delete=models.CASCADE, related_name="csv_imports")
    created_by = models.ForeignKey("User", on_delete=models.SET_NULL, null=True, blank=True)

    # Lifecycle
    started_at = models.DateTimeField(default=timezone.now, help_text="When the upload was received")
    finished_at = models.DateTimeField(null=True, blank=True, help_text="When async matching/insertion completed")

    # Source metadata
    filename = models.CharField(max_length=512, null=True, blank=True, help_text="Uploaded filename, if available")
    id_type = models.CharField(max_length=32, choices=ID_TYPE_CHOICES, help_text="How rows were interpreted")
    email_property_key = models.CharField(
        max_length=200,
        null=True,
        blank=True,
        help_text="Person property key matched against, when id_type='email'",
    )

    # Parse stage (sync)
    rows_total = models.PositiveIntegerField(null=True, blank=True, help_text="Total data rows read from the CSV")
    rows_skipped = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Rows skipped due to malformed structure (wrong column count, empty cells)",
    )
    ids_submitted = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Number of non-empty IDs handed off to the matcher",
    )

    # Match/insert stage (async)
    persons_matched = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Unique persons resolved from the submitted IDs",
    )
    persons_added = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Persons newly inserted into the cohort",
    )
    persons_already_in_cohort = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Matched persons that were already cohort members",
    )
    unmatched_count = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Submitted IDs that did not resolve to a person",
    )
    unmatched_sample = models.JSONField(
        null=True,
        blank=True,
        help_text=f"Up to {UNMATCHED_SAMPLE_CAP} unmatched IDs for user feedback",
    )

    # Errors
    error = models.TextField(null=True, blank=True, help_text="Error message if the import failed")
    error_code = models.CharField(
        max_length=64,
        null=True,
        blank=True,
        help_text="Error code for categorizing failures (e.g., 'parse_error', 'no_valid_ids')",
    )

    class Meta:
        db_table = "posthog_cohortcsvimport"
        indexes = [
            models.Index(fields=["team", "cohort", "-started_at"]),
            models.Index(fields=["cohort", "-started_at"]),
        ]

    def __str__(self) -> str:
        return f"CohortCSVImport(cohort={self.cohort_id}, started_at={self.started_at})"

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def is_completed(self) -> bool:
        return self.finished_at is not None

    @property
    def is_successful(self) -> bool:
        return self.is_completed and self.error is None


@dataclass
class CSVImportTracker:
    """
    Accumulates per-stage metrics across batches during async matching/insertion.

    Persists to a `CohortCSVImport` record via `apply_to`.
    """

    persons_matched: int = 0
    persons_added: int = 0
    persons_already_in_cohort: int = 0
    unmatched_count: int = 0
    unmatched_sample: list[str] = field(default_factory=list)
    _matched_person_uuids: set[str] = field(default_factory=set)

    def record_matched(self, matched_person_uuids: list[str]) -> None:
        for uuid in matched_person_uuids:
            uuid_str = str(uuid)
            if uuid_str not in self._matched_person_uuids:
                self._matched_person_uuids.add(uuid_str)
                self.persons_matched += 1

    def record_unmatched(self, ids: list[str]) -> None:
        self.unmatched_count += len(ids)
        remaining_capacity = UNMATCHED_SAMPLE_CAP - len(self.unmatched_sample)
        if remaining_capacity > 0 and ids:
            self.unmatched_sample.extend(str(item) for item in ids[:remaining_capacity])

    def record_added(self, count: int) -> None:
        self.persons_added += count

    def record_already_in_cohort(self, count: int) -> None:
        self.persons_already_in_cohort += count

    def apply_to(self, import_record: CohortCSVImport) -> None:
        import_record.persons_matched = self.persons_matched
        import_record.persons_added = self.persons_added
        import_record.persons_already_in_cohort = self.persons_already_in_cohort
        import_record.unmatched_count = self.unmatched_count
        import_record.unmatched_sample = list(self.unmatched_sample)
