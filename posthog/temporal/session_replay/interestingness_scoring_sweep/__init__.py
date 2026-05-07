"""Temporal pipeline that writes interestingness scores onto raw_sessions_v3.

See README.md in this directory for the architecture rationale.
"""

from posthog.temporal.session_replay.interestingness_scoring_sweep.activities import (
    list_chunks_activity,
    score_chunk_activity,
)
from posthog.temporal.session_replay.interestingness_scoring_sweep.workflow import ScoreSessionsBatchWorkflow

INTERESTINGNESS_SCORING_SWEEP_WORKFLOWS = [ScoreSessionsBatchWorkflow]
INTERESTINGNESS_SCORING_SWEEP_ACTIVITIES = [list_chunks_activity, score_chunk_activity]
