"""Temporal pipeline that writes interestingness scores onto raw_sessions_v3.

See README.md in this directory for the architecture rationale.
"""

from posthog.temporal.session_scoring.activities import list_chunks_activity, score_chunk_activity
from posthog.temporal.session_scoring.workflow import ScoreSessionsBatchWorkflow

SESSION_SCORING_WORKFLOWS = [ScoreSessionsBatchWorkflow]
SESSION_SCORING_ACTIVITIES = [list_chunks_activity, score_chunk_activity]
