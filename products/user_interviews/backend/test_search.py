from typing import Any

from posthog.test.base import APIBaseTest
from unittest.mock import MagicMock, patch

from parameterized import parameterized
from rest_framework import status

from products.user_interviews.backend.models import UserInterview, UserInterviewTopic


class _FeatureFlagEnabledMixin(APIBaseTest):
    def setUp(self) -> None:
        super().setUp()
        patcher = patch("posthoganalytics.feature_enabled", return_value=True)
        patcher.start()
        self.addCleanup(patcher.stop)


class TestUserInterviewSearch(_FeatureFlagEnabledMixin):
    def setUp(self) -> None:
        super().setUp()
        self.topic = UserInterviewTopic.objects.create(
            team=self.team,
            created_by=self.user,
            interviewee_emails=["alex@example.com"],
            topic="Replay adoption",
            agent_context="ctx",
            questions=[],
        )
        self.interview_a = UserInterview.objects.create(
            team=self.team,
            topic=self.topic,
            interviewee_identifier="alex@example.com",
            interviewee_emails=["alex@example.com"],
            transcript="alex talked about session replay buffering",
            summary="alex finds session replay slow on long sessions",
            created_by=self.user,
        )
        self.interview_b = UserInterview.objects.create(
            team=self.team,
            topic=self.topic,
            interviewee_identifier="bob@example.com",
            interviewee_emails=["bob@example.com"],
            transcript="bob loves heatmaps but ignores replays",
            summary="bob uses heatmaps daily",
            created_by=self.user,
        )

    def _url(self) -> str:
        return f"/api/environments/{self.team.id}/user_interviews/search/"

    def _embedding_response(self) -> MagicMock:
        resp = MagicMock()
        resp.embedding = [0.0] * 3072
        resp.tokens_used = 4
        resp.did_truncate = False
        return resp

    def _hogql_rows(self, rows: list[tuple[Any, ...]]) -> MagicMock:
        result = MagicMock()
        result.results = rows
        return result

    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_returns_ranked_matches(self, mock_embed, mock_hogql):
        mock_embed.return_value = self._embedding_response()
        mock_hogql.return_value = self._hogql_rows(
            [
                (str(self.interview_a.id), "transcript", "alex talked about session replay buffering", 0.12),
                (str(self.interview_a.id), "summary", "alex finds session replay slow on long sessions", 0.22),
                (str(self.interview_b.id), "transcript", "bob loves heatmaps but ignores replays", 0.48),
            ]
        )

        response = self.client.post(self._url(), {"query": "is session replay slow?"}, content_type="application/json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)

        body = response.json()
        self.assertEqual(len(body), 3)
        self.assertEqual(body[0]["interview_id"], str(self.interview_a.id))
        self.assertEqual(body[0]["document_type"], "transcript")
        self.assertAlmostEqual(body[0]["similarity"], 0.88, places=5)
        self.assertEqual(body[0]["content_snippet"], "alex talked about session replay buffering")
        self.assertEqual(body[0]["interviewee_identifier"], "alex@example.com")
        self.assertEqual(body[0]["topic_id"], str(self.topic.id))
        self.assertGreater(body[0]["similarity"], body[2]["similarity"])

        mock_embed.assert_called_once()
        embed_args, embed_kwargs = mock_embed.call_args
        self.assertEqual(embed_args[0], self.team)
        self.assertEqual(embed_args[1], "is session replay slow?")
        self.assertEqual(embed_kwargs["model"], "text-embedding-3-large-3072")

    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_caps_similarity_at_zero(self, mock_embed, mock_hogql):
        mock_embed.return_value = self._embedding_response()
        mock_hogql.return_value = self._hogql_rows([(str(self.interview_a.id), "transcript", "x", 1.4)])
        response = self.client.post(self._url(), {"query": "x"}, content_type="application/json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()[0]["similarity"], 0.0)

    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_truncates_long_content_snippet(self, mock_embed, mock_hogql):
        mock_embed.return_value = self._embedding_response()
        long_content = "x" * 1000
        mock_hogql.return_value = self._hogql_rows([(str(self.interview_a.id), "transcript", long_content, 0.1)])
        response = self.client.post(self._url(), {"query": "x"}, content_type="application/json")
        self.assertEqual(len(response.json()[0]["content_snippet"]), 500)

    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_skips_rows_for_deleted_interviews(self, mock_embed, mock_hogql):
        mock_embed.return_value = self._embedding_response()
        ghost_id = "00000000-0000-0000-0000-000000000000"
        mock_hogql.return_value = self._hogql_rows(
            [
                (str(self.interview_a.id), "transcript", "kept", 0.1),
                (ghost_id, "transcript", "orphan", 0.2),
            ]
        )
        response = self.client.post(self._url(), {"query": "x"}, content_type="application/json")
        body = response.json()
        self.assertEqual(len(body), 1)
        self.assertEqual(body[0]["interview_id"], str(self.interview_a.id))

    @parameterized.expand(
        [
            ("transcript_only", ["transcript"], {"transcript"}),
            ("summary_only", ["summary"], {"summary"}),
            ("both_explicit", ["transcript", "summary"], {"transcript", "summary"}),
        ]
    )
    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_forwards_document_types_filter(
        self, _name, document_types, expected_in_placeholders, mock_embed, mock_hogql
    ):
        mock_embed.return_value = self._embedding_response()
        mock_hogql.return_value = self._hogql_rows([])

        self.client.post(
            self._url(),
            {"query": "x", "document_types": document_types},
            content_type="application/json",
        )

        mock_hogql.assert_called_once()
        placeholders = mock_hogql.call_args.kwargs["placeholders"]
        self.assertEqual(set(placeholders["document_types"].value), expected_in_placeholders)

    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_forwards_topic_id_filter(self, mock_embed, mock_hogql):
        mock_embed.return_value = self._embedding_response()
        mock_hogql.return_value = self._hogql_rows([])

        self.client.post(
            self._url(),
            {"query": "x", "topic_id": str(self.topic.id)},
            content_type="application/json",
        )

        placeholders = mock_hogql.call_args.kwargs["placeholders"]
        self.assertEqual(placeholders["topic_id"].value, str(self.topic.id))
        hogql_query = mock_hogql.call_args.kwargs["query"]
        self.assertIn("JSONExtractString(metadata, 'topic_id')", hogql_query)

    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_defaults_to_both_document_types_when_unset(self, mock_embed, mock_hogql):
        mock_embed.return_value = self._embedding_response()
        mock_hogql.return_value = self._hogql_rows([])

        self.client.post(self._url(), {"query": "x"}, content_type="application/json")

        placeholders = mock_hogql.call_args.kwargs["placeholders"]
        self.assertEqual(set(placeholders["document_types"].value), {"transcript", "summary"})

    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_enforces_default_limit_when_omitted(self, mock_embed, mock_hogql):
        mock_embed.return_value = self._embedding_response()
        mock_hogql.return_value = self._hogql_rows([])

        self.client.post(self._url(), {"query": "x"}, content_type="application/json")

        placeholders = mock_hogql.call_args.kwargs["placeholders"]
        self.assertEqual(placeholders["limit"].value, 10)

    def test_search_rejects_query_above_max_length(self):
        long_query = "x" * 2001
        response = self.client.post(self._url(), {"query": long_query}, content_type="application/json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_search_rejects_limit_above_max(self):
        response = self.client.post(
            self._url(),
            {"query": "x", "limit": 51},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_search_rejects_invalid_document_type(self):
        response = self.client.post(
            self._url(),
            {"query": "x", "document_types": ["nonsense"]},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_search_rejects_missing_query(self):
        response = self.client.post(self._url(), {}, content_type="application/json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    @patch("products.user_interviews.backend.api.execute_hogql_query")
    @patch("products.user_interviews.backend.api.generate_embedding")
    def test_search_does_not_leak_across_teams(self, mock_embed, mock_hogql):
        mock_embed.return_value = self._embedding_response()
        mock_hogql.return_value = self._hogql_rows([])

        self.client.post(self._url(), {"query": "x"}, content_type="application/json")

        placeholders = mock_hogql.call_args.kwargs["placeholders"]
        self.assertEqual(placeholders["team_id"].value, self.team.id)
