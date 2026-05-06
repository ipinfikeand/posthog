from typing import Any

from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, override_settings

from posthog.api.services.flags_service import _INTERNAL_SECRET_HEADER, get_flags_from_service


def _ok_response(payload: dict[str, Any] | None = None) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = payload or {"flags": {}}
    response.raise_for_status.return_value = None
    return response


@override_settings(
    FEATURE_FLAGS_SERVICE_URL="http://flags-public:3001",
    INTERNAL_FLAGS_SERVICE_URL="http://flags-internal:3001",
    INTERNAL_FLAGS_SHARED_SECRET="",
)
class TestFlagsServicePublic(SimpleTestCase):
    @patch("posthog.api.services.flags_service._FLAGS_SERVICE_SESSION")
    def test_default_call_targets_public_endpoint(self, mock_session: MagicMock) -> None:
        mock_session.post.return_value = _ok_response()

        get_flags_from_service(token="phc_abc", distinct_id="user1")

        mock_session.post.assert_called_once()
        url = mock_session.post.call_args.args[0]
        self.assertEqual(url, "http://flags-public:3001/flags")

        kwargs = mock_session.post.call_args.kwargs
        self.assertEqual(kwargs["json"], {"token": "phc_abc", "distinct_id": "user1"})
        self.assertEqual(kwargs["params"], {"v": "2"})
        # No internal header on the public path.
        self.assertIsNone(kwargs.get("headers"))

    @patch("posthog.api.services.flags_service._FLAGS_SERVICE_SESSION")
    def test_groups_are_forwarded(self, mock_session: MagicMock) -> None:
        mock_session.post.return_value = _ok_response()

        get_flags_from_service(token="phc_abc", distinct_id="user1", groups={"company": "acme"})

        kwargs = mock_session.post.call_args.kwargs
        self.assertEqual(
            kwargs["json"],
            {"token": "phc_abc", "distinct_id": "user1", "groups": {"company": "acme"}},
        )


@override_settings(
    FEATURE_FLAGS_SERVICE_URL="http://flags-public:3001",
    INTERNAL_FLAGS_SERVICE_URL="http://flags-internal:3001",
    INTERNAL_FLAGS_SHARED_SECRET="hunter2",
)
class TestFlagsServiceInternal(SimpleTestCase):
    @patch("posthog.api.services.flags_service._FLAGS_SERVICE_SESSION")
    def test_internal_call_targets_internal_url_and_path(self, mock_session: MagicMock) -> None:
        mock_session.post.return_value = _ok_response()

        get_flags_from_service(token="phc_abc", distinct_id="user1", internal=True)

        url = mock_session.post.call_args.args[0]
        self.assertEqual(url, "http://flags-internal:3001/internal/flags")

    @patch("posthog.api.services.flags_service._FLAGS_SERVICE_SESSION")
    def test_internal_call_includes_shared_secret_header(self, mock_session: MagicMock) -> None:
        mock_session.post.return_value = _ok_response()

        get_flags_from_service(token="phc_abc", distinct_id="user1", internal=True)

        kwargs = mock_session.post.call_args.kwargs
        self.assertEqual(kwargs["headers"], {_INTERNAL_SECRET_HEADER: "hunter2"})


@override_settings(
    FEATURE_FLAGS_SERVICE_URL="http://flags-public:3001",
    INTERNAL_FLAGS_SERVICE_URL="http://flags-internal:3001",
    INTERNAL_FLAGS_SHARED_SECRET="",
)
class TestFlagsServiceInternalNoSecret(SimpleTestCase):
    @patch("posthog.api.services.flags_service._FLAGS_SERVICE_SESSION")
    def test_internal_call_omits_header_when_secret_unset(self, mock_session: MagicMock) -> None:
        mock_session.post.return_value = _ok_response()

        get_flags_from_service(token="phc_abc", distinct_id="user1", internal=True)

        kwargs = mock_session.post.call_args.kwargs
        # `headers=None` (vs. `{}`) avoids requests merging an empty dict into
        # the session's default headers and keeps the call signature stable.
        self.assertIsNone(kwargs.get("headers"))


@override_settings(
    FEATURE_FLAGS_SERVICE_URL="http://flags-public:3001",
    INTERNAL_FLAGS_SHARED_SECRET="hunter2",
)
class TestFlagsServiceInternalUrlFallback(SimpleTestCase):
    @patch("posthog.api.services.flags_service._FLAGS_SERVICE_SESSION")
    def test_internal_url_falls_back_to_public_when_unset(self, mock_session: MagicMock) -> None:
        # INTERNAL_FLAGS_SERVICE_URL not set → use FEATURE_FLAGS_SERVICE_URL.
        # Important for local dev where there's only one flags binding.
        with override_settings(INTERNAL_FLAGS_SERVICE_URL=""):
            mock_session.post.return_value = _ok_response()
            get_flags_from_service(token="phc_abc", distinct_id="user1", internal=True)
            url = mock_session.post.call_args.args[0]
            self.assertEqual(url, "http://flags-public:3001/internal/flags")
