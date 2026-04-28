from posthog.test.base import APIBaseTest, ClickhouseTestMixin
from unittest.mock import patch

from parameterized import parameterized
from rest_framework import status

from posthog.models.organization import Organization, OrganizationMembership
from posthog.models.project_secret_api_key import ProjectSecretAPIKey
from posthog.models.team import Team
from posthog.models.utils import hash_key_value

from products.endpoints.backend.tests.conftest import create_endpoint_with_version

SAMPLE_QUERY = {"kind": "HogQLQuery", "query": "SELECT 1"}
_UNSET = object()


def _ff_returns_true_for_hogql_access_control(flag_key, *args, **kwargs):
    return True if flag_key == "hogql-access-control" else False


def _make_psak(team, label="psak", scopes=_UNSET):
    # Token must match _SECRET_API_KEY_RE = r"^phs_[a-zA-Z0-9]+$", so only alphanumerics after phs_.
    suffix = "".join(c for c in label if c.isalnum())
    token = "phs_" + ("a" * 35) + suffix
    psak = ProjectSecretAPIKey.objects.create(
        team=team,
        label=label,
        mask_value=f"phs_...{suffix[:4]}",
        secure_value=hash_key_value(token),
        scopes=["endpoint:read"] if scopes is _UNSET else scopes,
    )
    return token, psak


class TestEndpointViewSetPSAKAuth(ClickhouseTestMixin, APIBaseTest):
    def setUp(self):
        super().setUp()
        self.endpoint = create_endpoint_with_version(
            name="my_endpoint",
            team=self.team,
            query=SAMPLE_QUERY,
            created_by=self.user,
        )
        # Log out the test client so only the PSAK header authenticates requests
        self.client.logout()

    def _auth_headers(self, token):
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def test_psak_can_run_endpoint(self):
        token, _ = _make_psak(self.team, label="run-key")

        response = self.client.post(
            f"/api/projects/{self.team.id}/endpoints/my_endpoint/run/",
            data={},
            content_type="application/json",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)

    def test_psak_can_run_endpoint_via_body_token(self):
        token, _ = _make_psak(self.team, label="body-key")

        response = self.client.post(
            f"/api/projects/{self.team.id}/endpoints/my_endpoint/run/",
            data={"secret_api_key": token},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)

    def test_psak_can_run_endpoint_with_hogql_access_control_on(self):
        # Without the FF mock, the test path skips the access-control branch
        # in posthog/hogql/database/database.py and never exercises the
        # synthetic-user code path.
        token, _ = _make_psak(self.team, label="run-with-rbac")

        with patch("posthoganalytics.feature_enabled", side_effect=_ff_returns_true_for_hogql_access_control):
            response = self.client.post(
                f"/api/projects/{self.team.id}/endpoints/my_endpoint/run/",
                data={},
                content_type="application/json",
                **self._auth_headers(token),
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)

    @parameterized.expand(
        [
            # PSAK must only authorize the `run` action — every other action returns 403.
            ("list", "GET", ""),
            ("retrieve", "GET", "my_endpoint/"),
            ("update", "PUT", "my_endpoint/"),
            ("partial_update", "PATCH", "my_endpoint/"),
            ("destroy", "DELETE", "my_endpoint/"),
            ("openapi_spec", "GET", "my_endpoint/openapi.json/"),
            ("materialization_status", "GET", "my_endpoint/materialization_status/"),
            ("materialization_preview", "POST", "my_endpoint/materialization_preview/"),
            ("versions", "GET", "my_endpoint/versions/"),
        ]
    )
    def test_psak_blocked_on_non_run_actions(self, _name, method, path_suffix):
        token, _ = _make_psak(self.team, label=f"non-run-{_name}")

        response = self.client.generic(
            method,
            f"/api/projects/{self.team.id}/endpoints/{path_suffix}",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.content)

    def test_psak_blocked_on_create(self):
        token, _ = _make_psak(self.team, label="create-key")

        response = self.client.post(
            f"/api/projects/{self.team.id}/endpoints/",
            data={"name": "new_endpoint", "query": SAMPLE_QUERY},
            content_type="application/json",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @parameterized.expand(
        [
            ("empty_list", []),
            ("null", None),
        ]
    )
    def test_psak_without_endpoint_scope_returns_403(self, _name, scopes):
        token, _ = _make_psak(self.team, label=f"no-scope-{_name}", scopes=scopes)

        response = self.client.post(
            f"/api/projects/{self.team.id}/endpoints/my_endpoint/run/",
            data={},
            content_type="application/json",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_unknown_psak_returns_401(self):
        # Valid-looking but not-in-DB token
        response = self.client.post(
            f"/api/projects/{self.team.id}/endpoints/my_endpoint/run/",
            data={},
            content_type="application/json",
            **self._auth_headers("phs_" + "z" * 35),
        )

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_psak_team_mismatch_returns_403(self):
        # PSAK belongs to team A; request targets team B.
        other_org = Organization.objects.create(name="Other Org")
        other_team = Team.objects.create(organization=other_org, name="Other Team")

        token, _ = _make_psak(self.team, label="team-mismatch-key")

        response = self.client.post(
            f"/api/projects/{other_team.id}/endpoints/my_endpoint/run/",
            data={},
            content_type="application/json",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_psak_does_not_authenticate_local_evaluation(self):
        # local_evaluation only accepts Team.secret_api_token via TeamSecretTokenAuthentication.
        token, _ = _make_psak(self.team, label="local-eval-key")

        response = self.client.get(
            f"/api/feature_flag/local_evaluation/",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_session_auth_still_works_on_endpoint_viewset(self):
        # Regression: wiring PSAK into authentication_classes must not break session auth.
        self.organization_membership.level = OrganizationMembership.Level.ADMIN
        self.organization_membership.save()
        self.client.force_login(self.user)

        response = self.client.get(f"/api/projects/{self.team.id}/endpoints/")
        self.assertEqual(response.status_code, status.HTTP_200_OK)


class TestProjectSecretAPIKeyLookup(APIBaseTest):
    def test_find_project_secret_api_key_match(self):
        from posthog.models.project_secret_api_key import find_project_secret_api_key

        token = "phs_" + "m" * 35
        psak = ProjectSecretAPIKey.objects.create(
            team=self.team,
            label="lookup",
            secure_value=hash_key_value(token),
            scopes=["endpoint:read"],
        )

        found = find_project_secret_api_key(token)
        assert found is not None
        self.assertEqual(found.pk, psak.pk)

    def test_find_project_secret_api_key_no_match(self):
        from posthog.models.project_secret_api_key import find_project_secret_api_key

        self.assertIsNone(find_project_secret_api_key("phs_" + "q" * 35))

    def test_find_project_secret_api_key_hash_isolation(self):
        from posthog.models.project_secret_api_key import find_project_secret_api_key

        token_a = "phs_" + "a" * 35
        token_b = "phs_" + "b" * 35
        psak_a = ProjectSecretAPIKey.objects.create(
            team=self.team,
            label="a",
            secure_value=hash_key_value(token_a),
        )

        found = find_project_secret_api_key(token_a)
        assert found is not None
        self.assertEqual(found.pk, psak_a.pk)

        self.assertIsNone(find_project_secret_api_key(token_b))
