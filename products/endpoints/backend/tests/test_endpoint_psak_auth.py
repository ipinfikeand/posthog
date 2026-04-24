from posthog.test.base import APIBaseTest

from rest_framework import status

from posthog.models.organization import Organization, OrganizationMembership
from posthog.models.project_secret_api_key import ProjectSecretAPIKey
from posthog.models.team import Team
from posthog.models.utils import hash_key_value

from products.endpoints.backend.tests.conftest import create_endpoint_with_version

SAMPLE_QUERY = {"kind": "HogQLQuery", "query": "SELECT 1"}


def _make_psak(team, label="psak", scopes=None):
    # Token must match _SECRET_API_KEY_RE = r"^phs_[a-zA-Z0-9]+$", so only alphanumerics after phs_.
    suffix = "".join(c for c in label if c.isalnum())
    token = "phs_" + ("a" * 35) + suffix
    psak = ProjectSecretAPIKey.objects.create(
        team=team,
        label=label,
        mask_value=f"phs_...{suffix[:4]}",
        secure_value=hash_key_value(token),
        scopes=scopes if scopes is not None else ["endpoint:read"],
    )
    return token, psak


class TestEndpointViewSetPSAKAuth(APIBaseTest):
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

    def test_psak_endpoint_read_list_succeeds(self):
        token, _ = _make_psak(self.team, label="list-key")

        response = self.client.get(
            f"/api/projects/{self.team.id}/endpoints/",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)
        assert any(e["name"] == "my_endpoint" for e in response.json()["results"])

    def test_psak_endpoint_read_retrieve_succeeds(self):
        token, _ = _make_psak(self.team, label="retrieve-key")

        response = self.client.get(
            f"/api/projects/{self.team.id}/endpoints/my_endpoint/",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)

    def test_psak_without_scope_returns_403(self):
        token, _ = _make_psak(self.team, label="no-scope-key", scopes=[])

        response = self.client.get(
            f"/api/projects/{self.team.id}/endpoints/",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_psak_with_null_scopes_returns_403(self):
        token, _ = _make_psak(self.team, label="null-scope-key", scopes=None)

        response = self.client.get(
            f"/api/projects/{self.team.id}/endpoints/",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_psak_read_cannot_write(self):
        token, _ = _make_psak(self.team, label="write-attempt-key")

        response = self.client.delete(
            f"/api/projects/{self.team.id}/endpoints/my_endpoint/",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_unknown_psak_returns_401(self):
        # Valid-looking but not-in-DB token
        response = self.client.get(
            f"/api/projects/{self.team.id}/endpoints/",
            **self._auth_headers("phs_" + "z" * 35),
        )

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_psak_team_mismatch_returns_403(self):
        # PSAK belongs to team A; request targets team B.
        other_org = Organization.objects.create(name="Other Org")
        other_team = Team.objects.create(organization=other_org, name="Other Team")

        token, _ = _make_psak(self.team, label="team-mismatch-key")

        response = self.client.get(
            f"/api/projects/{other_team.id}/endpoints/",
            **self._auth_headers(token),
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_psak_does_not_authenticate_local_evaluation(self):
        # Sending a PSAK to the legacy local_evaluation endpoint must not authenticate:
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
