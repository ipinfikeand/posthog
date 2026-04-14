from posthog.test.base import APIBaseTest

from rest_framework import status

from posthog.models import OrganizationMembership, PropertyDefinition

from products.access_control.backend.models.property_access_control import PropertyAccessControl
from products.access_control.backend.property_access_control import PropertyAccessLevel


class TestPropertyAccessControlViewSet(APIBaseTest):
    def setUp(self):
        super().setUp()
        # Write operations require project admin privileges
        self.organization_membership.level = OrganizationMembership.Level.ADMIN
        self.organization_membership.save()

        self.prop_def = PropertyDefinition.objects.create(
            team=self.team,
            name="secret_field",
            property_type="String",
            type=PropertyDefinition.Type.EVENT,
        )
        self.url = f"/api/projects/{self.team.pk}/property_access_controls/"
        self.list_url = f"{self.url}?property_definition_id={self.prop_def.id}"

    def _post(self, data: dict):
        payload = {"property_definition_id": str(self.prop_def.id), **data}
        return self.client.post(self.url, payload, format="json")

    def test_list_empty(self):
        response = self.client.get(self.list_url)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["access_controls"] == []
        assert data["default_access_level"] == PropertyAccessLevel.READ_WRITE.value
        assert set(data["available_access_levels"]) == {e.value for e in PropertyAccessLevel}

    def test_list_missing_property_definition_id_returns_400(self):
        response = self.client.get(self.url)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_create_default_rule(self):
        response = self._post({"access_level": PropertyAccessLevel.NONE.value})
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["access_level"] == PropertyAccessLevel.NONE.value

        # verify it shows up in list
        list_response = self.client.get(self.list_url)
        assert list_response.json()["default_access_level"] == PropertyAccessLevel.NONE.value
        assert len(list_response.json()["access_controls"]) == 1

    def test_create_member_override(self):
        response = self._post(
            {
                "access_level": PropertyAccessLevel.READ_WRITE.value,
                "organization_member": str(self.organization_membership.id),
            }
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["access_level"] == PropertyAccessLevel.READ_WRITE.value
        # PrimaryKeyRelatedField serializes the FK as the PK value
        assert str(response.json()["organization_member"]) == str(self.organization_membership.id)

    def test_create_role_override(self):
        from ee.models.rbac.role import Role

        role = Role.objects.create(name="Analyst", organization=self.organization)
        response = self._post(
            {
                "access_level": PropertyAccessLevel.READ.value,
                "role": str(role.id),
            }
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["access_level"] == PropertyAccessLevel.READ.value
        assert str(response.json()["role"]) == str(role.id)

    def test_update_existing_rule(self):
        # create a rule
        self._post({"access_level": PropertyAccessLevel.NONE.value})
        # update it
        response = self._post({"access_level": PropertyAccessLevel.READ_WRITE.value})
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["access_level"] == PropertyAccessLevel.READ_WRITE.value

        # only one rule should exist
        assert PropertyAccessControl.objects.filter(property_definition=self.prop_def).count() == 1

    def test_delete_override_with_null_access_level(self):
        # create a rule first
        self._post({"access_level": PropertyAccessLevel.NONE.value})
        assert PropertyAccessControl.objects.filter(property_definition=self.prop_def).count() == 1

        # delete it by sending null
        response = self._post({"access_level": None})
        assert response.status_code == status.HTTP_204_NO_CONTENT
        assert PropertyAccessControl.objects.filter(property_definition=self.prop_def).count() == 0

    def test_list_with_multiple_rules(self):
        from ee.models.rbac.role import Role

        role = Role.objects.create(name="Analyst", organization=self.organization)

        # default rule
        self._post({"access_level": PropertyAccessLevel.NONE.value})
        # member override
        self._post(
            {
                "access_level": PropertyAccessLevel.READ_WRITE.value,
                "organization_member": str(self.organization_membership.id),
            }
        )
        # role override
        self._post(
            {"access_level": PropertyAccessLevel.READ.value, "role": str(role.id)},
        )

        response = self.client.get(self.list_url)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data["access_controls"]) == 3
        assert data["default_access_level"] == PropertyAccessLevel.NONE.value

    def test_non_admin_can_read_but_not_write(self):
        # Downgrade to regular member
        self.organization_membership.level = OrganizationMembership.Level.MEMBER
        self.organization_membership.save()

        # GET should work (read access)
        response = self.client.get(self.list_url)
        assert response.status_code == status.HTTP_200_OK

        # POST should be forbidden (write access requires admin)
        response = self._post({"access_level": PropertyAccessLevel.NONE.value})
        assert response.status_code == status.HTTP_403_FORBIDDEN
