"""
DRF views for access_control.

Responsibilities:
- Validate incoming JSON (via serializers)
- Convert JSON to DTOs
- Call facade methods
- Convert DTOs to JSON responses

No business logic or ORM access here — that belongs in the facade / logic layer.
"""

from typing import Any

from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework import status
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from posthog.api.routing import TeamAndOrgViewSetMixin
from posthog.permissions import TeamMemberStrictManagementPermission

from ..facade import api
from ..facade.contracts import DeletePropertyAccessControlInput, PropertyAccessLevel, UpsertPropertyAccessControlInput
from .serializers import (
    PropertyAccessControlRuleSerializer,
    PropertyAccessControlStateSerializer,
    PropertyAccessControlUpdateSerializer,
)


class PropertyAccessControlViewSet(TeamAndOrgViewSetMixin, GenericViewSet):
    """
    Manages property-level access control rules for property definitions.

    Mounted at `/api/projects/{project_id}/property_access_controls/`. The target
    property definition is provided via the `property_definition_id` query parameter
    on GET requests and in the request body on POST requests.
    """

    scope_object = "property_definition"
    serializer_class = PropertyAccessControlRuleSerializer
    permission_classes = [TeamMemberStrictManagementPermission]

    @extend_schema(
        parameters=[
            OpenApiParameter(
                name="property_definition_id",
                description="The property definition ID to fetch access control rules for.",
                required=True,
                type=str,
            ),
        ],
        responses={200: PropertyAccessControlStateSerializer},
        description="Get all property access control rules for a property definition.",
    )
    def list(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        property_definition_id = request.query_params.get("property_definition_id")
        if not property_definition_id:
            raise ValidationError({"property_definition_id": "This query parameter is required."})

        try:
            state = api.get_property_access_state(
                team_id=self.team_id,
                property_definition_id=property_definition_id,
            )
        except api.PropertyDefinitionNotFoundError:
            raise NotFound("Property definition not found.")

        return Response(PropertyAccessControlStateSerializer(state).data)

    @extend_schema(
        request=PropertyAccessControlUpdateSerializer,
        responses={200: PropertyAccessControlRuleSerializer},
        description="Create or update a property access control rule. Send access_level=null to delete an override.",
    )
    def create(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        serializer = PropertyAccessControlUpdateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        property_definition_id = data["property_definition_id"]
        raw_access_level = data["access_level"]
        org_member_id = data.get("organization_member")
        role_id = data.get("role")

        if raw_access_level is None:
            # Delete the override
            try:
                api.delete_property_access_control(
                    team_id=self.team_id,
                    input=DeletePropertyAccessControlInput(
                        property_definition_id=property_definition_id,
                        organization_member_id=org_member_id,
                        role_id=role_id,
                    ),
                )
            except api.PropertyDefinitionNotFoundError:
                raise NotFound("Property definition not found.")
            except api.PropertyAccessControlRuleNotFoundError:
                return Response(status=status.HTTP_404_NOT_FOUND)
            return Response(status=status.HTTP_204_NO_CONTENT)

        created_by_id: int | None = request.user.pk if request.user.is_authenticated else None
        try:
            rule = api.upsert_property_access_control(
                team_id=self.team_id,
                created_by_id=created_by_id,
                input=UpsertPropertyAccessControlInput(
                    property_definition_id=property_definition_id,
                    access_level=PropertyAccessLevel(raw_access_level),
                    organization_member_id=org_member_id,
                    role_id=role_id,
                ),
            )
        except api.PropertyDefinitionNotFoundError:
            raise NotFound("Property definition not found.")

        return Response(
            PropertyAccessControlRuleSerializer(rule).data,
            status=status.HTTP_200_OK,
        )
