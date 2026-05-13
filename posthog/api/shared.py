"""
This module contains serializers that are used across other serializers for nested representations.
"""

import copy
import json
from typing import Any, Optional

from drf_spectacular.utils import extend_schema_field
from opentelemetry import trace
from rest_framework import serializers
from rest_framework.fields import SkipField
from rest_framework.relations import PKOnlyObject
from rest_framework.utils import model_meta

from posthog.models import Organization, Team, User
from posthog.models.organization import OrganizationMembership
from posthog.models.project import Project

tracer = trace.get_tracer(__name__)

TEST_ACCOUNT_FILTERS_HELP_TEXT = (
    "Property filters that identify internal/test traffic to be excluded from insights. "
    'Each entry is a filter object like {"key": "email", "value": "@your-company.com", '
    '"operator": "not_icontains", "type": "person"}; reference a cohort with '
    '{"key": "id", "value": <cohortId>, "operator": "not_in", "type": "cohort"}. '
    "Pass an empty array to clear all filters."
)


@extend_schema_field(
    {
        "type": "array",
        "description": TEST_ACCOUNT_FILTERS_HELP_TEXT,
        "items": {
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Property name to filter on."},
                "type": {
                    "type": "string",
                    "description": "Filter kind, e.g. `person`, `event`, `event_feature`, `element`, or `cohort`.",
                },
                "operator": {
                    "type": "string",
                    "description": "Comparison operator, e.g. `not_icontains`, `not_in`, or `is_set`.",
                },
                "value": {
                    "description": "Value to compare against — a string, number, array, or null depending on the operator.",
                },
            },
            "required": ["key"],
        },
    }
)
class TestAccountFiltersField(serializers.JSONField):
    """Typed `JSONField` for `Team.test_account_filters`.

    The column is free-form JSON, but every consumer (settings UI, HogQL filters, MCP tools) expects
    a list of property filters. Declaring it with this field gives the generated API/MCP schemas an
    explicit array shape instead of an opaque `unknown`, so clients stop sending a JSON-encoded string.
    """

    def to_internal_value(self, data):
        # Be forgiving with clients that pass a JSON-encoded string instead of the array — an
        # MCP/AI agent's most common slip-up is JSON.stringify-ing a value it just read. If the
        # string parses to a list, treat it as the array the caller intended; otherwise let
        # `validate_test_account_filters` reject it with a clear error.
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
            except (TypeError, ValueError):
                parsed = None
            if isinstance(parsed, list):
                data = parsed
        return super().to_internal_value(data)


class UserBasicSerializer(serializers.ModelSerializer):
    hedgehog_config = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            "id",
            "uuid",
            "distinct_id",
            "first_name",
            "last_name",
            "email",
            "is_email_verified",
            "hedgehog_config",
            "role_at_organization",
        ]

    def get_hedgehog_config(self, user: User) -> Optional[dict]:
        if user.hedgehog_config:
            if user.hedgehog_config.get("version") == 2:
                actor_options = user.hedgehog_config.get("actor_options", {})
                return {
                    "use_as_profile": user.hedgehog_config.get("use_as_profile"),
                    "color": actor_options.get("color"),
                    "accessories": actor_options.get("accessories"),
                    "skin": actor_options.get("skin"),
                }
            else:
                return {
                    "use_as_profile": user.hedgehog_config.get("use_as_profile"),
                    "color": user.hedgehog_config.get("color"),
                    "accessories": user.hedgehog_config.get("accessories"),
                    "skin": user.hedgehog_config.get("skin"),
                }
        return None


class ProjectBasicSerializer(serializers.ModelSerializer):
    """
    Serializer for `Project` model with minimal attributes to speeed up loading and transfer times.
    Also used for nested serializers.
    """

    class Meta:
        model = Project
        fields = (
            "id",
            "organization_id",
            "name",
        )
        read_only_fields = fields


class ProjectBackwardCompatBasicSerializer(serializers.ModelSerializer):
    """
    Like `ProjectBasicSerializer`, but also works as a drop-in replacement for `TeamBasicSerializer` by way of
    passthrough fields. This allows the meaning of `Team` to change from "project" to "environment" without breaking
    backward compatibility of the REST API.
    Do not use this in greenfield endpoints!
    """

    instance: Optional[Project]

    class Meta:
        model = Project
        fields = (
            "id",
            "uuid",  # Compat with TeamSerializer
            "organization",
            "api_token",  # Compat with TeamSerializer
            "name",
            "completed_snippet_onboarding",  # Compat with TeamSerializer
            "has_completed_onboarding_for",  # Compat with TeamSerializer
            "ingested_event",  # Compat with TeamSerializer
            "is_demo",  # Compat with TeamSerializer
            "timezone",  # Compat with TeamSerializer
            "access_control",  # Compat with TeamSerializer
        )
        read_only_fields = fields
        team_passthrough_fields = {
            "uuid",
            "api_token",
            "completed_snippet_onboarding",
            "has_completed_onboarding_for",
            "ingested_event",
            "is_demo",
            "timezone",
            "access_control",
        }

    def get_fields(self):
        declared_fields = copy.deepcopy(self._declared_fields)

        info = model_meta.get_field_info(Project)
        team_info = model_meta.get_field_info(Team)
        for field_name, field in team_info.fields.items():
            if field_name in info.fields:
                continue
            info.fields[field_name] = field
            info.fields_and_pk[field_name] = field
        for field_name, relation in team_info.forward_relations.items():
            if field_name in info.forward_relations:
                continue
            info.forward_relations[field_name] = relation
            info.relations[field_name] = relation
        for accessor_name, relation in team_info.reverse_relations.items():
            if accessor_name in info.reverse_relations:
                continue
            info.reverse_relations[accessor_name] = relation
            info.relations[accessor_name] = relation

        field_names = self.get_field_names(declared_fields, info)

        extra_kwargs = self.get_extra_kwargs()
        extra_kwargs, hidden_fields = self.get_uniqueness_extra_kwargs(field_names, declared_fields, extra_kwargs)

        fields = {}
        for field_name in field_names:
            if field_name in declared_fields:
                fields[field_name] = declared_fields[field_name]
                continue
            extra_field_kwargs = extra_kwargs.get(field_name, {})
            source = extra_field_kwargs.get("source", "*")
            if source == "*":
                source = field_name
            field_class, field_kwargs = self.build_field(source, info, model_class=Project, nested_depth=0)
            field_kwargs = self.include_extra_kwargs(field_kwargs, extra_field_kwargs)
            fields[field_name] = field_class(**field_kwargs)
        fields.update(hidden_fields)
        return fields

    def build_field(self, field_name, info, model_class, nested_depth):
        if field_name in self.Meta.team_passthrough_fields:
            model_class = Team
        return super().build_field(field_name, info, model_class, nested_depth)

    def to_representation(self, instance):
        """
        Object instance -> Dict of primitive datatypes. Basically copied from Serializer.to_representation
        """
        ret: dict[str, Any] = {}
        fields = self._readable_fields

        for field in fields:
            assert field.field_name is not None
            try:
                attribute_source = instance
                if field.field_name in self.Meta.team_passthrough_fields:
                    # This branch is the only material change from the original method
                    attribute_source = instance.passthrough_team
                attribute = field.get_attribute(attribute_source)
            except SkipField:
                continue

            check_for_none = attribute.pk if isinstance(attribute, PKOnlyObject) else attribute
            if check_for_none is None:
                ret[field.field_name] = None
            else:
                ret[field.field_name] = field.to_representation(attribute)

        return ret


class TeamBasicSerializer(serializers.ModelSerializer):
    """
    Serializer for `Team` model with minimal attributes to speeed up loading and transfer times.
    Also used for nested serializers.
    """

    class Meta:
        model = Team
        fields = (
            "id",
            "uuid",
            "organization",
            "project_id",
            "api_token",
            "name",
            "completed_snippet_onboarding",
            "has_completed_onboarding_for",
            "ingested_event",
            "is_demo",
            "timezone",
            "access_control",
        )
        read_only_fields = fields

    @tracer.start_as_current_span("team_basic_serializer.to_representation")
    def to_representation(self, instance):
        return super().to_representation(instance)


class TeamPublicSerializer(serializers.ModelSerializer):
    """
    Serializer for `Team` model with attributes suitable for completely public sharing (primarily shared dashboards).
    """

    class Meta:
        model = Team
        fields = ("id", "project_id", "uuid", "name", "timezone", "default_data_theme")
        read_only_fields = fields


class OrganizationBasicSerializer(serializers.ModelSerializer):
    """
    Serializer for `Organization` model with minimal attributes to speeed up loading and transfer times.
    Also used for nested serializers.
    """

    membership_level = serializers.SerializerMethodField()

    class Meta:
        model = Organization
        fields = [
            "id",
            "name",
            "slug",
            "logo_media_id",
            "membership_level",
            "members_can_use_personal_api_keys",
            "is_active",
            "is_not_active_reason",
            "is_pending_deletion",
        ]

    def get_membership_level(self, organization: Organization) -> Optional[OrganizationMembership.Level]:
        membership = OrganizationMembership.objects.filter(
            organization=organization, user=self.context["request"].user
        ).first()
        return OrganizationMembership.Level(membership.level) if membership is not None else None

    @tracer.start_as_current_span("organization_basic_serializer.to_representation")
    def to_representation(self, instance):
        return super().to_representation(instance)


class FilterBaseSerializer(serializers.Serializer):
    type = serializers.ChoiceField(choices=["events", "actions"])
    id = serializers.CharField(required=False)
    name = serializers.CharField(required=False, allow_null=True)
    order = serializers.IntegerField(required=False)
    properties = serializers.ListField(child=serializers.DictField(), default=[])


class FiltersSerializer(serializers.Serializer):
    events = FilterBaseSerializer(many=True, required=False)
    actions = FilterBaseSerializer(many=True, required=False)
    filter_test_accounts = serializers.BooleanField(required=False)
