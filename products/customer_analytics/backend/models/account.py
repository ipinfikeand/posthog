from typing import Any

from django.apps import apps
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import JSONField, Q
from django.db.models.signals import pre_save
from django.dispatch import receiver

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from posthog.models import User
from posthog.models.utils import CreatedMetaFields, UpdatedMetaFields, UUIDModel


class AccountAssignment(BaseModel):
    id: int
    email: str


class AccountProperties(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_type_index: int | None = None
    group_keys: list[str] = []
    csm: AccountAssignment | None = None
    account_executive: AccountAssignment | None = None
    account_owner: AccountAssignment | None = None

    @model_validator(mode="after")
    def _groups_consistent(self) -> "AccountProperties":
        if self.group_keys and self.group_type_index is None:
            raise ValueError("group_type_index must be set when group_keys is non-empty")
        return self

    @field_validator("group_keys")
    @classmethod
    def _dedupe_group_keys(cls, group_keys) -> list[str]:
        return list(set(group_keys))


class AccountManager(models.Manager["Account"]):
    def filter(self, *args: Any, **kwargs: Any) -> models.QuerySet["Account"]:
        """
        Adds the `group_keys__contains_any=[...]` lookup: matches accounts whose stored
        `group_keys` JSON array shares at least one key with the provided list.

        Limitation: only fires on `Account.objects.filter(...)`. Chained calls
        (`.all().filter(...)`, `.exclude(...).filter(...)`) go through the QuerySet's
        `filter` and bypass this override.
        """
        if "group_keys__contains_any" in kwargs:
            keys = kwargs.pop("group_keys__contains_any")
            if not keys:
                return self.get_queryset().none()
            group_key_filter = Q()
            for group_key in keys:
                group_key_filter |= Q(_properties__contains={"group_keys": [group_key]})
            args = (*args, group_key_filter)
        return super().filter(*args, **kwargs)

    def create(
        self,
        *,
        properties: "dict | AccountProperties | None" = None,
        **kwargs: Any,
    ) -> "Account":
        if properties is not None:
            validated = (
                properties
                if isinstance(properties, AccountProperties)
                else AccountProperties.model_validate(properties)
            )
            kwargs["_properties"] = validated.model_dump(mode="json")
        return super().create(**kwargs)


class Account(UUIDModel, CreatedMetaFields, UpdatedMetaFields):
    team = models.ForeignKey("posthog.Team", on_delete=models.CASCADE)

    external_id = models.CharField(max_length=400, null=True, blank=True)
    name = models.CharField(max_length=400)
    _properties = JSONField(default=dict, db_column="properties")

    objects = AccountManager()

    @property
    def properties(self) -> AccountProperties:
        return AccountProperties.model_validate(self._properties or {})

    @properties.setter
    def properties(self, value: "dict | AccountProperties") -> None:
        validated = value if isinstance(value, AccountProperties) else AccountProperties.model_validate(value)
        self._properties = validated.model_dump(mode="json")

    def assign_csm(self, user: User, save: bool = True) -> "Account":
        return self._assign_role("csm", user, save=save)

    def assign_account_executive(self, user: User, save: bool = True) -> "Account":
        return self._assign_role("account_executive", user, save=save)

    def assign_account_owner(self, user: User, save: bool = True) -> "Account":
        return self._assign_role("account_owner", user, save=save)

    def _assign_role(self, role: str, user: User, save: bool = True) -> "Account":
        props = self.properties
        setattr(props, role, AccountAssignment(id=user.id, email=user.email))
        self.properties = props
        if save:
            self.save()
        return self

    def save(self, *args, **kwargs) -> None:
        self._validate_group_keys_unique_within_team()
        super().save(*args, **kwargs)

    def get_dupes(self, group_keys: list[str] | set[str]) -> models.QuerySet["Account"]:
        return type(self).objects.filter(team_id=self.team_id, group_keys__contains_any=group_keys).exclude(pk=self.pk)  # type: ignore[misc]

    def _validate_group_keys_unique_within_team(self) -> None:
        own_keys = set(self.properties.group_keys)
        if not own_keys:
            return

        dupes = self.get_dupes(group_keys=own_keys)
        if dupes.exists():
            dupe_keys: set[str] = set()
            for key_list in dupes.values_list("_properties__group_keys", flat=True):
                dupe_keys.update(key_list)
            shared = sorted(own_keys & dupe_keys)
            raise ValidationError(f"group_keys already attached to a different account: {shared}")


@receiver(pre_save, sender="customer_analytics.TeamCustomerAnalyticsConfig")
def _enforce_account_group_type_index_drift_policy(sender, instance, **kwargs) -> None:
    if instance.pk is None:
        return

    update_fields = kwargs.get("update_fields")
    if update_fields and "account_group_type_index" not in update_fields:
        return

    previous = sender.objects.filter(pk=instance.pk).values_list("account_group_type_index", flat=True).first()
    if previous == instance.account_group_type_index:
        return

    AccountModel = apps.get_model("customer_analytics", "Account")
    if AccountModel.objects.filter(team_id=instance.team_id).exists():
        raise ValidationError("Cannot change account_group_type_index once accounts exist for this team")
