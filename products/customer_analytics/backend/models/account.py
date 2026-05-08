from django.apps import apps
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import JSONField
from django.db.models.signals import pre_save
from django.dispatch import receiver

from pydantic import BaseModel, ConfigDict, model_validator

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


class AccountManager(models.Manager):
    def get_sibling_accounts(
        self, *, team_id: int, group_key: str, exclude_pk: "str | None" = None
    ) -> models.QuerySet["Account"]:
        qs = self.filter(
            team_id=team_id,
            _properties__contains={"group_keys": [group_key]},
        )
        if exclude_pk is not None:
            qs = qs.exclude(pk=exclude_pk)
        return qs


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

    def _validate_group_keys_unique_within_team(self) -> None:
        group_keys = self.properties.group_keys
        if not group_keys:
            return

        if len(set(group_keys)) != len(group_keys):
            raise ValidationError("group_keys must not contain duplicates")

        conflicts = [
            key
            for key in group_keys
            if type(self).objects.get_sibling_accounts(team_id=self.team_id, group_key=key, exclude_pk=self.pk).exists()
        ]
        if conflicts:
            raise ValidationError(f"group_keys already attached to a different account: {sorted(conflicts)}")


@receiver(pre_save, sender="customer_analytics.TeamCustomerAnalyticsConfig")
def _enforce_account_group_type_index_drift_policy(sender, instance, **kwargs) -> None:
    if instance.pk is None:
        return

    previous = sender.objects.filter(pk=instance.pk).values_list("account_group_type_index", flat=True).first()
    if previous == instance.account_group_type_index:
        return

    AccountModel = apps.get_model("customer_analytics", "Account")
    if AccountModel.objects.filter(team_id=instance.team_id).exists():
        raise ValidationError("Cannot change account_group_type_index once accounts exist for this team")
