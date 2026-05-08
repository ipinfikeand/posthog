import pytest
from posthog.test.base import BaseTest

from django.core.exceptions import ValidationError as DjangoValidationError

from parameterized import parameterized
from pydantic import ValidationError as PydanticValidationError

from posthog.models import Team, User

from products.customer_analytics.backend.models import Account, TeamCustomerAnalyticsConfig
from products.customer_analytics.backend.models.account import AccountAssignment, AccountProperties


class AccountAssignRoleTest(BaseTest):
    def setUp(self):
        self.user = User.objects.create_user(
            email="test@example.com", password=None, first_name="Member", is_email_verified=True
        )

    @parameterized.expand([("csm",), ("account_executive",), ("account_owner",)])
    def test_assign_role_persists_typed_assignment(self, role):
        account = Account.objects.create(team=self.team, name=f"Account with {role}")

        getattr(account, f"assign_{role}")(self.user)

        account.refresh_from_db()
        expected = AccountAssignment(id=self.user.id, email=self.user.email)
        assert getattr(account.properties, role) == expected


class AccountPropertiesValidationTest(BaseTest):
    def setUp(self):
        self.user = User.objects.create_user(
            email="apv@example.com", password=None, first_name="APV", is_email_verified=True
        )

    def test_rejects_group_keys_without_type(self):
        with pytest.raises(PydanticValidationError):
            AccountProperties.model_validate({"group_keys": ["acme"], "group_type_index": None})

    def test_rejects_unknown_keys(self):
        with pytest.raises(PydanticValidationError):
            AccountProperties.model_validate({"unknown_field": "x"})

    def test_typed_property_round_trip_through_setter(self):
        account = Account.objects.create(team=self.team, name="Round-trip")
        account.properties = AccountProperties(
            group_type_index=0,
            group_keys=["acme"],
            csm=AccountAssignment(id=self.user.id, email=self.user.email),
        )
        account.save()
        account.refresh_from_db()

        props = account.properties
        assert isinstance(props, AccountProperties)
        assert props.group_type_index == 0
        assert props.group_keys == ["acme"]
        assert props.csm == AccountAssignment(id=self.user.id, email=self.user.email)
        assert props.account_executive is None
        assert props.account_owner is None

    def test_setter_validates_dict_input(self):
        account = Account.objects.create(team=self.team, name="Bad input")

        with pytest.raises(PydanticValidationError):
            account.properties = {"unknown_field": "x"}


class AccountSaveUniquenessTest(BaseTest):
    def test_save_rejects_creating_second_account_with_same_group_key(self):
        Account.objects.create(
            team=self.team,
            name="First",
            properties={"group_type_index": 0, "group_keys": ["acme"]},
        )

        with pytest.raises(DjangoValidationError):
            Account.objects.create(
                team=self.team,
                name="Second",
                properties={"group_type_index": 0, "group_keys": ["acme"]},
            )

    def test_save_rejects_moving_group_key_to_another_account(self):
        first = Account.objects.create(
            team=self.team,
            name="First",
            properties={"group_type_index": 0, "group_keys": ["acme"]},
        )
        second = Account.objects.create(
            team=self.team,
            name="Second",
            properties={"group_type_index": 0, "group_keys": ["beta"]},
        )

        second.properties = {"group_type_index": 0, "group_keys": ["acme"]}
        with pytest.raises(DjangoValidationError):
            second.save()

        first.refresh_from_db()
        second.refresh_from_db()
        assert first.properties.group_keys == ["acme"]
        assert second.properties.group_keys == ["beta"]

    def test_save_rejects_duplicate_keys_within_same_account(self):
        account = Account(
            team=self.team,
            name="Dup",
        )
        account.properties = {"group_type_index": 0, "group_keys": ["acme", "acme"]}

        with pytest.raises(DjangoValidationError):
            account.save()

    def test_save_allows_idempotent_resave(self):
        account = Account.objects.create(
            team=self.team,
            name="Account",
            properties={"group_type_index": 0, "group_keys": ["acme"]},
        )

        account.name = "Renamed"
        account.save()
        account.refresh_from_db()
        assert account.name == "Renamed"
        assert account.properties.group_keys == ["acme"]

    def test_save_allows_same_group_key_across_teams(self):
        other_team = Team.objects.create(organization=self.organization)
        Account.objects.create(
            team=other_team,
            name="Other team",
            properties={"group_type_index": 0, "group_keys": ["acme"]},
        )

        account = Account.objects.create(
            team=self.team,
            name="Same key, different team",
            properties={"group_type_index": 0, "group_keys": ["acme"]},
        )

        assert account.properties.group_keys == ["acme"]


class TeamCustomerAnalyticsConfigDriftPolicyTest(BaseTest):
    def setUp(self):
        self.config = TeamCustomerAnalyticsConfig.objects.get(team=self.team)
        self.config.account_group_type_index = 0
        self.config.save()

    def test_drift_blocked_when_accounts_exist(self):
        Account.objects.create(team=self.team, name="Existing")

        self.config.account_group_type_index = 1
        with pytest.raises(DjangoValidationError):
            self.config.save()

    def test_drift_allowed_when_no_accounts_exist(self):
        self.config.account_group_type_index = 1
        self.config.save()

        self.config.refresh_from_db()
        assert self.config.account_group_type_index == 1
