from django.db import migrations

LEGACY_STRIPE_API_VERSION = "2024-09-30.acacia"


def set_default_stripe_api_version(apps, schema_editor):
    ExternalDataSource = apps.get_model("data_warehouse", "ExternalDataSource")

    for source in ExternalDataSource.objects.filter(source_type="Stripe"):
        job_inputs = source.job_inputs
        if not isinstance(job_inputs, dict):
            continue

        if "stripe_api_version" in job_inputs:
            continue

        job_inputs["stripe_api_version"] = LEGACY_STRIPE_API_VERSION
        source.job_inputs = job_inputs
        source.save(update_fields=["job_inputs"])


def reverse_set_default_stripe_api_version(apps, schema_editor):
    ExternalDataSource = apps.get_model("data_warehouse", "ExternalDataSource")

    for source in ExternalDataSource.objects.filter(source_type="Stripe"):
        job_inputs = source.job_inputs
        if not isinstance(job_inputs, dict):
            continue

        if job_inputs.get("stripe_api_version") != LEGACY_STRIPE_API_VERSION:
            continue

        job_inputs.pop("stripe_api_version", None)
        source.job_inputs = job_inputs
        source.save(update_fields=["job_inputs"])


class Migration(migrations.Migration):
    dependencies = [
        ("data_warehouse", "0045_alter_externaldatasource_source_type"),
    ]

    operations = [
        migrations.RunPython(set_default_stripe_api_version, reverse_set_default_stripe_api_version),
    ]
