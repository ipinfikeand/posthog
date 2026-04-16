from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("visual_review", "0005_tolerated_hashes"),
    ]

    operations = [
        migrations.AddField(
            model_name="toleratedhash",
            name="alternate_phash",
            field=models.CharField(blank=True, default="", max_length=16),
        ),
    ]
