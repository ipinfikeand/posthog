from django.db import migrations


class Migration(migrations.Migration):
    """Drop posthog_eve_proj_id_22de03_idx — (coalesce(project_id, team_id), event), ~67 GB.

    Deferred follow-up to #57588 (Tier 1 drops). Gated on #57590 (REINDEX of
    posthog_event_property_unique_proj_event_property): the unique constraint
    (coalesce, event, property) covers this index as a strict leading prefix
    and is the planner's only logical fallback. Until it is reindexed from
    ~1187 GB bloated to ~400-600 GB natural, dropping this index would migrate
    any residual queries onto a much wider working set and push more pages out
    of shared_buffers.

    Safety proofs:
    - pganalyze run_index_selection: scansAffected = 0 for modeled workload.
    - Codebase audit: no read path queries (coalesce(project_id, team_id), event)
      without a property predicate; all reads either include property (covered
      by the unique constraint) or don't touch coalesce/event together.
    - prod-us EXPLAIN of property-definition listing falls cleanly to the
      unique constraint prefix scan, no regressions observed.

    Land order: merge AFTER #57590 has shipped and replica buffer-cache
    pressure has stabilized.
    """

    atomic = False

    dependencies = [
        ("event_definitions", "0005_eventdefinition_rename_promoted_to_primary"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.RemoveIndex(
                    model_name="eventproperty",
                    name="posthog_eve_proj_id_22de03_idx",
                ),
            ],
            database_operations=[
                migrations.RunSQL(
                    sql="DROP INDEX CONCURRENTLY IF EXISTS posthog_eve_proj_id_22de03_idx",
                    reverse_sql="""
                        CREATE INDEX CONCURRENTLY IF NOT EXISTS posthog_eve_proj_id_22de03_idx
                        ON posthog_eventproperty (COALESCE(project_id, (team_id)::bigint), event)
                    """,
                ),
            ],
        ),
    ]
