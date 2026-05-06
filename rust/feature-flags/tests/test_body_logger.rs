//! End-to-end tests for the per-team /flags body-logger config refresh path.
//!
//! Verifies that a row written to `posthog_instancesetting` under
//! `constance:posthog:FLAGS_LOG_BODIES_TEAMS` propagates into the Rust
//! service's in-memory config when `BodyLogger::refresh_if_stale` runs,
//! mirroring the established `RATE_LIMITING_ALLOW_LIST_TEAMS` pattern.
//!
//! All scenarios share one combined test so they don't race on the same
//! `posthog_instancesetting` row when cargo runs integration tests in
//! parallel — same approach as `test_db_rate_limit_allowlist`.

use feature_flags::config::{BodyLogTeams, Config};
use feature_flags::handler::body_logger::BodyLogger;
use feature_flags::utils::test_utils::TestContext;
use sqlx::postgres::PgPoolOptions;

const REFRESH_TTL_SECS: u64 = 60;

fn pg_pool_from_config(config: &Config) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect_lazy(&config.read_database_url)
        .expect("failed to create test PgPool")
}

#[tokio::test]
async fn test_db_body_logger_refresh() {
    let context = TestContext::new(None).await;
    context
        .delete_instance_setting("FLAGS_LOG_BODIES_TEAMS")
        .await
        .unwrap();

    let pool = pg_pool_from_config(&Config::default_test_config());

    // --- Scenario 1: missing row preserves env-var default ---
    {
        let mut initial = std::collections::HashMap::new();
        initial.insert(42, vec!["env-default".into()]);
        let logger = BodyLogger::new(BodyLogTeams(initial), 65_536);

        logger.refresh_if_stale(&pool, REFRESH_TTL_SECS).await;

        let patterns = logger
            .for_team(42)
            .expect("missing DB row should preserve env-var default");
        assert!(patterns.matches("env-default"));
        assert!(!patterns.matches("other-flag"));
    }

    // --- Scenario 2: row with empty patterns yields log-all ---
    {
        let logger = BodyLogger::new(BodyLogTeams::default(), 65_536);
        assert!(logger.for_team(123).is_none(), "scenario 2: starts empty");

        context
            .set_instance_setting("FLAGS_LOG_BODIES_TEAMS", r#"{"123": []}"#)
            .await
            .unwrap();

        logger.refresh_if_stale(&pool, REFRESH_TTL_SECS).await;

        let patterns = logger
            .for_team(123)
            .expect("scenario 2: team 123 should be enabled after refresh");
        assert!(patterns.matches("anything"), "scenario 2: log-all matches");
        assert!(
            logger.for_team(456).is_none(),
            "scenario 2: team 456 stays unlisted"
        );
    }

    // --- Scenario 3: non-empty patterns filter by flag key ---
    {
        context
            .set_instance_setting(
                "FLAGS_LOG_BODIES_TEAMS",
                r#"{"789": ["my-feature", "checkout-*"]}"#,
            )
            .await
            .unwrap();

        let logger = BodyLogger::new(BodyLogTeams::default(), 65_536);
        logger.refresh_if_stale(&pool, REFRESH_TTL_SECS).await;

        let patterns = logger
            .for_team(789)
            .expect("scenario 3: team 789 should be enabled after refresh");
        assert!(patterns.matches("my-feature"));
        assert!(patterns.matches("checkout-foo"));
        assert!(!patterns.matches("other-flag"));
    }

    // --- Scenario 4: Django string-encoded value also parses ---
    {
        context
            .set_instance_setting("FLAGS_LOG_BODIES_TEAMS", r#""{\"555\": []}""#)
            .await
            .unwrap();

        let logger = BodyLogger::new(BodyLogTeams::default(), 65_536);
        logger.refresh_if_stale(&pool, REFRESH_TTL_SECS).await;

        let patterns = logger
            .for_team(555)
            .expect("scenario 4: team 555 should be enabled after refresh");
        assert!(patterns.matches("anything"), "scenario 4: log-all matches");
    }

    context
        .delete_instance_setting("FLAGS_LOG_BODIES_TEAMS")
        .await
        .unwrap();
}
