use anyhow::Result;
use rand::Rng;
use reqwest::StatusCode;
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::*;

use feature_flags::api::types::FlagsResponse;
use feature_flags::config::DEFAULT_TEST_CONFIG;
use feature_flags::flags::flag_analytics::get_team_request_key;
use feature_flags::flags::flag_request::FlagRequestType;
use feature_flags::utils::test_utils::{
    insert_flags_for_team_in_redis, insert_new_team_in_redis, setup_redis_client, TestContext,
};
use limiters::redis::ServiceName;

pub mod common;

/// Helper to build a flags fixture with a single 100% rollout flag.
fn billable_flag_fixture(team_id: i32) -> serde_json::Value {
    json!([{
        "id": 1,
        "key": "billable-flag",
        "name": "Billable Flag",
        "active": true,
        "deleted": false,
        "team_id": team_id,
        "filters": {
            "groups": [{
                "properties": [],
                "rollout_percentage": 100
            }],
        },
    }])
}

/// `/internal/flags` evaluates flags successfully and does not increment the
/// per-team billing counter (the whole point of the route).
///
/// Paired assertion: the public `/flags` route *does* increment the same counter
/// using the same fixture. Without this companion check, a regression that
/// silently disabled all billing increments would still pass the
/// "internal-route-doesn't-bill" assertion below.
#[tokio::test]
async fn internal_route_skips_billing_counter() -> Result<()> {
    let config = DEFAULT_TEST_CONFIG.clone();
    let distinct_id = format!("internal_test_{}", rand::thread_rng().gen::<u32>());

    let client = setup_redis_client(Some(config.redis_url.clone())).await;
    let team = insert_new_team_in_redis(client.clone()).await.unwrap();
    let token = team.api_token.clone();

    let context = TestContext::new(None).await;
    context.insert_new_team(Some(team.id)).await.unwrap();
    context
        .insert_person(team.id, distinct_id.clone(), None)
        .await
        .unwrap();

    insert_flags_for_team_in_redis(
        client.clone(),
        team.id,
        Some(billable_flag_fixture(team.id).to_string()),
    )
    .await?;

    let billing_key = get_team_request_key(team.id, FlagRequestType::Decide);
    client.del(billing_key.clone()).await.unwrap();

    let server = ServerHandle::for_config(config).await;
    let payload = json!({"token": token, "distinct_id": distinct_id});

    let bucket = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            / 120
    };

    // Internal route: no counter increment.
    let res = server
        .send_internal_flags_request(payload.to_string(), None)
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let body: FlagsResponse = res.json().await?;
    assert!(body.flags.contains_key("billable-flag"));
    assert!(body.quota_limited.is_none());

    let counter = client
        .hget(billing_key.clone(), bucket().to_string())
        .await;
    assert!(
        counter.is_err(),
        "billing counter must not be incremented for internal requests"
    );

    // Companion: public route on the same team/fixture *does* increment.
    // Guards against a regression that disables billing increments wholesale.
    let res = server
        .send_flags_request(payload.to_string(), Some("2"), None)
        .await;
    assert_eq!(StatusCode::OK, res.status());

    let counter = client.hget(billing_key, bucket().to_string()).await;
    assert_eq!(
        counter.unwrap(),
        "1",
        "billing counter must be incremented for public requests"
    );

    Ok(())
}

/// When the team is over its flag-evaluation quota, the public `/flags` route
/// returns `quota_limited`. The internal route must keep evaluating — internal
/// callers should not be blocked by the customer's billing state.
#[tokio::test]
async fn internal_route_bypasses_quota_limiter() -> Result<()> {
    let config = DEFAULT_TEST_CONFIG.clone();
    let token = format!("internal_test_token_{}", rand::thread_rng().gen::<u64>());
    let team_id = 98765;

    let server = ServerHandle::for_config_with_mock_redis(
        config.clone(),
        vec![token.clone()], // marked as quota-limited
        vec![(token.clone(), team_id)],
    )
    .await;

    let payload = json!({"token": token, "distinct_id": "user1"});

    // Public /flags reflects the quota state.
    let res = server
        .send_flags_request(payload.to_string(), Some("2"), None)
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let public: FlagsResponse = res.json().await?;
    assert_eq!(
        public.quota_limited,
        Some(vec![ServiceName::FeatureFlags.as_string()]),
        "public route should reflect quota limit"
    );

    // Internal /internal/flags ignores the quota.
    let res = server
        .send_internal_flags_request(payload.to_string(), None)
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let internal: FlagsResponse = res.json().await?;
    assert!(
        internal.quota_limited.is_none(),
        "internal route must not surface quota_limited; got {:?}",
        internal.quota_limited
    );

    Ok(())
}

/// When `internal_flags_shared_secret` is unset (default), the route accepts
/// callers without a header. This is the local-dev path.
#[tokio::test]
async fn internal_route_accepts_caller_when_no_secret_configured() -> Result<()> {
    let config = DEFAULT_TEST_CONFIG.clone();
    assert!(
        config.internal_flags_shared_secret.is_empty(),
        "test config must default to unset secret"
    );

    let token = format!("test_token_{}", rand::thread_rng().gen::<u64>());
    let server =
        ServerHandle::for_config_with_mock_redis(config, vec![], vec![(token.clone(), 12345)])
            .await;

    let payload = json!({"token": token, "distinct_id": "user1"});
    let res = server
        .send_internal_flags_request(payload.to_string(), None)
        .await;
    assert_eq!(StatusCode::OK, res.status());

    Ok(())
}

/// When the shared secret is configured, requests without a matching header
/// must be rejected with 401 — even if the token itself is valid.
#[tokio::test]
async fn internal_route_rejects_invalid_secret_when_configured() -> Result<()> {
    let mut config = DEFAULT_TEST_CONFIG.clone();
    config.internal_flags_shared_secret = "correct-secret".to_string();

    let token = format!("test_token_{}", rand::thread_rng().gen::<u64>());
    let server =
        ServerHandle::for_config_with_mock_redis(config, vec![], vec![(token.clone(), 12345)])
            .await;

    let payload = json!({"token": token, "distinct_id": "user1"});

    // No header → rejected.
    let res = server
        .send_internal_flags_request(payload.to_string(), None)
        .await;
    assert_eq!(StatusCode::UNAUTHORIZED, res.status());

    // Wrong header → rejected.
    let res = server
        .send_internal_flags_request(payload.to_string(), Some("wrong-secret"))
        .await;
    assert_eq!(StatusCode::UNAUTHORIZED, res.status());

    // Correct header → allowed.
    let res = server
        .send_internal_flags_request(payload.to_string(), Some("correct-secret"))
        .await;
    assert_eq!(StatusCode::OK, res.status());

    Ok(())
}

/// `/internal/flags` is POST-only. Other methods get 405 Method Not Allowed.
#[tokio::test]
async fn internal_route_rejects_non_post_methods() -> Result<()> {
    let config = DEFAULT_TEST_CONFIG.clone();
    let server = ServerHandle::for_config(config).await;
    let client = reqwest::Client::new();

    let res = client
        .get(format!("http://{}/internal/flags", server.addr))
        .send()
        .await?;
    assert_eq!(StatusCode::METHOD_NOT_ALLOWED, res.status());

    Ok(())
}
