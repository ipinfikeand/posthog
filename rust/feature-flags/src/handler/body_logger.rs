//! Per-team request/response body logging for the `/flags` endpoint.
//!
//! Configured via the `FLAGS_LOG_BODIES_TEAMS` env var at startup and refreshed
//! at runtime from `posthog_instancesetting`
//! (key: `constance:posthog:FLAGS_LOG_BODIES_TEAMS`) every ~60s. Mirrors the
//! propagation pattern used by `RATE_LIMITING_ALLOW_LIST_TEAMS` in
//! `api/flag_definitions.rs`.
//!
//! Stored per-team config maps a team ID to a list of compiled wildcard
//! patterns:
//! - empty list = log every flag in the response
//! - non-empty list = filter the response's `flags` map to keys matching any pattern
//!
//! Patterns support `*` wildcards (e.g., `my-feature`, `checkout-*`,
//! `*-targeting-*`); exact keys (no `*`) match by string equality.

use crate::api::instance_setting::{constance_key, fetch_instance_setting_raw_value};
use crate::api::types::{FlagDetails, FlagsResponse};
use crate::config::BodyLogTeams;
use common_types::TeamId;
use once_cell::sync::Lazy;
use serde::Serialize;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;
use uuid::Uuid;

/// `tracing` event-name field, also used as the message text. Loki joins this
/// to the canonical log line on `request_id`.
const BODY_LOG_EVENT: &str = "flags_body_log";

static CONSTANCE_KEY: Lazy<String> = Lazy::new(|| constance_key("FLAGS_LOG_BODIES_TEAMS"));

/// Compiled glob-style pattern: literal segments separated by `*`.
///
/// `"checkout-*"` → `["checkout-"]` with `prefix_anchored = true`,
/// `suffix_anchored = false`. `"my-feature"` (no `*`) → exact match.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CompiledPattern {
    segments: Vec<String>,
    prefix_anchored: bool,
    suffix_anchored: bool,
}

impl CompiledPattern {
    fn compile(pattern: &str) -> Self {
        let prefix_anchored = !pattern.starts_with('*');
        let suffix_anchored = !pattern.ends_with('*');
        let segments: Vec<String> = pattern
            .split('*')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        Self {
            segments,
            prefix_anchored,
            suffix_anchored,
        }
    }

    fn matches(&self, key: &str) -> bool {
        if self.segments.is_empty() {
            return true;
        }
        if self.segments.len() == 1 && self.prefix_anchored && self.suffix_anchored {
            return key == self.segments[0];
        }

        let segs = &self.segments;
        let mut remaining = key;

        if self.prefix_anchored {
            match remaining.strip_prefix(segs[0].as_str()) {
                Some(rest) => remaining = rest,
                None => return false,
            }
        }

        let middle_start = if self.prefix_anchored { 1 } else { 0 };
        let middle_end = if self.suffix_anchored {
            segs.len().saturating_sub(1)
        } else {
            segs.len()
        };

        if middle_start > middle_end {
            return false;
        }

        for segment in &segs[middle_start..middle_end] {
            match remaining.find(segment.as_str()) {
                Some(idx) => remaining = &remaining[idx + segment.len()..],
                None => return false,
            }
        }

        if self.suffix_anchored {
            let last = segs[segs.len() - 1].as_str();
            if !remaining.ends_with(last) {
                return false;
            }
        }

        true
    }
}

/// Per-team body-logging filter. Holds both the original pattern strings (for
/// faithful round-tripping into log fields) and their compiled forms (for
/// matching). `raw.is_empty()` means "log every flag" for this team.
#[derive(Debug)]
pub struct TeamPatterns {
    raw: Vec<String>,
    compiled: Vec<CompiledPattern>,
}

impl TeamPatterns {
    fn new(raw: Vec<String>) -> Self {
        let compiled = raw.iter().map(|p| CompiledPattern::compile(p)).collect();
        Self { raw, compiled }
    }

    /// True when the response's flags should be filtered. False = log all.
    pub fn is_filter_active(&self) -> bool {
        !self.raw.is_empty()
    }

    /// True when `key` either matches one of the configured patterns, or
    /// when no patterns are configured (log-all mode).
    pub fn matches(&self, key: &str) -> bool {
        !self.is_filter_active() || self.compiled.iter().any(|p| p.matches(key))
    }
}

/// In-memory body-logging config, refreshed periodically from Postgres.
pub struct BodyLogger {
    config: RwLock<HashMap<TeamId, Arc<TeamPatterns>>>,
    /// Cheap short-circuit for the common no-team-enabled path. Tracks
    /// whether the config map is non-empty so per-request lookups can skip
    /// the read lock entirely. Updated atomically alongside `config`.
    any_enabled: AtomicBool,
    /// Unix epoch seconds of the last refresh attempt. Atomic so callers can
    /// claim refresh ownership without holding the config lock.
    last_refresh: AtomicU64,
    /// Maximum request body bytes to log; bodies above this are truncated.
    pub request_max_bytes: usize,
}

impl BodyLogger {
    pub fn new(initial: BodyLogTeams, request_max_bytes: usize) -> Self {
        let map = compile_all(initial);
        let any_enabled = AtomicBool::new(!map.is_empty());
        Self {
            config: RwLock::new(map),
            any_enabled,
            last_refresh: AtomicU64::new(0),
            request_max_bytes,
        }
    }

    /// True when at least one team is in the allow-list. Lets callers skip
    /// per-request work (body clone, lock acquisition) in the common case.
    pub fn has_any_enabled(&self) -> bool {
        self.any_enabled.load(Ordering::Relaxed)
    }

    /// Returns the per-team filter, or `None` when the team isn't enabled.
    pub fn for_team(&self, team_id: TeamId) -> Option<Arc<TeamPatterns>> {
        if !self.has_any_enabled() {
            return None;
        }
        self.config
            .read()
            .expect("body logger config poisoned")
            .get(&team_id)
            .cloned()
    }

    /// Cheap pre-spawn check: true when the in-memory config is older than
    /// `ttl_secs`. Callers race-safely spawn a refresh; the CAS in
    /// `refresh_if_stale` ensures only one wins per window.
    pub fn is_stale(&self, ttl_secs: u64) -> bool {
        let now = unix_secs_now();
        now.saturating_sub(self.last_refresh.load(Ordering::Relaxed)) >= ttl_secs
    }

    /// Atomic check-and-set: returns true at most once per `ttl_secs` window.
    /// The caller that wins the claim is responsible for performing the refresh.
    fn claim_refresh(&self, ttl_secs: u64) -> bool {
        let now = unix_secs_now();
        let last = self.last_refresh.load(Ordering::Relaxed);
        if now.saturating_sub(last) < ttl_secs {
            return false;
        }
        self.last_refresh
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    fn update(&self, raw: BodyLogTeams) {
        let map = compile_all(raw);
        self.any_enabled.store(!map.is_empty(), Ordering::Relaxed);
        let mut cfg = self.config.write().expect("body logger config poisoned");
        *cfg = map;
    }

    /// Refresh the body-log config from the database if the in-memory copy is
    /// stale. Best-effort: on DB error, the cached config is kept and a
    /// warning is logged. Cheap to call on every request — the atomic CAS
    /// short-circuits all but one caller per `ttl_secs` window.
    pub async fn refresh_if_stale(&self, pool: &PgPool, ttl_secs: u64) {
        if !self.claim_refresh(ttl_secs) {
            return;
        }

        match fetch_from_db(pool).await {
            Ok(Some(raw)) => self.update(raw),
            Ok(None) => {
                // Row not in DB — keep the env-var default at boot, or the
                // most recent successful refresh.
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to refresh FLAGS_LOG_BODIES_TEAMS from database, keeping cached value"
                );
            }
        }
    }

    /// Emit the `flags_body_log` tracing event for a request that was
    /// resolved to an opted-in team. The caller is responsible for ensuring
    /// `for_team(team_id)` returned `Some(patterns)` and that `raw_body`
    /// is the original request bytes.
    pub fn emit_event(
        &self,
        request_id: Uuid,
        team_id: TeamId,
        raw_body: &[u8],
        response: &FlagsResponse,
        patterns: &TeamPatterns,
    ) {
        let (truncated, request_truncated, request_original_size_bytes) =
            truncate_body(raw_body, self.request_max_bytes);
        let request_body = String::from_utf8_lossy(truncated);
        let (response_body, total, logged) = serialize_filtered_response(response, patterns);
        let response_filter_patterns = patterns.raw.join(",");

        tracing::info!(
            event = BODY_LOG_EVENT,
            request_id = %request_id,
            team_id = team_id,
            request_body = %request_body,
            response_body = %response_body,
            request_truncated = request_truncated,
            request_original_size_bytes = request_original_size_bytes,
            response_filtered = patterns.is_filter_active(),
            response_filter_patterns = %response_filter_patterns,
            response_flag_count_total = total,
            response_flag_count_logged = logged,
            BODY_LOG_EVENT,
        );
    }
}

fn compile_all(raw: BodyLogTeams) -> HashMap<TeamId, Arc<TeamPatterns>> {
    raw.0
        .into_iter()
        .map(|(team_id, patterns)| (team_id, Arc::new(TeamPatterns::new(patterns))))
        .collect()
}

fn unix_secs_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn fetch_from_db(pool: &PgPool) -> Result<Option<BodyLogTeams>, String> {
    let raw = match fetch_instance_setting_raw_value(pool, &CONSTANCE_KEY).await? {
        Some(v) => v,
        None => return Ok(None),
    };
    raw.parse::<BodyLogTeams>().map(Some)
}

/// Truncate a body to `max_bytes`, returning the prefix slice, whether it was
/// truncated, and the original byte length. Respects UTF-8 char boundaries
/// (RFC 3629) so the resulting slice is safe to pass to `from_utf8_lossy`
/// without splitting a multi-byte sequence.
pub fn truncate_body(body: &[u8], max_bytes: usize) -> (&[u8], bool, usize) {
    let original_len = body.len();
    if original_len <= max_bytes {
        return (body, false, original_len);
    }

    // Walk back from `max_bytes` past any UTF-8 continuation bytes (10xxxxxx)
    // to the start of the last fully-included character.
    let mut end = max_bytes;
    while end > 0 && (body[end] & 0b1100_0000) == 0b1000_0000 {
        end -= 1;
    }
    (&body[..end], true, original_len)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoggedResponse<'a> {
    errors_while_computing_flags: bool,
    flags: HashMap<&'a String, &'a FlagDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quota_limited: &'a Option<Vec<String>>,
    request_id: Uuid,
    evaluated_at: i64,
}

/// Serialize the response with `flags` filtered to keys matching `patterns`.
/// Returns the JSON string plus `(total_flags, logged_flags)` counts. One
/// serialization pass — no intermediate `serde_json::Value`.
fn serialize_filtered_response(
    response: &FlagsResponse,
    patterns: &TeamPatterns,
) -> (String, usize, usize) {
    let total = response.flags.len();

    let flags: HashMap<&String, &FlagDetails> = if patterns.is_filter_active() {
        response
            .flags
            .iter()
            .filter(|(key, _)| patterns.matches(key))
            .collect()
    } else {
        response.flags.iter().collect()
    };
    let logged = flags.len();

    let payload = LoggedResponse {
        errors_while_computing_flags: response.errors_while_computing_flags,
        flags,
        quota_limited: &response.quota_limited,
        request_id: response.request_id,
        evaluated_at: response.evaluated_at,
    };

    let body = serde_json::to_string(&payload).unwrap_or_default();
    (body, total, logged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::{
        FlagDetails, FlagDetailsMetadata, FlagEvaluationReason, FlagsResponse,
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    fn make_flag(key: &str, enabled: bool) -> FlagDetails {
        FlagDetails {
            key: key.to_string(),
            enabled,
            variant: None,
            failed: false,
            reason: FlagEvaluationReason {
                code: "matched".to_string(),
                description: None,
                condition_index: None,
            },
            metadata: FlagDetailsMetadata {
                id: 1,
                version: 1,
                description: None,
                payload: None,
            },
            conditions: None,
        }
    }

    fn make_response(flag_keys: &[&str]) -> FlagsResponse {
        let mut flags = HashMap::new();
        for k in flag_keys {
            flags.insert(k.to_string(), make_flag(k, true));
        }
        FlagsResponse::new(false, flags, None, Uuid::nil())
    }

    #[test]
    fn pattern_exact_match() {
        let p = CompiledPattern::compile("my-feature");
        assert!(p.matches("my-feature"));
        assert!(!p.matches("my-feature-2"));
        assert!(!p.matches("not-my-feature"));
    }

    #[test]
    fn pattern_prefix_wildcard() {
        let p = CompiledPattern::compile("checkout-*");
        assert!(p.matches("checkout-foo"));
        assert!(p.matches("checkout-"));
        assert!(p.matches("checkout-bar-baz"));
        assert!(!p.matches("checkou"));
        assert!(!p.matches("not-checkout-x"));
    }

    #[test]
    fn pattern_suffix_wildcard() {
        let p = CompiledPattern::compile("*-targeting");
        assert!(p.matches("survey-targeting"));
        assert!(p.matches("-targeting"));
        assert!(!p.matches("targeting-other"));
    }

    #[test]
    fn pattern_middle_wildcard() {
        let p = CompiledPattern::compile("survey-*-targeting");
        assert!(p.matches("survey-abc-targeting"));
        assert!(p.matches("survey--targeting"));
        assert!(!p.matches("survey-abc-other"));
        assert!(!p.matches("not-survey-abc-targeting"));
    }

    #[test]
    fn pattern_match_all() {
        let p = CompiledPattern::compile("*");
        assert!(p.matches(""));
        assert!(p.matches("anything"));
        assert!(p.matches("with-dashes-123"));
    }

    #[test]
    fn for_team_skip_when_unlisted() {
        let logger = BodyLogger::new(BodyLogTeams::default(), 65_536);
        assert!(!logger.has_any_enabled());
        assert!(logger.for_team(42).is_none());
    }

    #[test]
    fn for_team_log_all_when_empty_patterns() {
        let mut map = HashMap::new();
        map.insert(42, vec![]);
        let logger = BodyLogger::new(BodyLogTeams(map), 65_536);
        assert!(logger.has_any_enabled());
        let p = logger.for_team(42).expect("expected entry for team 42");
        assert!(!p.is_filter_active());
        assert!(p.matches("anything-goes"));
    }

    #[test]
    fn for_team_log_matching_when_patterns_set() {
        let mut map = HashMap::new();
        map.insert(42, vec!["my-feature".into(), "checkout-*".into()]);
        let logger = BodyLogger::new(BodyLogTeams(map), 65_536);
        let p = logger.for_team(42).expect("expected entry for team 42");
        assert!(p.is_filter_active());
        assert_eq!(p.raw, vec!["my-feature", "checkout-*"]);
        assert!(p.matches("my-feature"));
        assert!(p.matches("checkout-foo"));
        assert!(!p.matches("other-flag"));
    }

    #[test]
    fn serialize_filtered_response_passes_all_when_log_all() {
        let resp = make_response(&["a", "b", "c"]);
        let patterns = TeamPatterns::new(vec![]);
        let (_body, total, logged) = serialize_filtered_response(&resp, &patterns);
        assert_eq!(total, 3);
        assert_eq!(logged, 3);
    }

    #[test]
    fn serialize_filtered_response_filters_to_matching() {
        let resp = make_response(&["my-feature", "checkout-foo", "other"]);
        let patterns = TeamPatterns::new(vec!["my-feature".into(), "checkout-*".into()]);
        let (_body, total, logged) = serialize_filtered_response(&resp, &patterns);
        assert_eq!(total, 3);
        assert_eq!(logged, 2);
    }

    #[test]
    fn serialize_filtered_response_zero_when_no_match() {
        let resp = make_response(&["a", "b"]);
        let patterns = TeamPatterns::new(vec!["nothing-matches-*".into()]);
        let (_body, total, logged) = serialize_filtered_response(&resp, &patterns);
        assert_eq!(total, 2);
        assert_eq!(logged, 0);
    }

    #[test]
    fn truncate_body_under_cap() {
        let (out, truncated, original) = truncate_body(b"hello", 10);
        assert_eq!(out, b"hello");
        assert!(!truncated);
        assert_eq!(original, 5);
    }

    #[test]
    fn truncate_body_at_cap() {
        let (out, truncated, original) = truncate_body(b"hello", 5);
        assert_eq!(out, b"hello");
        assert!(!truncated);
        assert_eq!(original, 5);
    }

    #[test]
    fn truncate_body_over_cap() {
        let (out, truncated, original) = truncate_body(b"hello world", 5);
        assert_eq!(out, b"hello");
        assert!(truncated);
        assert_eq!(original, 11);
    }

    #[test]
    fn truncate_body_respects_utf8_boundary() {
        // "héllo" — é is 2 bytes (0xC3 0xA9). Cap at 2 must not split it.
        let body = "héllo".as_bytes();
        let (out, truncated, _) = truncate_body(body, 2);
        assert_eq!(out, b"h");
        assert!(truncated);
    }

    #[test]
    fn body_log_teams_parses_empty() {
        assert!("{}".parse::<BodyLogTeams>().unwrap().0.is_empty());
        assert!("".parse::<BodyLogTeams>().unwrap().0.is_empty());
    }

    #[test]
    fn body_log_teams_parses_populated() {
        let parsed: BodyLogTeams = r#"{"123": [], "456": ["my-feature", "checkout-*"]}"#
            .parse()
            .unwrap();
        assert_eq!(parsed.0.len(), 2);
        assert!(parsed.0[&123].is_empty());
        assert_eq!(parsed.0[&456], vec!["my-feature", "checkout-*"]);
    }

    #[test]
    fn body_log_teams_rejects_invalid_team_id() {
        let result: Result<BodyLogTeams, _> = r#"{"abc": []}"#.parse();
        assert!(result.is_err());
    }

    #[test]
    fn refresh_claim_is_atomic_per_window() {
        let logger = BodyLogger::new(BodyLogTeams::default(), 65_536);
        assert!(logger.claim_refresh(60));
        assert!(!logger.claim_refresh(60));
        assert!(!logger.claim_refresh(60));
    }

    #[test]
    fn is_stale_reflects_last_refresh() {
        let logger = BodyLogger::new(BodyLogTeams::default(), 65_536);
        assert!(logger.is_stale(60));
        let _ = logger.claim_refresh(60);
        assert!(!logger.is_stale(60));
    }
}
