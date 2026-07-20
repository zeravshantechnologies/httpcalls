//! Centralized Health Manager for service health monitoring.
//!
//! This module orchestrates health checks with:
//! - Cooldown management (2-minute cache)
//! - Recursion prevention (only one health check at a time)
//! - Duplicate request prevention
//! - Service status caching
//!
//! Health checks are only triggered after repeated failures with:
//! - 502 Bad Gateway
//! - 503 Service Unavailable
//! - 504 Gateway Timeout
//! - Timeout errors
//! - Connection Refused errors
//!
//! IMPORTANT: This module is designed for WASM (single-threaded).
//! It uses `RefCell` instead of `Mutex` and `js_sys::Date::now()`
//! instead of `std::time::Instant`, both of which are unavailable in WASM.

use std::cell::RefCell;
use gloo_console::log;
use httpmessenger::{AppAction, HealthStatus, StoreDispatcher};
use crate::{HttpClient, HttpError};

/// Cooldown duration before a new health check can be initiated (in milliseconds)
const HEALTH_CHECK_COOLDOWN_MS: f64 = 120_000.0; // 2 minutes

// Global health manager state.
// Uses `thread_local!` + `RefCell` because WASM is single-threaded —
// `Mutex` panics on recursive acquire and `Instant` is unavailable.
thread_local! {
    static HEALTH_MANAGER: RefCell<HealthManagerState> =
        RefCell::new(HealthManagerState::new());
}

/// Internal state for the health manager
struct HealthManagerState {
    /// Whether a health check is currently in progress
    health_check_in_progress: bool,
    /// Timestamp of the last completed health check (ms since epoch, from js_sys::Date::now())
    last_health_check_time: f64,
    /// Cached health status result
    cached_status: Option<HealthStatus>,
}

impl HealthManagerState {
    const fn new() -> Self {
        Self {
            health_check_in_progress: false,
            last_health_check_time: 0.0,
            cached_status: None,
        }
    }
}

/// Get current timestamp in milliseconds since epoch.
/// Returns 0.0 on non-wasm targets (for testing).
fn now_ms() -> f64 {
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::now()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        0.0
    }
}

/// Determines if an error type warrants a health check
pub fn is_health_check_trigger_error(error: &HttpError) -> bool {
    match error {
        HttpError::Http { status, .. } => {
            *status == 502 || *status == 503 || *status == 504
        }
        HttpError::Timeout => true,
        HttpError::Network { message } => {
            // Connection refused or network errors
            message.contains("Connection refused")
                || message.contains("NetworkError")
                || message.contains("Failed to fetch")
                || message.contains("TypeError")
        }
        // Do NOT trigger health checks for:
        // - Client errors (4xx)
        // - Cancelled requests
        // - Invalid URLs
        // - Serialization errors
        // - Configuration errors
        _ => false,
    }
}

/// Start a health check if conditions are met.
///
/// Returns `true` if a health check was initiated, `false` if it was skipped
/// (due to cooldown, already in progress, or cached result still valid).
///
/// CRITICAL: The `RefCell` borrow is released BEFORE spawning the async task
/// to avoid recursive borrow panics in WASM's single-threaded executor.
pub fn start_health_check(dispatch: &StoreDispatcher) -> bool {
    // Scoped block to ensure the RefCell borrow is dropped before any async work
    let should_start = HEALTH_MANAGER.with_borrow_mut(|state| {
        // Prevent recursion: if a health check is already in progress, skip
        if state.health_check_in_progress {
            log!("Health Manager: Health check already in progress, skipping");
            return false;
        }

        // Check cooldown: if last check was within 2 minutes, reuse cached result
        let now = now_ms();
        if state.last_health_check_time > 0.0 {
            let elapsed = now - state.last_health_check_time;
            if elapsed < HEALTH_CHECK_COOLDOWN_MS {
                log!("Health Manager: Cooldown active ({}s remaining), using cached result",
                    ((HEALTH_CHECK_COOLDOWN_MS - elapsed) / 1000.0) as u64);

                // Re-emit cached status if available
                if let Some(ref cached) = state.cached_status {
                    let dispatch_clone = dispatch.clone();
                    let cached_clone = cached.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        dispatch_clone.emit(AppAction::ServiceHealthUpdated(cached_clone));
                    });
                }
                return false;
            }
        }

        // Mark health check as in progress to prevent recursion
        state.health_check_in_progress = true;
        log!("Health Manager: Starting health check");
        true
    }); // <-- RefCell borrow DROPPED here, before spawn_local

    if should_start {
        let dispatch_clone = dispatch.clone();
        wasm_bindgen_futures::spawn_local(async move {
            perform_health_check(dispatch_clone).await;
        });
    }

    should_start
}

/// Check if a health check is currently running
pub fn is_health_check_running() -> bool {
    HEALTH_MANAGER.with_borrow(|state| state.health_check_in_progress)
}

/// Get the cached health status, if any
pub fn get_cached_status() -> Option<HealthStatus> {
    HEALTH_MANAGER.with_borrow(|state| state.cached_status.clone())
}

/// Perform the actual health check API call.
/// This function is called asynchronously and updates the global store.
async fn perform_health_check(dispatch: StoreDispatcher) {
    log!("Health Manager: Performing health check API call");

    // Use a dedicated HttpClient WITHOUT the store dispatcher to prevent
    // the health check request from triggering loader/notification side effects.
    // Also use skip_health_check() to prevent recursive health checks.
    let client = HttpClient::new();

    // The health endpoint returns the status of all services
    let result = client
        .get("/health/")
        .timeout(10_000) // 10 second timeout for health check
        .skip_health_check() // CRITICAL: prevent recursion
        .send()
        .await;

    let health_status = match result {
        Ok(response) => {
            log!("Health Manager: Health check response status: {}", response.status);
            if response.is_success() {
                // Parse the response body for health status
                match parse_health_response(&response.body) {
                    Ok(status) => status,
                    Err(e) => {
                        log!("Health Manager: Failed to parse health response: {}", e);
                        // Health endpoint returned 200 but body was empty/unparseable.
                        // Since we were triggered by real failures, treat as partial outage.
                        HealthStatus {
                            all_services_down: false,
                            down_services: vec!["unknown".to_string()],
                            message: "Some services are temporarily unavailable. You may experience issues in those modules. Our team has been notified.".to_string(),
                            last_updated: now_ms(),
                        }
                    }
                }
            } else {
                // Health endpoint itself returned an error
                // This could mean the reverse proxy is down
                log!("Health Manager: Health endpoint returned error status: {}", response.status);
                HealthStatus {
                    all_services_down: true,
                    down_services: vec!["health_endpoint".to_string()],
                    message: "System is temporarily unavailable due to maintenance or technical issues.".to_string(),
                    last_updated: now_ms(),
                }
            }
        }
        Err(_e) => {
            log!("Health Manager: Health check request failed");
            // If health check itself fails, treat as complete outage
            HealthStatus {
                all_services_down: true,
                down_services: vec!["health_endpoint".to_string()],
                message: "System is temporarily unavailable due to maintenance or technical issues.".to_string(),
                last_updated: now_ms(),
            }
        }
    };

    // Update the global store with health status
    dispatch.emit(AppAction::ServiceHealthUpdated(health_status.clone()));

    // Update the health manager state (new borrow — no recursion since previous borrow was dropped)
    HEALTH_MANAGER.with_borrow_mut(|state| {
        state.health_check_in_progress = false;
        state.last_health_check_time = now_ms();
        state.cached_status = Some(health_status);
        log!("Health Manager: Health check completed and cached");
    });
}

/// Parse the health check response body into a HealthStatus.
///
/// Expected backend response format:
/// ```json
/// {
///   "all_services_down": false,
///   "down_services": ["service1", "service2"],
///   "message": "Some services are experiencing issues"
/// }
/// ```
fn parse_health_response(body: &str) -> Result<HealthStatus, String> {
    #[derive(serde::Deserialize)]
    struct HealthResponse {
        #[serde(default)]
        all_services_down: bool,
        #[serde(default)]
        down_services: Vec<String>,
        #[serde(default)]
        message: String,
    }

    let parsed: HealthResponse = serde_json::from_str(body)
        .map_err(|e| format!("JSON parse error: {}", e))?;

    Ok(HealthStatus {
        all_services_down: parsed.all_services_down,
        down_services: parsed.down_services.clone(),
        message: if parsed.message.is_empty() {
            if parsed.all_services_down {
                "System is temporarily unavailable due to maintenance or technical issues.".to_string()
            } else if !parsed.down_services.is_empty() {
                "Some services are temporarily unavailable. You may experience issues in those modules. Our team has been notified.".to_string()
            } else {
                String::new()
            }
        } else {
            parsed.message
        },
        last_updated: now_ms(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_health_check_trigger_error_502() {
        let err = HttpError::Http { status: 502, message: "Bad Gateway".to_string(), body: None };
        assert!(is_health_check_trigger_error(&err));
    }

    #[test]
    fn test_is_health_check_trigger_error_503() {
        let err = HttpError::Http { status: 503, message: "Service Unavailable".to_string(), body: None };
        assert!(is_health_check_trigger_error(&err));
    }

    #[test]
    fn test_is_health_check_trigger_error_504() {
        let err = HttpError::Http { status: 504, message: "Gateway Timeout".to_string(), body: None };
        assert!(is_health_check_trigger_error(&err));
    }

    #[test]
    fn test_is_health_check_trigger_error_timeout() {
        assert!(is_health_check_trigger_error(&HttpError::Timeout));
    }

    #[test]
    fn test_is_health_check_trigger_error_connection_refused() {
        let err = HttpError::Network { message: "Connection refused".to_string() };
        assert!(is_health_check_trigger_error(&err));
    }

    #[test]
    fn test_is_health_check_trigger_error_400_not_triggered() {
        let err = HttpError::Http { status: 400, message: "Bad Request".to_string(), body: None };
        assert!(!is_health_check_trigger_error(&err));
    }

    #[test]
    fn test_is_health_check_trigger_error_404_not_triggered() {
        let err = HttpError::Http { status: 404, message: "Not Found".to_string(), body: None };
        assert!(!is_health_check_trigger_error(&err));
    }

    #[test]
    fn test_is_health_check_trigger_error_500_not_triggered() {
        let err = HttpError::Http { status: 500, message: "Internal Server Error".to_string(), body: None };
        assert!(!is_health_check_trigger_error(&err));
    }

    #[test]
    fn test_parse_health_response_partial_outage() {
        let body = r#"{"all_services_down": false, "down_services": ["service1", "service2"], "message": ""}"#;
        let result = parse_health_response(body).unwrap();
        assert!(!result.all_services_down);
        assert_eq!(result.down_services.len(), 2);
        assert!(result.message.contains("Some services are temporarily unavailable"));
    }

    #[test]
    fn test_parse_health_response_complete_outage() {
        let body = r#"{"all_services_down": true, "down_services": [], "message": ""}"#;
        let result = parse_health_response(body).unwrap();
        assert!(result.all_services_down);
        assert!(result.message.contains("System is temporarily unavailable"));
    }

    #[test]
    fn test_parse_health_response_custom_message() {
        let body = r#"{"all_services_down": false, "down_services": [], "message": "Custom outage message"}"#;
        let result = parse_health_response(body).unwrap();
        assert_eq!(result.message, "Custom outage message");
    }
}
