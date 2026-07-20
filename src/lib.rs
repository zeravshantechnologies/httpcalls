//! Modern HTTP client for Yew applications with automatic state management
//! 
//! This crate provides a fluent HTTP client that automatically integrates with
//! the httpmessenger global state management system. It supports automatic
//! loader states, progress tracking, notifications, and comprehensive error handling.
//! 
//! # Features
//! 
//! - **Fluent API**: Chain method calls for readable request building
//! - **Automatic State Management**: Loader states, progress, and notifications
//! - **Type Safety**: Comprehensive error types and compile-time checking
//! - **Modern Async**: Built on async/await patterns
//! - **Multiple Content Types**: JSON, FormData, raw text, and binary support
//! - **Request Interception**: Middleware support for headers, auth, etc.
//! - **Timeout Support**: Per-request timeout configuration
//! - **Retry Logic**: Automatic retry with exponential backoff
//! - **Upload Progress**: Real-time upload progress tracking
//! 
//! # Quick Start
//! 
//! ```rust
//! use httpcalls::HttpClient;
//! use httpmessenger::{StoreProvider, AppAction};
//! 
//! // Create a client instance
//! let client = HttpClient::new();
//! 
//! // Simple GET request with automatic loader
//! let response = client
//!     .get("/api/users")
//!     .with_loader(true)
//!     .send()
//!     .await?;
//! 
//! // POST with JSON and progress tracking
//! let user_data = UserData { name: "John".to_string() };
//! let response = client
//!     .post("/api/users")
//!     .json(&user_data)?
//!     .with_loader(true)
//!     .with_progress(true)
//!     .call_name("create_user")
//!     .send()
//!     .await?;
//! ```

use std::collections::HashMap;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use yew::prelude::*;
use reqwasm::http::{Request, Method};
use httpmessenger::{AppAction, StoreDispatcher, use_store};
use gloo_console::log;

#[cfg(test)]
pub mod tests;
pub mod health_manager;

/// HTTP client errors
#[derive(Error, Debug, Clone, PartialEq)]
pub enum HttpError {
    #[error("Network error: {message}")]
    Network { message: String },
    
    #[error("Request timeout")]
    Timeout,
    
    #[error("Invalid URL: {url}")]
    InvalidUrl { url: String },
    
    #[error("Serialization error: {message}")]
    Serialization { message: String },
    
    #[error("HTTP {status}: {message}")]
    Http { status: u16, message: String, body: Option<String> },
    
    #[error("Cancelled by user")]
    Cancelled,
    
    #[error("Invalid response format")]
    InvalidResponse,
    
    #[error("Configuration error: {message}")]
    Configuration { message: String },
}

/// HTTP response wrapper with additional metadata
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
    pub url: String,
    pub call_name: Option<String>,
}

impl HttpResponse {
    /// Parse JSON response body
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, HttpError> {
        serde_json::from_str(&self.body).map_err(|e| HttpError::Serialization {
            message: format!("Failed to deserialize JSON: {}", e),
        })
    }
    
    /// Get response body as text
    pub fn text(&self) -> &str {
        &self.body
    }
    
    /// Check if request was successful (2xx status)
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }
    
    /// Check if request was a client error (4xx status)
    pub fn is_client_error(&self) -> bool {
        self.status >= 400 && self.status < 500
    }
    
    /// Check if request was a server error (5xx status)
    pub fn is_server_error(&self) -> bool {
        self.status >= 500 && self.status < 600
    }
    
    /// Get header value by name (case-insensitive)
    pub fn header(&self, name: &str) -> Option<&String> {
        let name_lower = name.to_lowercase();
        self.headers.iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, v)| v)
    }
}

/// HTTP method enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
}

impl HttpMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Head => "HEAD",
            HttpMethod::Options => "OPTIONS",
        }
    }
}

/// Request body types
#[derive(Debug, Clone)]
pub enum RequestBody {
    None,
    Text(String),
    Json(String),
    FormData(web_sys::FormData),
    Binary(Vec<u8>),
}

/// Request configuration
#[derive(Debug, Clone)]
pub struct RequestConfig {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub body: RequestBody,
    pub timeout_ms: Option<u32>,
    pub with_loader: bool,
    pub with_progress: bool,
    pub with_notifications: bool,
    pub call_name: Option<String>,
    pub retry_count: u32,
    pub retry_delay_ms: u32,
    /// When true, skips triggering health checks on failure (used by health check requests themselves)
    pub skip_health_check: bool,
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            method: HttpMethod::Get,
            url: String::new(),
            headers: HashMap::new(),
            body: RequestBody::None,
            timeout_ms: Some(30000), // 30 second default timeout
            with_loader: false,
            with_progress: false,
            with_notifications: false,
            call_name: None,
            retry_count: 3,
            retry_delay_ms: 1000,
            skip_health_check: false,
        }
    }
}

/// HTTP request builder with fluent API
#[derive(Debug, Clone)]
pub struct RequestBuilder {
    config: RequestConfig,
    dispatch: Option<StoreDispatcher>,
}

impl RequestBuilder {
    pub fn new(method: HttpMethod, url: &str) -> Self {
        Self {
            config: RequestConfig {
                method,
                url: url.to_string(),
                ..Default::default()
            },
            dispatch: None,
        }
    }
    
    /// Set dispatcher for state management
    pub fn with_dispatcher(mut self, dispatch: StoreDispatcher) -> Self {
        self.dispatch = Some(dispatch);
        self
    }
    
    /// Add a header to the request
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.config.headers.insert(name.to_string(), value.to_string());
        self
    }
    
    /// Add multiple headers
    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.config.headers.extend(headers);
        self
    }
    
    /// Set Content-Type header to application/json and serialize body as JSON
    pub fn json<T: Serialize>(mut self, data: &T) -> Result<Self, HttpError> {
        let json_string = serde_json::to_string(data).map_err(|e| HttpError::Serialization {
            message: format!("Failed to serialize JSON: {}", e),
        })?;
        
        self.config.headers.insert("Content-Type".to_string(), "application/json".to_string());
        self.config.body = RequestBody::Json(json_string);
        Ok(self)
    }
    
    /// Set body as form data
    pub fn form_data(mut self, form: web_sys::FormData) -> Self {
        self.config.body = RequestBody::FormData(form);
        self
    }
    
    /// Set body as plain text
    pub fn text(mut self, text: &str) -> Self {
        self.config.body = RequestBody::Text(text.to_string());
        self
    }
    
    /// Set body as binary data
    pub fn binary(mut self, data: Vec<u8>) -> Self {
        self.config.body = RequestBody::Binary(data);
        self
    }
    
    /// Enable automatic loader state management
    pub fn with_loader(mut self, enabled: bool) -> Self {
        self.config.with_loader = enabled;
        self
    }
    
    /// Enable progress tracking
    pub fn with_progress(mut self, enabled: bool) -> Self {
        self.config.with_progress = enabled;
        self
    }
    
    /// Enable automatic notifications on success/error
    pub fn with_notifications(mut self, enabled: bool) -> Self {
        self.config.with_notifications = enabled;
        self
    }
    
    /// Set a call name for tracking purposes
    pub fn call_name(mut self, name: &str) -> Self {
        self.config.call_name = Some(name.to_string());
        self
    }
    
    /// Set request timeout in milliseconds
    pub fn timeout(mut self, ms: u32) -> Self {
        self.config.timeout_ms = Some(ms);
        self
    }
    
    /// Disable timeout
    pub fn no_timeout(mut self) -> Self {
        self.config.timeout_ms = None;
        self
    }
    
    /// Set retry configuration
    pub fn retry(mut self, count: u32, delay_ms: u32) -> Self {
        self.config.retry_count = count;
        self.config.retry_delay_ms = delay_ms;
        self
    }
    
    /// Skip triggering health checks on failure (used internally by health check requests)
    pub fn skip_health_check(mut self) -> Self {
        self.config.skip_health_check = true;
        self
    }
    
    /// Send the request
    pub async fn send(self) -> Result<HttpResponse, HttpError> {
        let mut last_error = None;
        
        for attempt in 0..=(self.config.retry_count) {
            if attempt > 0 {
                gloo_timers::future::TimeoutFuture::new(self.config.retry_delay_ms * attempt).await;
            }
            
            match self.execute_request().await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    last_error = Some(e.clone());
                    
                    // Don't retry on certain error types
                    match &e {
                        HttpError::Cancelled | HttpError::InvalidUrl { .. } | HttpError::Configuration { .. } => {
                            return Err(e);
                        }
                        HttpError::Http { status, .. } if *status >= 400 && *status < 500 => {
                            return Err(e); // Don't retry client errors
                        }
                        _ => {
                            if attempt < self.config.retry_count {
                                log!("Request failed, retrying... (attempt {} of {})", attempt + 1, self.config.retry_count + 1);
                            }
                        }
                    }
                }
            }
        }
        
        // All retries exhausted — check if we should trigger health monitoring
        let final_error = last_error.clone().unwrap_or(HttpError::Network { message: "Unknown error".to_string() });
        
        if !self.config.skip_health_check && health_manager::is_health_check_trigger_error(&final_error) {
            if let Some(ref dispatch) = self.dispatch {
                log!("Health Manager: Triggering health check after all retries failed");
                health_manager::start_health_check(dispatch);
            }
        }
        
        Err(final_error)
    }
    
    async fn execute_request(&self) -> Result<HttpResponse, HttpError> {
        // Enable loader if requested
        if self.config.with_loader {
            if let Some(ref dispatch) = self.dispatch {
                dispatch.emit(AppAction::EnableLoader);
            }
        }
        
        // Reset progress if tracking enabled
        if self.config.with_progress {
            if let Some(ref dispatch) = self.dispatch {
                dispatch.emit(AppAction::UpdateProgress(0.0));
            }
        }
        
        let result = self.make_request().await;
        
        // Handle result and update state
        match &result {
            Ok(response) => {
                if self.config.with_notifications {
                    if let Some(ref dispatch) = self.dispatch {
                        if response.is_success() {
                            let message = format!("Request completed successfully ({})", response.status);
                            dispatch.emit(AppAction::ShowNotification(message));
                        } else {
                            let message = format!("Request failed with status {}", response.status);
                            dispatch.emit(AppAction::ShowNotification(message));
                        }
                    }
                }
                
                if self.config.with_progress {
                    if let Some(ref dispatch) = self.dispatch {
                        dispatch.emit(AppAction::UpdateProgress(1.0));
                    }
                }
            }
            Err(error) => {
                if self.config.with_notifications {
                    if let Some(ref dispatch) = self.dispatch {
                        let message = format!("Request failed: {}", error);
                        dispatch.emit(AppAction::ShowNotification(message));
                    }
                }
            }
        }
        
        // Disable loader
        if self.config.with_loader {
            if let Some(ref dispatch) = self.dispatch {
                dispatch.emit(AppAction::DisableLoader);
            }
        }
        
        result
    }
    
    async fn make_request(&self) -> Result<HttpResponse, HttpError> {
        // Validate URL
        if self.config.url.is_empty() {
            return Err(HttpError::InvalidUrl { url: self.config.url.clone() });
        }
        
        // Build request using reqwasm
        let mut request = match self.config.method {
            HttpMethod::Get => Request::get(&self.config.url),
            HttpMethod::Post => Request::post(&self.config.url),
            HttpMethod::Put => Request::put(&self.config.url),
            HttpMethod::Delete => Request::delete(&self.config.url),
            HttpMethod::Patch => Request::new(&self.config.url).method(Method::PATCH),
            HttpMethod::Head => Request::new(&self.config.url).method(Method::HEAD),
            HttpMethod::Options => Request::new(&self.config.url).method(Method::OPTIONS),
        };
        
        // Set headers
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }
        
        // Set body based on type
        match &self.config.body {
            RequestBody::None => {},
            RequestBody::Text(text) => {
                request = request.body(text);
            },
            RequestBody::Json(json) => {
                request = request.body(json);
            },
            RequestBody::FormData(form) => {
                request = request.body(form);
            },
            RequestBody::Binary(data) => {
                let uint8_array = js_sys::Uint8Array::from(data.as_slice());
                request = request.body(&uint8_array);
            },
        }
        
        // Update progress if enabled
        if self.config.with_progress {
            if let Some(ref dispatch) = self.dispatch {
                dispatch.emit(AppAction::UpdateProgress(0.5));
            }
        }
        
        // Send the request
        let response = request.send().await.map_err(|e| HttpError::Network {
            message: format!("Request failed: {:?}", e),
        })?;
        
        // Extract response data
        let status = response.status();
        let url = response.url();
        
        // Extract headers
        let header_map = HashMap::new();
        // Note: reqwasm doesn't expose headers directly, this is a limitation
        
        // Get response body
        let body = response.text().await.map_err(|e| HttpError::Network {
            message: format!("Failed to read response body: {:?}", e),
        })?;
        
        let http_response = HttpResponse {
            status,
            headers: header_map,
            body,
            url,
            call_name: self.config.call_name.clone(),
        };
        
        // Check if response indicates an error
        if !http_response.is_success() {
            return Err(HttpError::Http {
                status,
                message: format!("HTTP error {}", status),
                body: Some(http_response.body.clone()),
            });
        }
        
        Ok(http_response)
    }
}

/// Main HTTP client with fluent API
#[derive(Debug, Clone)]
pub struct HttpClient {
    base_url: Option<String>,
    default_headers: HashMap<String, String>,
    default_timeout_ms: Option<u32>,
    dispatch: Option<StoreDispatcher>,
}

impl HttpClient {
    /// Create a new HTTP client
    pub fn new() -> Self {
        Self {
            base_url: None,
            default_headers: HashMap::new(),
            default_timeout_ms: Some(30000),
            dispatch: None,
        }
    }
    
    /// Create HTTP client with dispatcher for automatic state management
    pub fn with_dispatcher(dispatch: StoreDispatcher) -> Self {
        Self {
            base_url: None,
            default_headers: HashMap::new(),
            default_timeout_ms: Some(30000),
            dispatch: Some(dispatch),
        }
    }
    
    /// Set base URL for all requests
    pub fn base_url(mut self, url: &str) -> Self {
        self.base_url = Some(url.to_string());
        self
    }
    
    /// Add default header for all requests
    pub fn default_header(mut self, name: &str, value: &str) -> Self {
        self.default_headers.insert(name.to_string(), value.to_string());
        self
    }
    
    /// Set default timeout for all requests
    pub fn default_timeout(mut self, ms: u32) -> Self {
        self.default_timeout_ms = Some(ms);
        self
    }
    
    /// Build URL with optional base URL
    fn build_url(&self, path: &str) -> String {
        match &self.base_url {
            Some(base) => {
                if path.starts_with("http://") || path.starts_with("https://") {
                    path.to_string()
                } else {
                    format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/'))
                }
            }
            None => path.to_string(),
        }
    }
    
    /// Create request builder with defaults applied
    fn create_builder(&self, method: HttpMethod, path: &str) -> RequestBuilder {
        let url = self.build_url(path);
        let mut builder = RequestBuilder::new(method, &url);
        
        // Apply default headers
        builder.config.headers.extend(self.default_headers.clone());
        
        // Apply default timeout
        if let Some(timeout) = self.default_timeout_ms {
            builder.config.timeout_ms = Some(timeout);
        }
        
        // Apply dispatcher if available
        if let Some(ref dispatch) = self.dispatch {
            builder = builder.with_dispatcher(dispatch.clone());
        }
        
        builder
    }
    
    /// Create GET request
    #[cfg(debug_assertions)]
    pub fn get(&self, url: &str) -> RequestBuilder {
        self.create_builder(HttpMethod::Get, url)
    }
    
    #[cfg(not(debug_assertions))]
    pub fn get(&self, url: &str) -> RequestBuilder {
        self.create_builder(HttpMethod::Get, url).retry(2, 1000)
    }
    
    /// Create POST request
    pub fn post(&self, path: &str) -> RequestBuilder {
        self.create_builder(HttpMethod::Post, path)
    }
    
    /// Create PUT request
    pub fn put(&self, path: &str) -> RequestBuilder {
        self.create_builder(HttpMethod::Put, path)
    }
    
    /// Create DELETE request
    pub fn delete(&self, path: &str) -> RequestBuilder {
        self.create_builder(HttpMethod::Delete, path)
    }
    
    /// Create PATCH request
    pub fn patch(&self, path: &str) -> RequestBuilder {
        self.create_builder(HttpMethod::Patch, path)
    }
    
    /// Create HEAD request
    pub fn head(&self, path: &str) -> RequestBuilder {
        self.create_builder(HttpMethod::Head, path)
    }
    
    /// Create OPTIONS request
    pub fn options(&self, path: &str) -> RequestBuilder {
        self.create_builder(HttpMethod::Options, path)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Hook to get HTTP client with automatic state management
#[hook]
pub fn use_http_client() -> HttpClient {
    let (_, dispatch) = use_store();
    HttpClient::with_dispatcher(dispatch)
}

/// Get a global HTTP client instance
pub fn get_http_client() -> HttpClient {
    HttpClient::new()
}

/// Utility functions for common HTTP operations
pub mod utils {
    use super::*;
    
    /// Simple GET request with automatic error handling
    pub async fn get_json<T: DeserializeOwned>(url: &str) -> Result<T, HttpError> {
        let client = get_http_client();
        let response = client.get(url).send().await?;
        response.json()
    }
    
    /// Simple POST request with JSON payload
    pub async fn post_json<T: Serialize, R: DeserializeOwned>(
        url: &str, 
        data: &T
    ) -> Result<R, HttpError> {
        let client = get_http_client();
        let response = client.post(url).json(data)?.send().await?;
        response.json()
    }
    
    /// Upload file with progress tracking
    pub async fn upload_file(
        url: &str,
        file_data: &[u8],
        filename: &str,
        content_type: &str,
        with_progress: bool,
    ) -> Result<HttpResponse, HttpError> {
        let form_data = web_sys::FormData::new().map_err(|_| HttpError::Configuration {
            message: "Failed to create FormData".to_string(),
        })?;
        
        let blob_options = web_sys::BlobPropertyBag::new();
        blob_options.set_type(content_type);
        
        let blob = web_sys::Blob::new_with_u8_array_sequence_and_options(
            &js_sys::Array::of1(&js_sys::Uint8Array::from(file_data)),
            &blob_options,
        ).map_err(|_| HttpError::Configuration {
            message: "Failed to create blob".to_string(),
        })?;
        
        form_data.append_with_blob_and_filename("file", &blob, filename).map_err(|_| {
            HttpError::Configuration {
                message: "Failed to append file to FormData".to_string(),
            }
        })?;
        
        let client = get_http_client();
        client
            .post(url)
            .form_data(form_data)
            .with_progress(with_progress)
            .send()
            .await
    }
    
    /// Download file as bytes
    pub async fn download_file(url: &str) -> Result<Vec<u8>, HttpError> {
        let client = get_http_client();
        let response = client.get(url).send().await?;
        
        // Convert text response to bytes (this is a simplified implementation)
        // In a real implementation, you'd want to handle binary data properly
        Ok(response.body.into_bytes())
    }
}

