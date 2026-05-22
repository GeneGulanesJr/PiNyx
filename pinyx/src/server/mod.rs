use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::response::Response;
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::logging::RequestLogger;
use crate::proxy::{create_adapter, ProviderAdapter};

#[derive(Debug, Deserialize)]
struct LiteLlmPricingEntry {
    #[serde(default)]
    input_cost_per_token: Option<f64>,
    #[serde(default)]
    output_cost_per_token: Option<f64>,
    #[serde(default)]
    max_input_tokens: Option<u32>,
    #[serde(default)]
    max_output_tokens: Option<u32>,
}

const LITELLM_PRICING_URL: &str = "https://raw.githubusercontent.com/BerriAI/litellm/litellm_internal_staging/model_prices_and_context_window.json";

fn normalize_model_key(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace("_", "-")
}

fn rebuild_adapters(config: &AppConfig) -> HashMap<String, Box<dyn ProviderAdapter>> {
    let mut adapters = HashMap::new();
    for (name, provider_config) in &config.providers {
        adapters.insert(name.clone(), create_adapter(provider_config));
    }
    adapters
}

pub struct AppState {
    pub config: Arc<Mutex<AppConfig>>,
    pub config_path: PathBuf,
    pub adapters: Arc<Mutex<HashMap<String, Box<dyn ProviderAdapter>>>>,
    pub logger: RequestLogger,
}

impl AppState {
    pub fn new(config: AppConfig, config_path: PathBuf) -> Self {
        let adapters = rebuild_adapters(&config);
        for (name, provider_config) in &config.providers {
            if config.resolve_api_key(&provider_config.api_key).is_some() {
                info!(provider = name, "resolved API key");
            } else {
                warn!(
                    provider = name,
                    key_ref = &provider_config.api_key,
                    "could not resolve API key"
                );
            }
        }

        let logger = RequestLogger::new();

        Self {
            config: Arc::new(Mutex::new(config)),
            config_path,
            adapters: Arc::new(Mutex::new(adapters)),
            logger,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebUiSettings {
    pub thinking_model: String,
    pub coding_model: String,
}

fn parse_model_id(model: &str) -> Option<(String, String)> {
    let pos = model.find('/')?;
    let provider = &model[..pos];
    let model_id = &model[pos + 1..];
    if provider.is_empty() || model_id.is_empty() {
        return None;
    }
    Some((provider.to_string(), model_id.to_string()))
}

async fn find_provider_for_model(state: &AppState, model: &str) -> Option<(String, String)> {
    if let Some((provider, model_id)) = parse_model_id(model) {
        if state.adapters.lock().await.contains_key(&provider) {
            return Some((provider, model_id));
        }
    }

    let config = state.config.lock().await;
    for (provider_name, provider_config) in &config.providers {
        for m in &provider_config.models {
            if m.id == model || m.name == model {
                return Some((provider_name.clone(), m.id.clone()));
            }
        }
    }

    None
}

fn build_error_response(status: StatusCode, message: &str, error_type: &str) -> Response {
    (
        status,
        axum::Json(json!({
            "error": {
                "message": message,
                "type": error_type
            }
        })),
    )
        .into_response()
}

async fn do_proxy(
    state: &Arc<AppState>,
    provider_name: &str,
    model_id: &str,
    body: Value,
    request_id: &str,
    start: std::time::Instant,
) -> Response {
    let api_key = {
        let config = state.config.lock().await;
        let provider = match config.providers.get(provider_name) {
            Some(p) => p,
            None => {
                return build_error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Unknown provider: {}", provider_name),
                    "invalid_request_error",
                );
            }
        };

        match config.resolve_api_key(&provider.api_key) {
            Some(key) => key,
            None => {
                return build_error_response(
                    StatusCode::UNAUTHORIZED,
                    &format!("No API key for provider: {}", provider_name),
                    "authentication_error",
                );
            }
        }
    };

    let adapter_guard = state.adapters.lock().await;
    let adapter = match adapter_guard.get(provider_name) {
        Some(a) => a,
        None => {
            return build_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Adapter not found",
                "server_error",
            );
        }
    };

    let is_stream = body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    match adapter.proxy(model_id, body.clone(), &api_key).await {
        Ok(adapter_response) => {
            let elapsed = start.elapsed();
            info!(
                request_id,
                provider = provider_name,
                model = model_id,
                status = adapter_response.status,
                elapsed_ms = elapsed.as_millis(),
                "upstream response received"
            );

            if is_stream {
                let logger = state.logger.clone();
                let rid = request_id.to_string();
                let prov = provider_name.to_string();
                let mdl = model_id.to_string();
                let elapsed_ms = elapsed.as_millis() as u64;
                let upstream_status = adapter_response.status;

                let mapped_stream = adapter_response
                    .body
                    .map(|chunk_result| match chunk_result {
                        Ok(chunk) => Ok(chunk),
                        Err(e) => {
                            error!(error = %e, "stream error");
                            Err(std::io::Error::new(std::io::ErrorKind::Other, e))
                        }
                    });

                let body = Body::from_stream(mapped_stream);

                tokio::spawn(async move {
                    logger
                        .log_request(&rid, &prov, &mdl, 0, 0, elapsed_ms, upstream_status)
                        .await;
                });

                Response::builder()
                    .status(adapter_response.status)
                    .header("Content-Type", "text/event-stream")
                    .header("Cache-Control", "no-cache")
                    .header("Connection", "keep-alive")
                    .header("X-Pinyx-Request-Id", request_id)
                    .body(body)
                    .unwrap()
            } else {
                let chunks: Vec<Bytes> = {
                    let mut collected = Vec::new();
                    let mut stream = std::pin::pin!(adapter_response.body);
                    while let Some(chunk_result) = stream.next().await {
                        match chunk_result {
                            Ok(chunk) => collected.push(chunk),
                            Err(e) => {
                                error!(error = %e, "error reading response body");
                                break;
                            }
                        }
                    }
                    collected
                };

                let total_bytes: Vec<u8> = chunks.into_iter().flatten().collect();

                let logger = state.logger.clone();
                let rid = request_id.to_string();
                let prov = provider_name.to_string();
                let mdl = model_id.to_string();
                let elapsed_ms = elapsed.as_millis() as u64;
                let upstream_status = adapter_response.status;

                tokio::spawn(async move {
                    logger
                        .log_request(&rid, &prov, &mdl, 0, 0, elapsed_ms, upstream_status)
                        .await;
                });

                Response::builder()
                    .status(adapter_response.status)
                    .header("Content-Type", "application/json")
                    .header("X-Pinyx-Request-Id", request_id)
                    .body(Body::from(total_bytes))
                    .unwrap()
            }
        }
        Err(e) => {
            let elapsed = start.elapsed();
            error!(request_id, error = %e, "proxy error");
            state
                .logger
                .log_request(
                    request_id,
                    provider_name,
                    model_id,
                    0,
                    0,
                    elapsed.as_millis() as u64,
                    500,
                )
                .await;

            build_error_response(
                StatusCode::BAD_GATEWAY,
                &format!("Upstream error: {}", e),
                "upstream_error",
            )
        }
    }
}

pub async fn openai_chat_completions(
    State(state): State<Arc<AppState>>,
    body: axum::Json<Value>,
) -> Response {
    let body = body.0;
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let request_id = uuid::Uuid::new_v4().to_string();
    let start = std::time::Instant::now();
    info!(request_id, model, "received OpenAI request");

    let (provider_name, model_id) = match find_provider_for_model(&state, &model).await {
        Some(result) => result,
        None => {
            return build_error_response(
                StatusCode::BAD_REQUEST,
                &format!("No provider found for model: {}", model),
                "invalid_request_error",
            )
        }
    };

    do_proxy(&state, &provider_name, &model_id, body, &request_id, start).await
}

pub async fn anthropic_messages(
    State(state): State<Arc<AppState>>,
    body: axum::Json<Value>,
) -> Response {
    let body = body.0;
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let request_id = uuid::Uuid::new_v4().to_string();
    let start = std::time::Instant::now();
    info!(request_id, model, "received Anthropic request");

    if let Some((provider, model_id)) = find_provider_for_model(&state, &model).await {
        return do_proxy(&state, &provider, &model_id, body, &request_id, start).await;
    }
    if state.adapters.lock().await.contains_key("anthropic") {
        return do_proxy(&state, "anthropic", &model, body, &request_id, start).await;
    }
    build_error_response(
        StatusCode::BAD_REQUEST,
        &format!("No provider found for model: {}", model),
        "invalid_request_error",
    )
}

pub async fn list_models(State(state): State<Arc<AppState>>) -> axum::Json<Value> {
    let mut models = Vec::new();
    let config = state.config.lock().await;
    for (provider_name, provider_config) in &config.providers {
        for m in &provider_config.models {
            models.push(json!({"id": format!("{}/{}", provider_name, m.id),"object": "model","created": 0,"owned_by": provider_name,"name": if m.name.is_empty() { &m.id } else { &m.name },"reasoning": m.reasoning,"input": m.input,"context_window": m.context_window,"max_tokens": m.max_tokens,"cost": {"input": m.cost.input,"output": m.cost.output,"cache_read": m.cost.cache_read,"cache_write": m.cost.cache_write}}));
        }
    }
    axum::Json(json!({"object":"list","data":models}))
}

pub async fn health(State(state): State<Arc<AppState>>) -> axum::Json<Value> {
    let mut providers = serde_json::Map::new();
    let config = state.config.lock().await;
    for (name, _) in &config.providers {
        let has_key = config
            .providers
            .get(name)
            .and_then(|p| config.resolve_api_key(&p.api_key))
            .is_some();
        providers.insert(name.clone(), json!({"status": if has_key { "configured" } else { "missing_key" },"api_key": has_key,}));
    }
    axum::Json(
        json!({"status":"ok","version": env!("CARGO_PKG_VERSION"),"providers":providers,"gateway":{"host": &config.gateway.host,"port": config.gateway.port}}),
    )
}

pub async fn get_config(State(state): State<Arc<AppState>>) -> axum::Json<AppConfig> {
    axum::Json(state.config.lock().await.clone())
}

pub async fn put_config(
    State(state): State<Arc<AppState>>,
    body: axum::Json<AppConfig>,
) -> Response {
    let config = body.0;
    let serialized = match serde_json::to_string_pretty(&config) {
        Ok(v) => v,
        Err(e) => {
            return build_error_response(StatusCode::BAD_REQUEST, &e.to_string(), "invalid_config")
        }
    };
    if let Some(parent) = state.config_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::write(&state.config_path, serialized) {
        return build_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "write_failed",
        );
    }
    *state.config.lock().await = config.clone();
    *state.adapters.lock().await = rebuild_adapters(&config);
    (
        StatusCode::OK,
        axum::Json(json!({"ok": true, "message": "config saved"})),
    )
        .into_response()
}

pub async fn get_settings(State(state): State<Arc<AppState>>) -> axum::Json<Value> {
    let path = state.config_path.with_file_name("settings.json");
    let settings = std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<WebUiSettings>(&s).ok())
        .unwrap_or(WebUiSettings {
            thinking_model: String::new(),
            coding_model: String::new(),
        });
    axum::Json(json!(settings))
}

pub async fn put_settings(
    State(state): State<Arc<AppState>>,
    body: axum::Json<WebUiSettings>,
) -> Response {
    let path = state.config_path.with_file_name("settings.json");
    let serialized = match serde_json::to_string_pretty(&body.0) {
        Ok(v) => v,
        Err(e) => {
            return build_error_response(
                StatusCode::BAD_REQUEST,
                &e.to_string(),
                "invalid_settings",
            )
        }
    };
    if let Err(e) = std::fs::write(path, serialized) {
        return build_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "write_failed",
        );
    }
    (StatusCode::OK, axum::Json(json!({"ok": true}))).into_response()
}

pub async fn sync_pricing(State(state): State<Arc<AppState>>) -> Response {
    let pricing_map = match reqwest::get(LITELLM_PRICING_URL).await {
        Ok(resp) => match resp.json::<HashMap<String, LiteLlmPricingEntry>>().await {
            Ok(json) => json,
            Err(e) => {
                return build_error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("invalid pricing JSON: {}", e),
                    "pricing_sync_error",
                )
            }
        },
        Err(e) => {
            return build_error_response(
                StatusCode::BAD_GATEWAY,
                &format!("failed to fetch pricing: {}", e),
                "pricing_sync_error",
            )
        }
    };

    let mut updated = 0usize;
    let mut config = state.config.lock().await;
    for (_provider_name, provider_config) in config.providers.iter_mut() {
        for model in provider_config.models.iter_mut() {
            let direct = pricing_map.get(&model.id);
            let normalized = normalize_model_key(&model.id);
            let alias = pricing_map.get(&normalized).or_else(|| {
                pricing_map.iter().find_map(|(k, v)| {
                    if normalize_model_key(k) == normalized {
                        Some(v)
                    } else {
                        None
                    }
                })
            });
            if let Some(entry) = direct.or(alias) {
                if let Some(v) = entry.input_cost_per_token {
                    model.cost.input = v;
                }
                if let Some(v) = entry.output_cost_per_token {
                    model.cost.output = v;
                }
                if let Some(v) = entry.max_input_tokens {
                    model.context_window = v;
                }
                if let Some(v) = entry.max_output_tokens {
                    model.max_tokens = v;
                }
                updated += 1;
            }
        }
    }

    let serialized = match serde_json::to_string_pretty(&*config) {
        Ok(v) => v,
        Err(e) => {
            return build_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &e.to_string(),
                "write_failed",
            )
        }
    };
    if let Some(parent) = state.config_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::write(&state.config_path, serialized) {
        return build_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "write_failed",
        );
    }

    (
        StatusCode::OK,
        axum::Json(json!({"ok": true, "updated_models": updated, "source": LITELLM_PRICING_URL})),
    )
        .into_response()
}

pub async fn web_ui() -> impl IntoResponse {
    let html = include_str!("../../web/index.html");
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html)
}
