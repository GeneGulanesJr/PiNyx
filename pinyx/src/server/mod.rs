use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::logging::RequestLogger;
use crate::proxy::{create_adapter, ProviderAdapter};

pub struct AppState {
    pub config: Arc<AppConfig>,
    pub adapters: HashMap<String, Box<dyn ProviderAdapter>>,
    pub api_keys: HashMap<String, String>,
    pub logger: RequestLogger,
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        let mut adapters = HashMap::new();
        let mut api_keys = HashMap::new();

        for (name, provider_config) in &config.providers {
            let adapter = create_adapter(provider_config);
            adapters.insert(name.clone(), adapter);

            match config.resolve_api_key(&provider_config.api_key) {
                Some(key) => {
                    info!(provider = name, "resolved API key");
                    api_keys.insert(name.clone(), key);
                }
                None => {
                    warn!(provider = name, key_ref = &provider_config.api_key, "could not resolve API key");
                }
            }
        }

        let logger = RequestLogger::new();

        Self {
            config: Arc::new(config),
            adapters,
            api_keys,
            logger,
        }
    }
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

fn find_provider_for_model(state: &AppState, model: &str) -> Option<(String, String)> {
    if let Some((provider, model_id)) = parse_model_id(model) {
        if state.adapters.contains_key(&provider) {
            return Some((provider, model_id));
        }
    }

    for (provider_name, provider_config) in &state.config.providers {
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
    let api_key = match state.api_keys.get(provider_name) {
        Some(key) => key.clone(),
        None => {
            return build_error_response(
                StatusCode::UNAUTHORIZED,
                &format!("No API key for provider: {}", provider_name),
                "authentication_error",
            );
        }
    };

    let adapter = match state.adapters.get(provider_name) {
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

                let mapped_stream = adapter_response.body.map(|chunk_result| {
                    match chunk_result {
                        Ok(chunk) => Ok(chunk),
                        Err(e) => {
                            error!(error = %e, "stream error");
                            Err(std::io::Error::new(std::io::ErrorKind::Other, e))
                        }
                    }
                });

                let body = Body::from_stream(mapped_stream);

                tokio::spawn(async move {
                    logger.log_request(&rid, &prov, &mdl, 0, 0, elapsed_ms, upstream_status).await;
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
                    logger.log_request(&rid, &prov, &mdl, 0, 0, elapsed_ms, upstream_status).await;
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
                .log_request(request_id, provider_name, model_id, 0, 0, elapsed.as_millis() as u64, 500)
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

    let (provider_name, model_id) = match find_provider_for_model(&state, &model) {
        Some(result) => result,
        None => {
            return build_error_response(
                StatusCode::BAD_REQUEST,
                &format!("No provider found for model: {}", model),
                "invalid_request_error",
            );
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

    if let Some((provider, model_id)) = find_provider_for_model(&state, &model) {
        return do_proxy(&state, &provider, &model_id, body, &request_id, start).await;
    }

    if state.adapters.contains_key("anthropic") {
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

    for (provider_name, provider_config) in &state.config.providers {
        for m in &provider_config.models {
            models.push(json!({
                "id": format!("{}/{}", provider_name, m.id),
                "object": "model",
                "created": 0,
                "owned_by": provider_name,
                "name": if m.name.is_empty() { &m.id } else { &m.name },
                "reasoning": m.reasoning,
                "input": m.input,
                "context_window": m.context_window,
                "max_tokens": m.max_tokens,
                "cost": {
                    "input": m.cost.input,
                    "output": m.cost.output,
                    "cache_read": m.cost.cache_read,
                    "cache_write": m.cost.cache_write
                }
            }));
        }
    }

    axum::Json(json!({
        "object": "list",
        "data": models
    }))
}

pub async fn health(State(state): State<Arc<AppState>>) -> axum::Json<Value> {
    let mut providers = serde_json::Map::new();
    for (name, _) in &state.config.providers {
        let has_key = state.api_keys.contains_key(name);
        providers.insert(
            name.clone(),
            json!({
                "status": if has_key { "configured" } else { "missing_key" },
                "api_key": has_key,
            }),
        );
    }

    axum::Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "providers": providers,
        "gateway": {
            "host": &state.config.gateway.host,
            "port": state.config.gateway.port,
        }
    }))
}
