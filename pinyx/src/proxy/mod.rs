use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use reqwest::Client;
use serde_json::Value;
use std::pin::Pin;
use tokio_stream::Stream;
use tracing::{debug, info};

use crate::config::ProviderConfig;

pub type BoxStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

pub struct AdapterResponse {
    pub status: u16,
    pub stream: bool,
    pub body: BoxStream,
}

impl std::fmt::Debug for AdapterResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterResponse")
            .field("status", &self.status)
            .field("stream", &self.stream)
            .field("body", &"<stream>")
            .finish()
    }
}

#[async_trait]
pub trait ProviderAdapter: Send + Sync {
    fn name(&self) -> &str;
    async fn proxy(
        &self,
        model: &str,
        body: Value,
        api_key: &str,
    ) -> Result<AdapterResponse, String>;
}

pub struct OpenAIAdapter {
    client: Client,
    config: ProviderConfig,
}

impl OpenAIAdapter {
    pub fn new(config: ProviderConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }
}

#[async_trait]
impl ProviderAdapter for OpenAIAdapter {
    fn name(&self) -> &str {
        "openai"
    }

    async fn proxy(
        &self,
        model: &str,
        body: Value,
        api_key: &str,
    ) -> Result<AdapterResponse, String> {
        let url = format!(
            "{}/chat/completions",
            self.config.base_url.trim_end_matches('/')
        );

        let mut body = body;
        if let Value::Object(ref mut map) = body {
            map.insert("model".to_string(), Value::String(model.to_string()));
        }

        debug!(provider = "openai", model, url, "proxying request");

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;

        let status = response.status().as_u16();
        let is_stream = body
            .get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if is_stream {
            let stream = response.bytes_stream().map(|result| result);
            Ok(AdapterResponse {
                status,
                stream: true,
                body: Box::pin(stream),
            })
        } else {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| format!("read body failed: {}", e))?;
            let stream = tokio_stream::once(Ok(bytes));
            Ok(AdapterResponse {
                status,
                stream: false,
                body: Box::pin(stream),
            })
        }
    }
}

pub struct AnthropicAdapter {
    client: Client,
    config: ProviderConfig,
}

impl AnthropicAdapter {
    pub fn new(config: ProviderConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }
}

#[async_trait]
impl ProviderAdapter for AnthropicAdapter {
    fn name(&self) -> &str {
        "anthropic"
    }

    async fn proxy(
        &self,
        model: &str,
        body: Value,
        api_key: &str,
    ) -> Result<AdapterResponse, String> {
        let url = format!("{}/v1/messages", self.config.base_url.trim_end_matches('/'));

        let mut body = body;
        if let Value::Object(ref mut map) = body {
            map.insert("model".to_string(), Value::String(model.to_string()));
        }

        debug!(provider = "anthropic", model, url, "proxying request");

        let is_stream = body
            .get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut builder = self
            .client
            .post(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .json(&body);

        if is_stream {
            builder = builder.header("Accept", "text/event-stream");
        }

        let response = builder
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;

        let status = response.status().as_u16();

        if is_stream {
            let stream = response.bytes_stream();
            Ok(AdapterResponse {
                status,
                stream: true,
                body: Box::pin(stream),
            })
        } else {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| format!("read body failed: {}", e))?;
            let stream = tokio_stream::once(Ok(bytes));
            Ok(AdapterResponse {
                status,
                stream: false,
                body: Box::pin(stream),
            })
        }
    }
}

pub struct GoogleAdapter {
    client: Client,
    config: ProviderConfig,
}

impl GoogleAdapter {
    pub fn new(config: ProviderConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }
}

#[async_trait]
impl ProviderAdapter for GoogleAdapter {
    fn name(&self) -> &str {
        "google"
    }

    async fn proxy(
        &self,
        model: &str,
        body: Value,
        api_key: &str,
    ) -> Result<AdapterResponse, String> {
        let url = format!(
            "{}/models/{}:streamGenerateContent?alt=sse&key={}",
            self.config.base_url.trim_end_matches('/'),
            model,
            api_key
        );

        debug!(provider = "google", model, "proxying request");

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;

        let status = response.status().as_u16();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| format!("read body failed: {}", e))?;

        let stream = tokio_stream::once(Ok(bytes));
        Ok(AdapterResponse {
            status,
            stream: false,
            body: Box::pin(stream),
        })
    }
}

pub fn create_adapter(config: &ProviderConfig) -> Box<dyn ProviderAdapter> {
    match config.api.as_str() {
        "anthropic-messages" => Box::new(AnthropicAdapter::new(config.clone())),
        "openai-completions" => Box::new(OpenAIAdapter::new(config.clone())),
        "google-generative-ai" => Box::new(GoogleAdapter::new(config.clone())),
        other => {
            info!(
                api = other,
                "using openai-completions adapter for unknown api"
            );
            Box::new(OpenAIAdapter::new(config.clone()))
        }
    }
}
