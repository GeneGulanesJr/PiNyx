use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_host")]
    pub host: String,
}

fn default_port() -> u16 {
    7331
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            host: default_host(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub api: String,
    #[serde(rename = "baseUrl")]
    pub base_url: String,
    #[serde(rename = "apiKey")]
    pub api_key: String,
    #[serde(default)]
    pub models: Vec<ModelConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub reasoning: bool,
    #[serde(rename = "contextWindow", default = "default_context_window")]
    pub context_window: u32,
    #[serde(rename = "maxTokens", default = "default_max_tokens")]
    pub max_tokens: u32,
    #[serde(default)]
    pub input: Vec<String>,
    #[serde(default)]
    pub cost: CostConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostConfig {
    #[serde(default)]
    pub input: f64,
    #[serde(default)]
    pub output: f64,
    #[serde(rename = "cacheRead", default)]
    pub cache_read: f64,
    #[serde(rename = "cacheWrite", default)]
    pub cache_write: f64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            input: 0.0,
            output: 0.0,
            cache_read: 0.0,
            cache_write: 0.0,
        }
    }
}

fn default_context_window() -> u32 {
    128000
}

fn default_max_tokens() -> u32 {
    16384
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub gateway: GatewayConfig,
    pub providers: HashMap<String, ProviderConfig>,
}

impl AppConfig {
    pub fn resolve_api_key(&self, key: &str) -> Option<String> {
        if key.starts_with('$') {
            std::env::var(&key[1..]).ok()
        } else {
            Some(key.to_string())
        }
    }

    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    pub fn default_path() -> std::path::PathBuf {
        let home = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
        home.join(".pinyx").join("pinyx.json")
    }
}
