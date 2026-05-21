use std::path::PathBuf;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};

#[derive(Debug, Clone)]
pub struct RequestLogger {
    log_dir: PathBuf,
}

impl RequestLogger {
    pub fn new() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let log_dir = home.join(".pinyx").join("logs");
        Self { log_dir }
    }

    pub async fn log_request(
        &self,
        request_id: &str,
        provider: &str,
        model: &str,
        input_tokens: u64,
        output_tokens: u64,
        elapsed_ms: u64,
        status: u16,
    ) {
        let entry = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "request_id": request_id,
            "provider": provider,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "elapsed_ms": elapsed_ms,
            "status": status,
        });

        if let Err(e) = self.append_log(&entry).await {
            error!(error = %e, "failed to write request log");
        }
    }

    async fn append_log(&self, entry: &serde_json::Value) -> std::io::Result<()> {
        let _ = fs::create_dir_all(&self.log_dir).await;

        let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let log_file = self.log_dir.join(format!("{}.jsonl", date));

        let mut line = serde_json::to_string(entry).unwrap_or_default();
        line.push('\n');

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file)
            .await?;

        file.write_all(line.as_bytes()).await?;
        file.flush().await?;

        debug!(path = %log_file.display(), "wrote log entry");
        Ok(())
    }
}
