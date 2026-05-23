mod config;
mod logging;
mod proxy;
mod server;

use axum::routing::{delete, get, post, put};
use axum::Router;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use config::AppConfig;
use server::AppState;

#[derive(Parser, Debug)]
#[command(name = "pinyx", about = "Model gateway for Pi coding agent", version)]
struct Args {
    #[arg(short, long, default_value = "")]
    config: String,

    #[arg(short, long, default_value_t = 0)]
    port: u16,

    #[arg(short, long)]
    host: Option<String>,

    #[arg(long)]
    check: bool,

    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let filter_level = if args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter_level)),
        )
        .json()
        .with_target(false)
        .init();

    let config_path = if args.config.is_empty() {
        AppConfig::default_path()
    } else {
        PathBuf::from(&args.config)
    };

    if !config_path.exists() {
        error!(path = %config_path.display(), "config file not found");
        eprintln!("Config file not found: {}", config_path.display());
        eprintln!("Create one at ~/.pinyx/pinyx.json");
        std::process::exit(1);
    }

    let mut config = match AppConfig::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to load config");
            eprintln!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    if args.check {
        info!("Config is valid");
        println!("Config is valid: {}", config_path.display());
        println!(
            "Providers: {}",
            config
                .providers
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
        return;
    }

    if args.port != 0 {
        config.gateway.port = args.port;
    }
    if let Some(host) = args.host {
        config.gateway.host = host;
    }

    info!(
        port = config.gateway.port,
        host = &config.gateway.host,
        "starting PiNyx gateway"
    );

    let addr = format!("{}:{}", config.gateway.host, config.gateway.port);
    let display_addr = addr.clone();
    let display_providers: Vec<String> = config.providers.keys().cloned().collect();

    let state = Arc::new(AppState::new(config, config_path.clone()));

    let app = Router::new()
        .route(
            "/v1/chat/completions",
            post(server::openai_chat_completions),
        )
        .route("/anthropic/v1/messages", post(server::anthropic_messages))
        .route("/v1/models", get(server::list_models))
        .route("/health", get(server::health))
        .route("/", get(server::web_ui))
        .route(
            "/api/config",
            get(server::get_config).put(server::put_config),
        )
        .route(
            "/api/settings",
            get(server::get_settings).put(server::put_settings),
        )
        .route("/api/pricing/sync", post(server::sync_pricing))
        .route("/api/keys", get(server::get_keys))
        .route("/api/keys/{provider}", put(server::put_key))
        .route("/api/providers", post(server::add_provider))
        .route("/api/providers/{provider}", delete(server::delete_provider))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, addr = &addr, "failed to bind");
            eprintln!("Failed to bind {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    info!(addr = &addr, "PiNyx gateway listening");

    println!("PiNyx gateway running on http://{}", display_addr);
    println!("Providers: {}", display_providers.join(", "));
    println!("Health: http://{}/health", display_addr);
    println!("Models: http://{}/v1/models", display_addr);
    println!("Onboarding UI: http://{}/", display_addr);

    axum::serve(listener, app).await.unwrap();
}
