// RUST_LOG=info cargo run -- 1 'http://[::1]:45002' 'http://[::1]:45003'
// RUST_LOG=info cargo run -- 2 'http://[::1]:45001' 'http://[::1]:45003'
// RUST_LOG=info cargo run -- 3 'http://[::1]:45001' 'http://[::1]:45002'

use prometheus_client::metrics::gauge::Gauge;
use sagitta2_raft::RaftState;

use std::sync::Mutex;

use actix_web::middleware::Compress;
use actix_web::{web, App, HttpResponse, HttpServer, Result};
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;
use tracing_subscriber::EnvFilter;

pub struct Metrics {
    is_leader: Gauge,
    current_term: Gauge,
    commit_index: Gauge,
}

pub struct AppState {
    pub registry: Registry,
}

pub async fn metrics_handler(state: web::Data<Mutex<AppState>>) -> Result<HttpResponse> {
    let state = state.lock().unwrap();

    let mut body = String::new();
    encode(&mut body, &state.registry).unwrap();
    Ok(HttpResponse::Ok()
        .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
        .body(body))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .init();

    let args: Vec<_> = std::env::args().collect();
    let id = args[1].parse::<i64>().unwrap();
    let other_servers = args[2..].to_vec();

    let raft_state = RaftState::new(id, other_servers).await;

    {
        let raft_state = raft_state.clone();
        tokio::spawn(async move {
            let addr = format!("[::1]:4500{}", id).parse().unwrap();
            raft_state.run(addr).await.unwrap();
        });
    }

    let metrics = web::Data::new(Metrics {
        is_leader: Gauge::default(),
        current_term: Gauge::default(),
        commit_index: Gauge::default(),
    });
    let mut state = AppState {
        registry: Registry::default(),
    };
    state
        .registry
        .register("is_leader", "leader", metrics.is_leader.clone());
    state
        .registry
        .register("current_term", "term", metrics.current_term.clone());
    state
        .registry
        .register("commit_index", "index", metrics.commit_index.clone());
    let state = web::Data::new(Mutex::new(state));

    {
        let metrics = metrics.clone();
        let raft_state = raft_state.clone();
        tokio::spawn(async move {
            loop {
                let is_leader = raft_state.is_leader().await;
                let current_term = raft_state.current_term().await;
                let commit_index = raft_state.commit_index().await;
                metrics.is_leader.set(if is_leader { 1 } else { 0 });
                metrics.current_term.set(current_term);
                metrics.commit_index.set(commit_index);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    HttpServer::new(move || {
        App::new()
            .wrap(Compress::default())
            .app_data(metrics.clone())
            .app_data(state.clone())
            .service(web::resource("/metrics").route(web::get().to(metrics_handler)))
    })
    .bind(("127.0.0.1", (8080 + id) as u16))?
    .run()
    .await
}
