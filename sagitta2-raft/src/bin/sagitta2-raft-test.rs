// RUST_LOG=info cargo run -- 1 'http://[::1]:45002' 'http://[::1]:45003'
// RUST_LOG=info cargo run -- 2 'http://[::1]:45001' 'http://[::1]:45003'
// RUST_LOG=info cargo run -- 3 'http://[::1]:45001' 'http://[::1]:45002'

use rand::prelude::*;
use sagitta2_raft::RaftState;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .init();

    let args: Vec<_> = std::env::args().collect();
    let id = args[1].parse::<i64>().unwrap();
    let other_servers = args[2..]
        .iter()
        .map(|s| {
            let id = s.chars().last().unwrap().to_digit(10).unwrap() as i64;
            (id, s.to_string())
        })
        .collect();

    let raft_state = RaftState::new(id, other_servers).await;

    {
        let raft_state = raft_state.clone();
        tokio::spawn(async move {
            let addr = format!("[::1]:4500{}", id).parse().unwrap();
            raft_state.run(addr).await.unwrap();
        });
    }

    let mut rng = thread_rng();
    loop {
        let wait_time = rng.gen_range(100..200);
        tokio::time::sleep(std::time::Duration::from_millis(wait_time)).await;

        let r = raft_state.append_log(vec![vec![rng.gen::<u8>(); 10]]).await;
        if r.is_err() {
            info!("append log failed: {:?}", r.err());
        }
    }
}
