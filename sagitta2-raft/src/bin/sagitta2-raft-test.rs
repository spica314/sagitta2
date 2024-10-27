// cargo run -- 1 'http://[::1]:45002' 'http://[::1]:45003'
// cargo run -- 2 'http://[::1]:45001' 'http://[::1]:45003'
// cargo run -- 3 'http://[::1]:45001' 'http://[::1]:45002'

use sagitta2_raft::RaftState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args: Vec<_> = std::env::args().collect();
    let id = args[1].parse::<i64>().unwrap();
    let other_servers = args[2..]
        .iter()
        .map(|s| {
            let id = s.chars().last().unwrap().to_digit(10).unwrap() as i64;
            (id, s.to_string())
        })
        .collect();

    let addr = format!("[::1]:4500{}", id).parse()?;
    let raft_state = RaftState::new(id, other_servers).await;

    raft_state.run(addr).await?;

    Ok(())
}
