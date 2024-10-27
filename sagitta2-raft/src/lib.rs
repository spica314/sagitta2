use log::{debug, info};
use rand::prelude::*;
use sagitta2_raft::raft_client::RaftClient;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use sagitta2_raft::raft_server::{Raft, RaftServer};
use sagitta2_raft::{
    AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
};

pub mod sagitta2_raft {
    tonic::include_proto!("sagitta2_raft"); // The string specified here must match the proto package name
}

#[derive(Debug, Clone)]
pub struct RaftState {
    id: i64,
    other_servers: Vec<(i64, String)>,

    inner: Arc<Mutex<RaftStateInner>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
struct RaftStateInner {
    role: RaftRole,
    last_heartbeat: std::time::Instant,

    current_term: i64,
    voted_for: Option<i64>,
    log: BTreeMap<i64, (i64, Vec<u8>)>,
    commit_index: i64,
    #[allow(dead_code)]
    last_applied: i64,
    #[allow(dead_code)]
    next_index: BTreeMap<i64, i64>,
    #[allow(dead_code)]
    match_index: BTreeMap<i64, i64>,
}

impl RaftState {
    pub async fn new(id: i64, other_servers: Vec<(i64, String)>) -> RaftState {
        // let mut other_servers2 = vec![];
        // for (id, addr) in other_servers {
        //     let addr: String = addr.parse().unwrap();
        //     let client = RaftClient::connect(addr.clone()).await.unwrap();
        //     let client = Arc::new(Mutex::new(client));
        //     other_servers2.push((id, addr, client));
        // }
        RaftState {
            id,
            // other_servers: other_servers2,
            other_servers,

            inner: Arc::new(Mutex::new(RaftStateInner {
                role: RaftRole::Follower,
                last_heartbeat: std::time::Instant::now(),

                current_term: 1,
                voted_for: None,
                log: BTreeMap::new(),
                commit_index: 0,
                last_applied: 0,
                next_index: BTreeMap::new(),
                match_index: BTreeMap::new(),
            })),
        }
    }

    pub async fn run(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let state = self.clone();
        let mut rng = thread_rng();
        let mut rng2 = StdRng::from_rng(&mut rng).unwrap();
        tokio::spawn(async move {
            loop {
                let wait_time = rng2.gen_range(150..300);
                tokio::time::sleep(tokio::time::Duration::from_millis(wait_time)).await;

                let (role, current_term, _, last_heartbeat) = {
                    let inner = state.inner.lock().await;
                    (
                        inner.role.clone(),
                        inner.current_term,
                        inner.voted_for,
                        inner.last_heartbeat,
                    )
                };

                debug!(
                    "Current term: {}, role = {:?}, last_heartbeat = {:?}",
                    current_term, role, last_heartbeat
                );

                if role == RaftRole::Follower
                    && std::time::Instant::now() - last_heartbeat
                        > std::time::Duration::from_millis(1000)
                {
                    let (current_term, last_log_index, last_log_term) = {
                        let mut inner = state.inner.lock().await;
                        inner.role = RaftRole::Candidate;
                        inner.current_term += 1;
                        inner.voted_for = Some(state.id);
                        inner.last_heartbeat = std::time::Instant::now();
                        let current_term = inner.current_term;
                        let last_log_index = inner.log.last_key_value().map(|t| *t.0).unwrap_or(0);
                        let last_log_term = inner.log.last_key_value().map(|t| t.1 .0).unwrap_or(0);
                        (current_term, last_log_index, last_log_term)
                    };

                    let mut votes = 1;
                    let mut replies = HashMap::new();
                    for (id, addr) in state.other_servers.iter() {
                        if let Ok(mut client) = RaftClient::connect(addr.clone()).await {
                            let request = RequestVoteRequest {
                                term: current_term,
                                candidate_id: state.id,
                                last_log_index,
                                last_log_term,
                            };
                            if let Ok(reply) = client.request_vote(request).await {
                                let reply = reply.into_inner();
                                replies.insert(*id, reply);
                                if reply.vote_granted {
                                    votes += 1;
                                }
                            }
                        }
                    }

                    if votes > state.other_servers.len() / 2 {
                        let mut inner = state.inner.lock().await;
                        if inner.current_term == current_term {
                            inner.role = RaftRole::Leader;
                            info!("id {} is now the leader", state.id);
                        } else {
                            inner.role = RaftRole::Follower;
                        }
                    } else {
                        let mut inner = state.inner.lock().await;
                        inner.role = RaftRole::Follower;
                    }

                    continue;
                }

                if role == RaftRole::Leader {
                    let (current_term, last_log_index, last_log_term, commit_index) = {
                        let mut inner = state.inner.lock().await;
                        inner.last_heartbeat = std::time::Instant::now();
                        let current_term = inner.current_term;
                        let last_log_index = inner.log.last_key_value().map(|t| *t.0).unwrap_or(0);
                        let last_log_term = inner.log.last_key_value().map(|t| t.1 .0).unwrap_or(0);
                        let commit_index = inner.commit_index;
                        (current_term, last_log_index, last_log_term, commit_index)
                    };
                    for (_, addr) in state.other_servers.iter() {
                        if let Ok(mut client) = RaftClient::connect(addr.clone()).await {
                            let request = AppendEntriesRequest {
                                term: current_term,
                                leader_id: state.id,
                                prev_log_index: last_log_index,
                                prev_log_term: last_log_term,
                                entries: vec![],
                                leader_commit: commit_index,
                            };
                            if let Ok(reply) = client.append_entries(request).await {
                                let reply = reply.into_inner();
                                if reply.term > current_term {
                                    let mut inner = state.inner.lock().await;
                                    inner.role = RaftRole::Follower;
                                    inner.current_term = reply.term;
                                    inner.voted_for = Some(state.id);
                                    inner.last_heartbeat = std::time::Instant::now();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Server::builder()
            .timeout(Duration::from_secs(5))
            .add_service(RaftServer::new(self.clone()))
            .serve(addr)
            .await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl Raft for RaftState {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<AppendEntriesReply>, Status> {
        // Return an instance of type HelloReply
        debug!("append_entries: Got a request: {:?}", request);

        let mut inner = self.inner.lock().await;

        // 1. Reply false if term < currentTerm (§5.1)
        if request.get_ref().term < inner.current_term {
            let reply = AppendEntriesReply {
                term: inner.current_term,
                success: false,
            };
            debug!("append_entries: Replying {:?}", reply);
            return Ok(Response::new(reply));
        }

        // 2. Reply false if log doesn’t contain an entry at prevLogIndex
        //    whose term matches prevLogTerm (§5.3)
        // (skip)

        // 3. If an existing entry conflicts with a new one (same index
        //    but different terms), delete the existing entry and all that
        //    follow it (§5.3)
        // (skip)

        // 4. Append any new entries not already in the log
        // (skip)

        // 5. If leaderCommit > commitIndex, set commitIndex =
        //    min(leaderCommit, index of last new entry)
        // (skip)

        inner.last_heartbeat = std::time::Instant::now();
        inner.current_term = request.get_ref().term;
        inner.voted_for = Some(request.get_ref().leader_id);

        let reply = AppendEntriesReply {
            term: inner.current_term,
            success: true,
        };
        debug!("append_entries: Replying {:?}", reply);
        Ok(Response::new(reply)) // Send back our formatted greeting
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<RequestVoteReply>, Status> {
        // Return an instance of type HelloReply
        debug!("request_vote: Got a request: {:?}", request);

        let mut inner = self.inner.lock().await;

        // 1. Reply false if term < currentTerm (§5.1)
        if request.get_ref().term < inner.current_term {
            let res = Response::new(RequestVoteReply {
                term: inner.current_term,
                vote_granted: false,
            });
            debug!("request_vote: Replying {:?}", res);
            return Ok(res);
        }

        // 2. If votedFor is null or candidateId,
        //    and candidate’s log is at least as up-to-date as receiver’s log,
        //    grant vote (§5.2, §5.4)
        if request.get_ref().term > inner.current_term {
            inner.current_term = request.get_ref().term;
            inner.voted_for = Some(request.get_ref().candidate_id);
            inner.role = RaftRole::Follower;
        }
        if inner.voted_for.is_none() || inner.voted_for == Some(request.get_ref().candidate_id) {
            let (last_index, last_term) = inner
                .log
                .last_key_value()
                .map(|t| (*t.0, t.1 .0))
                .unwrap_or((0, 0));
            if request.get_ref().last_log_index >= last_index
                && request.get_ref().last_log_term >= last_term
            {
                inner.voted_for = Some(request.get_ref().candidate_id);
                inner.current_term = request.get_ref().term;
                inner.last_heartbeat = std::time::Instant::now();
                let res = Response::new(RequestVoteReply {
                    term: inner.current_term,
                    vote_granted: true,
                });
                debug!("request_vote: Replying {:?}", res);
                return Ok(res);
            }
        }

        let res = Response::new(RequestVoteReply {
            term: inner.current_term,
            vote_granted: false,
        });
        debug!("request_vote: Replying {:?}", res);
        Ok(res)
    }
}
