pub mod raft_state_inner;

use rand::prelude::*;
use sagitta2_raft::raft_client::RaftClient;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use sagitta2_raft::raft_server::{Raft, RaftServer};
use sagitta2_raft::{
    AppendEntriesReply, AppendEntriesRequest, LogEntry, RequestVoteReply, RequestVoteRequest,
};

pub mod sagitta2_raft {
    tonic::include_proto!("sagitta2_raft"); // The string specified here must match the proto package name
}

use raft_state_inner::RaftStateInner;

#[derive(Debug, Clone)]
pub struct RaftState {
    id: i64,
    other_servers: Vec<String>,

    inner: Arc<Mutex<RaftStateInner>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub enum RaftError {
    NotLeader,
    Unknown,
}

#[derive(Debug, Clone)]
pub enum AppendEntriesWithIndexError {
    NoEntry,
    NoPrevEntry,
    Unknown,
    NotLeader,
}

impl RaftState {
    pub async fn new(id: i64, other_servers: Vec<String>) -> RaftState {
        let rng1 = thread_rng();
        let rng2 = StdRng::from_rng(rng1).unwrap();

        RaftState {
            id,
            other_servers,

            inner: Arc::new(Mutex::new(RaftStateInner::new(rng2))),
        }
    }

    pub async fn is_leader(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.role() == RaftRole::Leader
    }

    pub async fn current_term(&self) -> i64 {
        let inner = self.inner.lock().await;
        inner.current_term()
    }

    pub async fn commit_index(&self) -> i64 {
        let inner = self.inner.lock().await;
        inner.commit_index()
    }

    pub async fn append_entries_with_index_and_backfill_entries(
        &self,
        client: &mut RaftClient<tonic::transport::Channel>,
        begin_index: i64,
        end_index: i64,
        current_term: i64,
        leader_commit: i64,
    ) -> Result<Result<AppendEntriesReply, Status>, AppendEntriesWithIndexError> {
        let r = self
            .append_entries_with_index(client, begin_index, end_index, current_term, leader_commit)
            .await;

        if let Err(e) = r {
            error!("append_log: failed to send append entries: {:?}", e);
            return Err(e);
        }

        let r = r.unwrap();

        if let Err(e) = r {
            error!("append_log: failed to send append entries: {:?}", e);
            return Err(AppendEntriesWithIndexError::Unknown);
        }

        let reply = r.unwrap();
        if reply.term > current_term {
            let mut inner = self.inner.lock().await;
            inner.set_role(RaftRole::Follower);
            inner.update_current_term(reply.term, Some(self.id));
            return Err(AppendEntriesWithIndexError::NotLeader);
        }

        if reply.success {
            return Ok(Ok(reply));
        }

        let mut ok = true;
        let mut begin = None;
        for index in (0..begin_index).rev() {
            let r = self
                .append_entries_with_index(client, index, index + 1, current_term, leader_commit)
                .await;

            if let Err(e) = r {
                error!("append_log: failed to send append entries: {:?}", e);
                ok = false;
                break;
            }

            let r = r.unwrap();

            if let Err(e) = r {
                error!("append_log: failed to send append entries: {:?}", e);
                ok = false;
                break;
            }

            let reply = r.unwrap();

            if reply.term > current_term {
                let mut inner = self.inner.lock().await;
                inner.set_role(RaftRole::Follower);
                inner.update_current_term(reply.term, Some(self.id));
                return Err(AppendEntriesWithIndexError::NotLeader);
            }

            if reply.success {
                ok = true;
                begin = Some(index);
                break;
            }
        }

        if !ok {
            return Err(AppendEntriesWithIndexError::Unknown);
        }

        let begin = begin.unwrap();
        let mut ok2 = true;
        for index in begin + 1..begin_index {
            let r = self
                .append_entries_with_index(client, index, index + 1, current_term, leader_commit)
                .await;

            if let Err(e) = r {
                error!("append_log: failed to send append entries: {:?}", e);
                ok2 = false;
                break;
            }

            let r = r.unwrap();

            if let Err(e) = r {
                error!("append_log: failed to send append entries: {:?}", e);
                ok2 = false;
                break;
            }

            let reply = r.unwrap();

            if reply.term > current_term {
                let mut inner = self.inner.lock().await;
                inner.set_role(RaftRole::Follower);
                inner.update_current_term(reply.term, Some(self.id));
                ok2 = false;
                break;
            }

            if !reply.success {
                ok2 = false;
                break;
            }
        }

        if !ok2 {
            return Err(AppendEntriesWithIndexError::Unknown);
        }

        let r = self
            .append_entries_with_index(client, begin_index, end_index, current_term, leader_commit)
            .await;

        if let Err(e) = r {
            error!("append_log: failed to send append entries: {:?}", e);
            return Err(e);
        }

        let r = r.unwrap();

        if let Err(e) = r {
            error!("append_log: failed to send append entries: {:?}", e);
            return Err(AppendEntriesWithIndexError::Unknown);
        }

        let reply = r.unwrap();
        if reply.term > current_term {
            let mut inner = self.inner.lock().await;
            inner.set_role(RaftRole::Follower);
            inner.update_current_term(reply.term, Some(self.id));
            return Err(AppendEntriesWithIndexError::NotLeader);
        }

        Ok(Ok(reply))
    }

    pub async fn append_entries_with_index(
        &self,
        client: &mut RaftClient<tonic::transport::Channel>,
        begin_index: i64,
        end_index: i64,
        current_term: i64,
        leader_commit: i64,
    ) -> Result<Result<AppendEntriesReply, Status>, AppendEntriesWithIndexError> {
        let (prev_log_index, prev_log_term) = {
            let inner = self.inner.lock().await;
            if begin_index == 1 {
                (0, 0)
            } else {
                let prev = inner.log().get(&(begin_index - 1));
                if prev.is_none() {
                    return Err(AppendEntriesWithIndexError::NoPrevEntry);
                }
                let (prev_term, _) = prev.unwrap();
                (begin_index - 1, *prev_term)
            }
        };

        let mut entries = vec![];
        {
            let inner = self.inner.lock().await;
            for i in begin_index..end_index {
                let (term, entry) = {
                    let t = inner
                        .log()
                        .get(&i)
                        .map(|(term, entry)| (*term, entry.clone()));
                    if t.is_none() {
                        return Err(AppendEntriesWithIndexError::NoEntry);
                    }
                    t.unwrap()
                };
                entries.push(LogEntry {
                    index: i,
                    term,
                    command: entry,
                });
            }
        }

        let request = AppendEntriesRequest {
            term: current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        };
        if let Ok(reply) = client.append_entries(request).await {
            return Ok(Ok(reply.into_inner()));
        }

        Ok(Err(Status::internal("Failed to send append entries")))
    }

    pub async fn append_log(&self, entries: Vec<Vec<u8>>) -> Result<(), RaftError> {
        // leader check
        let (current_term, prev_log_index, commit_index) = {
            let mut inner = self.inner.lock().await;
            if inner.role() != RaftRole::Leader {
                return Err(RaftError::NotLeader);
            }

            let prev_log_index = inner.last_log_index();
            let prev_log_term = inner.last_log_term();
            let current_term = inner.current_term();
            debug!(
                "append_log: prev_log_index = {}, prev_log_term = {}",
                prev_log_index, prev_log_term
            );

            let mut entries2 = vec![];
            for entry in &entries {
                entries2.push((current_term, entry.clone()));
            }

            let ok = inner.append_log(prev_log_index, prev_log_term, entries2);
            if !ok {
                error!("append_log: failed to append log");
                return Err(RaftError::Unknown);
            }

            let commit_index = inner.commit_index();

            (current_term, prev_log_index, commit_index)
        };

        let mut vote = 1;
        for server_addr in &self.other_servers {
            if let Ok(mut client) = RaftClient::connect(server_addr.clone()).await {
                let r = self
                    .append_entries_with_index_and_backfill_entries(
                        &mut client,
                        prev_log_index + 1,
                        prev_log_index + 1 + entries.len() as i64,
                        current_term,
                        commit_index,
                    )
                    .await;

                if let Err(e) = r {
                    error!("append_log: failed to send append entries: {:?}", e);
                    continue;
                }

                let r = r.unwrap();

                if let Err(e) = r {
                    error!("append_log: failed to send append entries: {:?}", e);
                    continue;
                }

                if let Ok(reply) = r {
                    if reply.success {
                        vote += 1;
                    }
                }
            }
        }

        if vote > self.other_servers.len() / 2 {
            let mut inner = self.inner.lock().await;
            inner.update_commit_index(prev_log_index + entries.len() as i64);
            info!("commit index = {}", inner.commit_index());
        } else {
            error!("append_log: failed to get majority votes");
            let mut inner = self.inner.lock().await;
            inner.set_role(RaftRole::Follower);
            return Err(RaftError::Unknown);
        }

        Ok(())
    }

    pub async fn heartbeat_process(&self) {
        let wait_time = {
            let mut inner = self.inner.lock().await;
            inner.rng_mut().gen_range(150..300)
        };
        tokio::time::sleep(tokio::time::Duration::from_millis(wait_time)).await;

        let (role, current_term, _, last_heartbeat) = {
            let inner = self.inner.lock().await;
            (
                inner.role(),
                inner.current_term(),
                inner.voted_for(),
                inner.last_heartbeat(),
            )
        };

        debug!(
            "Current term: {}, role = {:?}, last_heartbeat = {:?}",
            current_term, role, last_heartbeat
        );

        if role == RaftRole::Follower
            && std::time::Instant::now() - last_heartbeat > std::time::Duration::from_millis(1000)
        {
            let (current_term, last_log_index, last_log_term) = {
                let mut inner = self.inner.lock().await;
                inner.set_role(RaftRole::Candidate);
                let next_term = inner.current_term() + 1;
                inner
                    .deref_mut()
                    .update_current_term(next_term, Some(self.id));
                let current_term = inner.current_term();
                let last_log_index = inner.last_log_index();
                let last_log_term = inner.last_log_term();
                (current_term, last_log_index, last_log_term)
            };

            let mut votes = 1;
            let mut replies = HashMap::new();
            for addr in self.other_servers.iter() {
                if let Ok(mut client) = RaftClient::connect(addr.clone()).await {
                    let request = RequestVoteRequest {
                        term: current_term,
                        candidate_id: self.id,
                        last_log_index,
                        last_log_term,
                    };
                    if let Ok(reply) = client.request_vote(request).await {
                        let reply = reply.into_inner();
                        replies.insert(addr, reply);
                        if reply.vote_granted {
                            votes += 1;
                        }
                    }
                }
            }

            if votes > self.other_servers.len() / 2 {
                let mut inner = self.inner.lock().await;
                if inner.current_term() == current_term {
                    inner.set_role(RaftRole::Leader);
                    info!("id {} is now the leader", self.id);
                } else {
                    inner.set_role(RaftRole::Follower);
                }
            } else {
                let mut inner = self.inner.lock().await;
                inner.set_role(RaftRole::Follower);
            }

            return;
        }

        if role == RaftRole::Leader {
            let (current_term, last_log_index, last_log_term, commit_index) = {
                let mut inner = self.inner.lock().await;
                inner.update_heartbeat();
                let current_term = inner.current_term();
                let last_log_index = inner.last_log_index();
                let last_log_term = inner.last_log_term();
                let commit_index = inner.commit_index();
                (current_term, last_log_index, last_log_term, commit_index)
            };
            for addr in self.other_servers.iter() {
                if let Ok(mut client) = RaftClient::connect(addr.clone()).await {
                    let request = AppendEntriesRequest {
                        term: current_term,
                        leader_id: self.id,
                        prev_log_index: last_log_index,
                        prev_log_term: last_log_term,
                        entries: vec![],
                        leader_commit: commit_index,
                    };
                    if let Ok(reply) = client.append_entries(request).await {
                        let reply = reply.into_inner();
                        if reply.term > current_term {
                            let mut inner = self.inner.lock().await;
                            inner.set_role(RaftRole::Follower);
                            inner.update_current_term(reply.term, Some(self.id));
                            break;
                        }
                    }
                }
            }
        }
    }

    pub async fn run(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let state = self.clone();
        tokio::spawn(async move {
            loop {
                let state = state.clone();
                let handler = tokio::spawn(async move {
                    loop {
                        state.heartbeat_process().await;
                    }
                });

                let r = handler.await;
                if let Err(e) = r {
                    error!("Error: {:?}", e);
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
        if request.get_ref().term < inner.current_term() {
            let reply = AppendEntriesReply {
                term: inner.current_term(),
                success: false,
            };
            debug!("append_entries: Replying {:?}", reply);
            warn!("append_entries: term < current_term");
            return Ok(Response::new(reply));
        }

        // 2. Reply false if log doesn’t contain an entry at prevLogIndex
        //    whose term matches prevLogTerm (§5.3)
        if !inner.check_term(
            request.get_ref().prev_log_index,
            request.get_ref().prev_log_term,
        ) {
            let reply = AppendEntriesReply {
                term: inner.current_term(),
                success: false,
            };
            debug!("append_entries: Replying {:?}", reply);
            warn!("append_entries: check_term failed");
            return Ok(Response::new(reply));
        }

        // 3. If an existing entry conflicts with a new one (same index
        //    but different terms), delete the existing entry and all that
        //    follow it (§5.3)
        inner.delete_logs_after(request.get_ref().prev_log_index);

        // 4. Append any new entries not already in the log
        inner.append_log(
            request.get_ref().prev_log_index,
            request.get_ref().prev_log_term,
            request
                .get_ref()
                .entries
                .iter()
                .map(|e| (e.term, e.command.clone()))
                .collect(),
        );

        // 5. If leaderCommit > commitIndex, set commitIndex =
        //    min(leaderCommit, index of last new entry)
        if request.get_ref().leader_commit > inner.commit_index() {
            let last_index = inner.last_log_index();
            inner.update_commit_index(std::cmp::min(request.get_ref().leader_commit, last_index));
        }

        inner.update_current_term(request.get_ref().term, Some(request.get_ref().leader_id));
        if inner.current_term() < request.get_ref().term {
            inner.set_current_term(request.get_ref().term);
        }

        let reply = AppendEntriesReply {
            term: inner.current_term(),
            success: true,
        };
        debug!("append_entries: Replying {:?}", reply);
        Ok(Response::new(reply))
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<RequestVoteReply>, Status> {
        // Return an instance of type HelloReply
        debug!("request_vote: Got a request: {:?}", request);

        let mut inner = self.inner.lock().await;

        // 1. Reply false if term < currentTerm (§5.1)
        if request.get_ref().term < inner.current_term() {
            let res = Response::new(RequestVoteReply {
                term: inner.current_term(),
                vote_granted: false,
            });
            debug!("request_vote: Replying {:?}", res);
            warn!("request_vote: term < current_term");
            return Ok(res);
        }

        // 2. If votedFor is null or candidateId,
        //    and candidate’s log is at least as up-to-date as receiver’s log,
        //    grant vote (§5.2, §5.4)
        if request.get_ref().term > inner.current_term() {
            inner.update_current_term(request.get_ref().term, Some(request.get_ref().candidate_id));
            inner.set_role(RaftRole::Follower);
        }
        if inner.voted_for().is_none() || inner.voted_for() == Some(request.get_ref().candidate_id)
        {
            let (last_index, last_term) = inner
                .log()
                .last_key_value()
                .map(|t| (*t.0, t.1 .0))
                .unwrap_or((0, 0));
            if request.get_ref().last_log_index >= last_index
                && request.get_ref().last_log_term >= last_term
            {
                inner.update_current_term(
                    request.get_ref().term,
                    Some(request.get_ref().candidate_id),
                );
                let res = Response::new(RequestVoteReply {
                    term: inner.current_term(),
                    vote_granted: true,
                });
                debug!("request_vote: Replying {:?}", res);
                return Ok(res);
            }
        }

        let res = Response::new(RequestVoteReply {
            term: inner.current_term(),
            vote_granted: false,
        });
        debug!("request_vote: Replying {:?}", res);
        warn!("request_vote: vote not granted");
        Ok(res)
    }
}
