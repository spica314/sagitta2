use std::collections::BTreeMap;

use crate::RaftRole;

#[derive(Debug, Clone)]
pub struct RaftStateInner {
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

impl Default for RaftStateInner {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftStateInner {
    pub fn new() -> RaftStateInner {
        RaftStateInner {
            role: RaftRole::Follower,
            last_heartbeat: std::time::Instant::now(),

            current_term: 0,
            voted_for: None,
            log: BTreeMap::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
        }
    }

    pub fn update_current_term(&mut self, term: i64, voted_for: Option<i64>) {
        self.current_term = term;
        self.voted_for = voted_for;
        self.last_heartbeat = std::time::Instant::now();
    }

    pub fn voted_for(&self) -> Option<i64> {
        self.voted_for
    }

    pub fn current_term(&self) -> i64 {
        self.current_term
    }

    pub fn role(&self) -> RaftRole {
        self.role
    }

    pub fn set_role(&mut self, role: RaftRole) {
        self.role = role;
    }

    pub fn last_heartbeat(&self) -> std::time::Instant {
        self.last_heartbeat
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = std::time::Instant::now();
    }

    pub fn last_log_index(&self) -> i64 {
        self.log.keys().last().cloned().unwrap_or(0)
    }

    pub fn last_log_term(&self) -> i64 {
        self.log.values().last().map(|(term, _)| *term).unwrap_or(0)
    }

    pub fn log(&self) -> &BTreeMap<i64, (i64, Vec<u8>)> {
        &self.log
    }

    pub fn commit_index(&self) -> i64 {
        self.commit_index
    }
}
