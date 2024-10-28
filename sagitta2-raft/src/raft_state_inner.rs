use std::collections::BTreeMap;

use rand::rngs::StdRng;

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

    rng: StdRng,
}

impl RaftStateInner {
    pub fn new(rng: StdRng) -> RaftStateInner {
        let mut log = BTreeMap::new();
        log.insert(0, (0, vec![]));

        RaftStateInner {
            role: RaftRole::Follower,
            last_heartbeat: std::time::Instant::now(),

            current_term: 0,
            voted_for: None,
            log,
            commit_index: 0,
            last_applied: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),

            rng,
        }
    }

    pub fn update_commit_index(&mut self, commit_index: i64) {
        self.commit_index = commit_index;
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

    pub fn rng_mut(&mut self) -> &mut StdRng {
        &mut self.rng
    }

    pub fn append_log(
        &mut self,
        prev_log_index: i64,
        prev_log_term: i64,
        entries: Vec<(i64, Vec<u8>)>,
    ) -> bool {
        if let Some((term, _)) = self.log.get(&prev_log_index) {
            if *term == prev_log_term {
                for (i, (term, entry)) in entries.into_iter().enumerate() {
                    self.log
                        .insert(prev_log_index + i as i64 + 1, (term, entry));
                }
                return true;
            }
        }
        false
    }

    pub fn check_term(&self, index: i64, expected_term: i64) -> bool {
        self.log
            .get(&index)
            .map(|(term, _)| *term == expected_term)
            .unwrap_or(false)
    }

    pub fn delete_logs_after(&mut self, index: i64) {
        let keys: Vec<_> = self.log.keys().cloned().filter(|&k| k > index).collect();
        for k in keys {
            self.log.remove(&k);
        }
    }

    pub fn set_current_term(&mut self, term: i64) {
        self.current_term = term;
    }
}
