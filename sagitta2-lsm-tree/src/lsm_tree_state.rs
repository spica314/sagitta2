use std::path::Path;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LSMTreeState {
    state_generation: u64,
    sstable_ids: Vec<Vec<u64>>,
    next_sstable_id: u64,
    wal_id: u64,
}

impl LSMTreeState {
    pub(crate) fn new() -> Self {
        LSMTreeState {
            state_generation: 0,
            sstable_ids: vec![],
            next_sstable_id: 1,
            wal_id: 0,
        }
    }

    pub(crate) async fn save<P: AsRef<Path>>(&self, path: P) {
        let mut file = File::create(path).await.unwrap();
        let state = serde_json::to_string(&self).unwrap();
        file.write_all(state.as_bytes()).await.unwrap();
    }

    pub(crate) async fn load<P: AsRef<Path>>(path: P) -> Self {
        let mut file = File::open(path).await.unwrap();
        let mut buf = vec![];
        file.read_to_end(&mut buf).await.unwrap();
        serde_json::from_slice(&buf).unwrap()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.state_generation
    }

    pub(crate) fn sstable_ids(&self) -> &Vec<Vec<u64>> {
        &self.sstable_ids
    }

    #[allow(dead_code)]
    pub(crate) fn gen_next_sstable_id(&mut self) -> u64 {
        let id = self.next_sstable_id;
        self.next_sstable_id += 1;
        id
    }

    #[allow(dead_code)]
    pub(crate) fn update_sstable(&mut self, old_level: usize, old_ids: Vec<u64>, new_id: u64) {
        // check
        assert!(old_ids
            .iter()
            .all(|id| self.sstable_ids[old_level].contains(id)));

        // extend sstables
        if old_level + 1 >= self.sstable_ids.len() {
            self.sstable_ids.push(vec![]);
        }

        // remove old_ids
        self.sstable_ids[old_level] = self.sstable_ids[old_level]
            .iter()
            .filter(|id| !old_ids.contains(id))
            .cloned()
            .collect();

        // add new_id
        self.sstable_ids[old_level + 1].push(new_id);

        // update generation
        self.state_generation += 1;
    }

    pub(crate) fn wal_id(&self) -> u64 {
        self.wal_id
    }
}
