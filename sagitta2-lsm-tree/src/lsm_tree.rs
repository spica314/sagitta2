use std::path::{Path, PathBuf};

use crate::{
    lsm_tree_config::LSMTreeConfig, lsm_tree_state::LSMTreeState, memtable::MemTable,
    sstable_reader::SSTableReader, wal_reader::WALReader, wal_writer::WALWriter, DataItem,
};

// directory structure:
// /path/to/lsm_tree/
//   state/
//     X.json
//   sstables/
//     0.sst
//     1.sst
//     2.sst
//     ...
//     N.sst
//   wal/
//     M.wal

#[derive(Debug)]
pub struct LSMTree {
    #[allow(dead_code)]
    config: LSMTreeConfig,
    path: PathBuf,
    state: LSMTreeState,
    memtable: MemTable,
    wal_writer: WALWriter,
}

impl LSMTree {
    pub async fn new<P: AsRef<Path>>(path: P, config: LSMTreeConfig) -> Self {
        let state = {
            let state_path = {
                let mut path = path.as_ref().to_path_buf();
                path.push("state");
                path
            };

            if !state_path.exists() {
                tokio::fs::create_dir_all(&state_path).await.unwrap();
            }

            let mut state_paths = vec![];
            let mut read_dir = tokio::fs::read_dir(&state_path).await.unwrap();
            while let Some(entry) = read_dir.next_entry().await.unwrap() {
                let path = entry.path();
                if path.is_file() && path.extension() == Some("json".as_ref()) {
                    let file_prefix = path.file_stem().unwrap().to_str().unwrap();
                    if let Ok(state_id) = file_prefix.parse::<u64>() {
                        state_paths.push((state_id, path));
                    }
                }
            }

            state_paths.sort_by_key(|(state_id, _)| *state_id);
            let latest_state_path = state_paths.last().map(|(_, path)| path.to_path_buf());

            let state_path = if let Some(latest_state_path) = latest_state_path {
                latest_state_path
            } else {
                // create initial state
                let state = LSMTreeState::new();
                let state_id = state.generation();
                let state_path = state_path.join(format!("{}.json", state_id));
                state.save(&state_path).await;

                state_path
            };
            LSMTreeState::load(&state_path).await
        };

        let memtable = {
            let wal_path = {
                let mut path = path.as_ref().to_path_buf();
                path.push("wal");
                path
            };

            if !wal_path.exists() {
                tokio::fs::create_dir_all(&wal_path).await.unwrap();
            }

            let mut wal_paths = vec![];
            let mut read_dir = tokio::fs::read_dir(&wal_path).await.unwrap();
            while let Some(entry) = read_dir.next_entry().await.unwrap() {
                let path = entry.path();
                if path.is_file() && path.extension() == Some("wal".as_ref()) {
                    let file_prefix = path.file_stem().unwrap().to_str().unwrap();
                    if let Ok(wal_id) = file_prefix.parse::<u64>() {
                        wal_paths.push((wal_id, path));
                    }
                }
            }

            wal_paths.sort_by_key(|(wal_id, _)| *wal_id);
            let latest_wal_path = wal_paths.last().map(|(_, path)| path.to_path_buf());

            if let Some(latest_wal_path) = latest_wal_path {
                let mut wal = WALReader::new(latest_wal_path).await.unwrap();
                let mut memtable = MemTable::new();
                while let Some(item) = wal.next().await {
                    match item {
                        DataItem::Put { key, value } => {
                            memtable.put(key, value);
                        }
                        DataItem::Delete { key } => {
                            memtable.delete(key);
                        }
                    }
                }
                memtable
            } else {
                // create initial memtable
                MemTable::new()
            }
        };

        let wal_writer = {
            let wal_path = {
                let mut path = path.as_ref().to_path_buf();
                path.push("wal");
                path
            };

            let wal_id = state.wal_id();
            let wal_path = wal_path.join(format!("{}.wal", wal_id));
            WALWriter::new(wal_path).await.unwrap()
        };

        LSMTree {
            config,
            path: path.as_ref().to_path_buf(),
            state,
            memtable,
            wal_writer,
        }
    }

    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), std::io::Error> {
        self.wal_writer.put(key.clone(), value.clone()).await?;
        self.memtable.put(key, value);
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        if let Some(item) = self.memtable.get(key) {
            match item {
                DataItem::Put { value, .. } => {
                    return Ok(Some(value.clone()));
                }
                DataItem::Delete { .. } => {
                    return Ok(None);
                }
            }
        }

        let sstable_ids = self.state.sstable_ids();
        for sstable_ids_level in sstable_ids {
            for sstable_id in sstable_ids_level.iter().rev() {
                let sstable_path = {
                    let mut path = self.path.clone();
                    path.push("sstables");
                    path.push(format!("{}.sst", sstable_id));
                    path
                };

                let mut reader = SSTableReader::new(sstable_path).await.unwrap();
                if let Some(value) = reader.get(key).await? {
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    pub async fn delete(&mut self, key: Vec<u8>) -> Result<(), std::io::Error> {
        self.wal_writer.delete(key.clone()).await?;
        self.memtable.delete(key);
        Ok(())
    }
}
