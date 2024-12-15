pub mod lsm_tree;
pub mod lsm_tree_config;
pub mod lsm_tree_state;
pub mod memtable;
pub mod sstable_reader;
pub mod sstable_sequential_reader;
pub mod sstable_writer;
pub mod wal_reader;
pub mod wal_writer;

// (random number)
pub(crate) const SSTABLE_MAGIC_NUMBER: u64 = 0x1847e2cbf5f372a0;
pub(crate) const WAL_MAGIC_NUMBER: u64 = 0xcc1e6a9e6b6c2000;

pub(crate) const VERSION: u64 = 1;

pub(crate) const DATA_ITEM_ID: u8 = 0;

pub(crate) const DELETE_DATA_ITEM_ID: u8 = 1;

pub(crate) const INDEX_ITEM_ID: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataItem {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl DataItem {
    pub fn key(&self) -> &[u8] {
        match self {
            DataItem::Put { key, .. } => key,
            DataItem::Delete { key } => key,
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::sstable_sequential_reader::SSTableSequentialReader;
    use crate::wal_reader::WALReader;
    use crate::wal_writer::WALWriter;
    use crate::DataItem;

    use super::sstable_reader::*;
    use super::sstable_writer::*;

    #[tokio::test]
    async fn test_sstable_writer_reader() {
        let path = PathBuf::from("/tmp/test_writer_reader.sst");
        if path.exists() {
            tokio::fs::remove_file(&path).await.unwrap();
        }
        let config = SSTableWriterConfig {
            index_branching_factor: 4,
            bloom_filter_size: 1024,
        };
        let mut writer = SSTableWriter::new(path.clone(), config.clone())
            .await
            .unwrap();

        let n = 1000;

        let mut kv_expected = BTreeMap::new();
        for i in 0..n {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            kv_expected.insert(key.clone(), value.clone());
        }

        let begin_t = std::time::Instant::now();

        for (key, value) in &kv_expected {
            writer.put(key.clone(), value.clone()).await.unwrap();
        }

        writer.finish().await.unwrap();

        let e = begin_t.elapsed();
        eprintln!("write: {}", e.as_secs_f64());
        eprintln!("write: {} ops / s", 1.0 / (e.as_secs_f64() / n as f64));

        let begin_t = std::time::Instant::now();

        let mut reader = SSTableReader::new(path.clone()).await.unwrap();
        for (key, expected_value) in kv_expected.iter() {
            // eprintln!("key = {:?}, expected = {:?}", key, expected_value);
            let value = reader.get(key).await.unwrap().unwrap();
            // eprintln!("value = {:?}", value);
            assert_eq!(value, expected_value.as_slice());
        }

        let e = begin_t.elapsed();
        eprintln!("read(seq): {}", e.as_secs_f64());
    }

    #[tokio::test]
    async fn test_sstable_writer_reader_2() {
        let path = PathBuf::from("/tmp/test_writer_reader_2.sst");
        if path.exists() {
            tokio::fs::remove_file(&path).await.unwrap();
        }
        let config = SSTableWriterConfig {
            index_branching_factor: 4,
            bloom_filter_size: 1024,
        };
        let mut writer = SSTableWriter::new(path.clone(), config.clone())
            .await
            .unwrap();

        let n = 1000;

        let mut kv_expected = BTreeMap::new();
        for i in 0..n {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            kv_expected.insert(key.clone(), value.clone());
        }

        let begin_t = std::time::Instant::now();

        for (i, (key, value)) in kv_expected.iter().enumerate() {
            if (i as usize).count_ones() % 2 == 1 {
                writer.delete(key.clone()).await.unwrap();
            } else {
                writer.put(key.clone(), value.clone()).await.unwrap();
            }
        }

        writer.finish().await.unwrap();

        let e = begin_t.elapsed();
        eprintln!("write: {}", e.as_secs_f64());
        eprintln!("write: {} ops / s", 1.0 / (e.as_secs_f64() / n as f64));

        let begin_t = std::time::Instant::now();

        let mut reader = SSTableReader::new(path.clone()).await.unwrap();
        for (i, (key, expected_value)) in kv_expected.iter().enumerate() {
            if (i as usize).count_ones() % 2 == 1 {
                assert!(reader.get(key).await.unwrap().is_none());
                continue;
            } else {
                let value = reader.get(key).await.unwrap().unwrap();
                assert_eq!(value, expected_value.as_slice());
            }
        }

        let e = begin_t.elapsed();
        eprintln!("read(seq): {}", e.as_secs_f64());
    }

    #[tokio::test]
    async fn test_sstable_writer_reader_3() {
        let path = PathBuf::from("/tmp/test_writer_reader_3.sst");
        if path.exists() {
            tokio::fs::remove_file(&path).await.unwrap();
        }
        let config = SSTableWriterConfig {
            index_branching_factor: 4,
            bloom_filter_size: 1024,
        };
        let mut writer = SSTableWriter::new(path.clone(), config.clone())
            .await
            .unwrap();

        let n = 1000;

        let mut kv_expected = BTreeMap::new();
        for i in 0..n {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            kv_expected.insert(key.clone(), value.clone());
        }

        let begin_t = std::time::Instant::now();

        for (i, (key, value)) in kv_expected.iter().enumerate() {
            if (i as usize).count_ones() % 2 == 1 {
                writer.delete(key.clone()).await.unwrap();
            } else {
                writer.put(key.clone(), value.clone()).await.unwrap();
            }
        }

        writer.finish().await.unwrap();

        let e = begin_t.elapsed();
        eprintln!("write: {}", e.as_secs_f64());
        eprintln!("write: {} ops / s", 1.0 / (e.as_secs_f64() / n as f64));

        let mut reader = SSTableSequentialReader::new(path.clone()).await.unwrap();
        let mut expected = vec![];
        for (i, (key, expected_value)) in kv_expected.iter().enumerate() {
            if (i as usize).count_ones() % 2 == 1 {
                expected.push(DataItem::Delete { key: key.clone() });
            } else {
                expected.push(DataItem::Put {
                    key: key.clone(),
                    value: expected_value.clone(),
                });
            }
        }

        let begin_t = std::time::Instant::now();

        let mut results = vec![];
        loop {
            match reader.next().await {
                Some(item) => {
                    results.push(item);
                }
                None => break,
            }
        }

        assert_eq!(results, expected);

        let e = begin_t.elapsed();
        eprintln!("read(seq): {}", e.as_secs_f64());
    }

    #[tokio::test]
    async fn test_wal_writer_reader_1() {
        let path = PathBuf::from("/tmp/test_wal_writer_reader_1.wal");
        if path.exists() {
            tokio::fs::remove_file(&path).await.unwrap();
        }
        let mut writer = WALWriter::new(path.clone()).await.unwrap();

        let n = 1000;

        let mut kv_expected = BTreeMap::new();
        for i in 0..n {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            kv_expected.insert(key.clone(), value.clone());
        }

        let begin_t = std::time::Instant::now();

        for (i, (key, value)) in kv_expected.iter().enumerate() {
            if (i as usize).count_ones() % 2 == 1 {
                writer.delete(key.clone()).await.unwrap();
            } else {
                writer.put(key.clone(), value.clone()).await.unwrap();
            }
        }

        let e = begin_t.elapsed();
        eprintln!("write: {}", e.as_secs_f64());
        eprintln!("write: {} ops / s", 1.0 / (e.as_secs_f64() / n as f64));

        let mut reader = WALReader::new(path.clone()).await.unwrap();
        let mut expected = vec![];
        for (i, (key, expected_value)) in kv_expected.iter().enumerate() {
            if (i as usize).count_ones() % 2 == 1 {
                expected.push(DataItem::Delete { key: key.clone() });
            } else {
                expected.push(DataItem::Put {
                    key: key.clone(),
                    value: expected_value.clone(),
                });
            }
        }

        let begin_t = std::time::Instant::now();

        let mut results = vec![];
        loop {
            match reader.next().await {
                Some(item) => {
                    results.push(item);
                }
                None => break,
            }
        }

        assert_eq!(results, expected);

        let e = begin_t.elapsed();
        eprintln!("read(seq): {}", e.as_secs_f64());
    }
}
