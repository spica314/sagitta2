pub mod sstable_reader;
pub mod sstable_writer;

// (random number)
pub(crate) const MAGIC_NUMBER: u64 = 0x1847e2cbf5f372a0;

pub(crate) const VERSION: u64 = 1;

pub(crate) const DATA_ITEM_ID: u8 = 0;

pub(crate) const DELETE_DATA_ITEM_ID: u8 = 1;

pub(crate) const INDEX_ITEM_ID: u8 = 2;

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

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
}
