use crate::*;
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter},
};

// SSTable
// +----------------+----------------+-----------------+
// | Header         | Bloom Filter   | Data / Index    |
// +----------------+----------------+-----------------+
// | 4096 byte      | {custom size}  | {custom size}   |
// +----------------+----------------+-----------------+
//
// Header
// +----------------+----------------+-------------------+-------------------+-----------------+
// | Magic Number   | Version        | Bloom Filter size | Root index offset | Padding         |
// +----------------+----------------+-------------------+-------------------+-----------------+
// | 8 byte         | 8 byte         | 8 byte            | 8 byte            | 4072 byte       |
// +----------------+----------------+-------------------+-------------------+-----------------+
//
// Bloom Filter
// +----------------+
// | Bloom Filter   |
// +----------------+
// | {custom size}  |
// +----------------+
//
// Data Item
// +----------------+----------------+-----------------+-----+---------+-------+---------+
// | Item Header    | Key Length     | Value Length    | Key | Padding | Value | Padding |
// +----------------+----------------+-----------------+-----+---------+-------+---------+
// | 8 byte         | 8 byte         | 8 byte          | ... | % 8     | ...   | % 8     |
// +----------------+----------------+-----------------+-----+---------+-------+---------+
//
// Delete Data Item
// +----------------+----------------+-----+---------+
// | Item Header    | Key Length     | Key | Padding |
// +----------------+----------------+-----+---------+
// | 8 byte         | 8 byte         | ... | % 8     |
// +----------------+----------------+-----+---------+
//
// Index Item
// +----------------+--------------+----------------+----------------+-----+----------------+--------------+-----+--------------+
// | Item Header    | Item Length  | Children Count | Child Offset 1 | ... | Child Offset n | Key Offset 2 | ... | Key Offset n |
// +----------------+--------------+----------------+----------------+-----+----------------+--------------+-----+--------------+
// | 8 byte         | 8 byte       | 8 byte         | 8 byte         | ... | 8 byte         | 8 byte       | ... | 8 byte       |
// +----------------+--------------+----------------+----------------+-----+----------------+--------------+-----+--------------+
//
// +-----------------+----------------+-----------+----------------+-----------------+----------------+-----------+
// | Key 2 Length    | Key 2          | Padding   | ...            | Key n Length    | Key n          | Padding   |
// +-----------------+----------------+-----------+----------------+-----------------+----------------+-----------+
// | 8 byte          | ...            | % 8       | ...            | 8 byte          | ...            | % 8       |
// +-----------------+----------------+-----------+----------------+-----------------+----------------+-----------+

#[derive(Debug, Clone)]
pub struct SSTableWriterConfig {
    pub index_branching_factor: usize,
    pub bloom_filter_size: usize,
}

#[derive(Debug)]
pub struct SSTableWriter {
    config: SSTableWriterConfig,
    writer: BufWriter<File>,
    // (key, offset)
    index: Vec<Vec<(Vec<u8>, usize)>>,
    #[allow(dead_code)]
    bloom_filter: Vec<u8>,
}

impl SSTableWriter {
    pub async fn new(path: PathBuf, config: SSTableWriterConfig) -> Result<Self, std::io::Error> {
        let file = File::create(path).await?;
        let mut writer = BufWriter::new(file);

        // Header
        writer.write_u64_le(MAGIC_NUMBER).await?;
        writer.write_u64_le(VERSION).await?;
        writer.write_u64_le(config.bloom_filter_size as u64).await?;
        writer.write_u64_le(0).await?;
        writer.write_all(&vec![0; 4072]).await?;

        // Bloom Filter
        writer.write_all(&vec![0; config.bloom_filter_size]).await?;

        Ok(SSTableWriter {
            config: config.clone(),
            writer,
            index: Vec::new(),
            bloom_filter: vec![0; config.bloom_filter_size],
        })
    }

    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), std::io::Error> {
        // check if offset is aligned to 8 bytes
        let item_offset = self.writer.stream_position().await.unwrap();
        assert!(item_offset % 8 == 0);

        // Data Item Header
        self.writer.write_u64_le(DATA_ITEM_ID as u64).await?;

        // key length
        self.writer.write_u64_le(key.len() as u64).await?;

        // value length
        self.writer.write_u64_le(value.len() as u64).await?;

        // key
        self.writer.write_all(&key).await?;
        // padding
        if key.len() % 8 != 0 {
            let padding = 8 - (key.len() % 8);
            self.writer.write_all(&vec![0; padding]).await?;
        }

        // value
        self.writer.write_all(&value).await?;
        // padding
        if value.len() % 8 != 0 {
            let padding = 8 - (value.len() % 8);
            self.writer.write_all(&vec![0; padding]).await?;
        }

        // add to index
        self.add_to_index(key, item_offset as usize).await?;

        Ok(())
    }

    pub async fn delete(&mut self, key: Vec<u8>) -> Result<(), std::io::Error> {
        // check if offset is aligned to 8 bytes
        let item_offset = self.writer.stream_position().await.unwrap();
        assert!(item_offset % 8 == 0);

        // Data Item Header
        self.writer.write_u64_le(DELETE_DATA_ITEM_ID as u64).await?;

        // key length
        self.writer.write_u64_le(key.len() as u64).await?;

        // key
        self.writer.write_all(&key).await?;
        // padding
        if key.len() % 8 != 0 {
            let padding = 8 - (key.len() % 8);
            self.writer.write_all(&vec![0; padding]).await?;
        }

        // add to index
        self.add_to_index(key, item_offset as usize).await?;

        Ok(())
    }

    async fn carry_up_index(&mut self, i: usize) -> Result<(), std::io::Error> {
        // check if offset is aligned to 8 bytes
        let offset = self.writer.stream_position().await.unwrap();
        assert!(offset % 8 == 0);

        // Index Item Header
        self.writer.write_u64_le(INDEX_ITEM_ID as u64).await?;

        // item length
        let children_count = self.index[i].len();
        let (length, key_offsets) = {
            let mut length = 0;
            let mut key_offsets = vec![];

            // item header
            length += 8;
            // item length
            length += 8;
            // children count
            length += 8;
            // child offset(s)
            length += 8 * children_count;
            // key offset(s)
            length += 8 * (children_count - 1);
            for (key, _) in self.index[i].iter().skip(1) {
                key_offsets.push(length);
                // key length
                length += 8;
                // key
                length += (key.len() + 7) / 8 * 8;
            }
            (length, key_offsets)
        };
        self.writer.write_u64_le(length as u64).await?;

        // children count
        self.writer.write_u64_le(children_count as u64).await?;

        // child offset(s)
        for (_, child_offset) in self.index[i].iter() {
            self.writer.write_u64_le(*child_offset as u64).await?;
        }

        // key offset(s)
        for key_offset in key_offsets {
            self.writer.write_u64_le(key_offset as u64).await?;
        }

        // key(s)
        for (key, _) in self.index[i].iter().skip(1) {
            // key length
            self.writer.write_u64_le(key.len() as u64).await?;
            // key
            self.writer.write_all(key).await?;
            // padding
            if key.len() % 8 != 0 {
                let padding = 8 - (key.len() % 8);
                self.writer.write_all(&vec![0; padding]).await?;
            }
        }

        if i + 1 >= self.index.len() {
            self.index.push(vec![]);
        }

        // carry up
        let key = self.index[i][0].0.clone();
        self.index[i + 1].push((key, offset as usize));
        self.index[i] = vec![];

        Ok(())
    }

    async fn add_to_index(&mut self, key: Vec<u8>, offset: usize) -> Result<(), std::io::Error> {
        if self.index.is_empty() {
            self.index.push(vec![(key.to_vec(), offset)]);
            return Ok(());
        } else {
            self.index[0].push((key.to_vec(), offset));
        }
        assert!(!self.index.is_empty());

        for i in 0..self.index.len() {
            // if the current index is not full, break
            if self.index[i].len() < self.config.index_branching_factor {
                break;
            }

            self.carry_up_index(i).await?;
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<(), std::io::Error> {
        // todo: bloom filter

        // root index offset
        if self.index.is_empty() {
            self.writer.seek(tokio::io::SeekFrom::Start(24)).await?;
            self.writer.write_u64_le(0).await?;
        } else {
            for i in 0..self.index.len() - 1 {
                if !self.index[i].is_empty() {
                    self.carry_up_index(i).await?;
                }
            }
            if self.index.last().as_ref().unwrap().len() > 1 {
                self.carry_up_index(self.index.len() - 1).await?;
            }
            let offset = self.index.last().as_ref().unwrap()[0].1;
            self.writer.seek(tokio::io::SeekFrom::Start(24)).await?;
            self.writer.write_u64_le(offset as u64).await?;
        }

        self.writer.flush().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_sstable_writer() {
        let path = PathBuf::from("/tmp/test_writer.sst");
        if path.exists() {
            tokio::fs::remove_file(&path).await.unwrap();
        }
        let config = SSTableWriterConfig {
            index_branching_factor: 2,
            bloom_filter_size: 1024,
        };
        let mut writer = SSTableWriter::new(path.clone(), config.clone())
            .await
            .unwrap();

        for i in 0..1000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            writer.put(key, value).await.unwrap();
        }

        writer.finish().await.unwrap();
    }
}
