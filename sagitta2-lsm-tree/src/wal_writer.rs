use crate::*;
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter},
};

// WAL Writer
// +----------------+-----------------+
// | Header         | Data / Index    |
// +----------------+-----------------+
// | 4096 byte      | {custom size}   |
// +----------------+-----------------+
//
// Header
// +----------------+----------------+-----------------+
// | Magic Number   | Version        | Padding         |
// +----------------+----------------+-----------------+
// | 8 byte         | 8 byte         | 4080 byte       |
// +----------------+----------------+-----------------+
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

#[derive(Debug)]
pub struct WALWriter {
    writer: BufWriter<File>,
}

impl WALWriter {
    pub async fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let file = File::create(path).await?;
        let mut writer = BufWriter::new(file);

        // Header
        writer.write_u64_le(WAL_MAGIC_NUMBER).await?;
        writer.write_u64_le(VERSION).await?;
        writer.write_all(&vec![0; 4080]).await?;

        Ok(WALWriter { writer })
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

        // flush
        self.writer.flush().await?;

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

        // flush
        self.writer.flush().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_wal_writer() {
        let path = PathBuf::from("/tmp/test_wal_writer.wal");
        if path.exists() {
            tokio::fs::remove_file(&path).await.unwrap();
        }
        let mut writer = WALWriter::new(path.clone()).await.unwrap();

        for i in 0..1000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            writer.put(key, value).await.unwrap();
        }
    }
}
