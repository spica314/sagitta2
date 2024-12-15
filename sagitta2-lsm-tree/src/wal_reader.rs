use crate::*;
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
};

#[derive(Debug)]
pub struct WALReader {
    file: BufReader<File>,
}

impl WALReader {
    pub async fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let file = File::open(path).await?;
        let mut file = BufReader::new(file);

        let offset = 4096;
        file.seek(tokio::io::SeekFrom::Start(offset)).await.unwrap();

        Ok(WALReader { file })
    }

    pub async fn next(&mut self) -> Option<DataItem> {
        loop {
            // item header
            let mut item_header = [0u8; 8];
            let r = self.file.read_exact(&mut item_header).await;
            if let Err(err) = r {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                panic!("Error: {:?}", err);
            }
            let item_header = u64::from_le_bytes(item_header);

            if item_header == DATA_ITEM_ID as u64 {
                // key length
                let mut key_length = [0u8; 8];
                self.file.read_exact(&mut key_length).await.unwrap();
                let key_length = u64::from_le_bytes(key_length);

                // value length
                let mut value_length = [0u8; 8];
                self.file.read_exact(&mut value_length).await.unwrap();
                let value_length = u64::from_le_bytes(value_length);

                // key
                let mut key = vec![0u8; key_length as usize];
                self.file.read_exact(&mut key).await.unwrap();

                // skip padding
                if key_length % 8 != 0 {
                    let padding = 8 - (key_length % 8);
                    self.file
                        .seek(tokio::io::SeekFrom::Current(padding as i64))
                        .await
                        .unwrap();
                }

                // value
                let mut value = vec![0u8; value_length as usize];
                self.file.read_exact(&mut value).await.unwrap();

                // skip padding
                if value_length % 8 != 0 {
                    let padding = 8 - (value_length % 8);
                    self.file
                        .seek(tokio::io::SeekFrom::Current(padding as i64))
                        .await
                        .unwrap();
                }

                return Some(DataItem::Put { key, value });
            } else if item_header == DELETE_DATA_ITEM_ID as u64 {
                // key length
                let mut key_length = [0u8; 8];
                self.file.read_exact(&mut key_length).await.unwrap();
                let key_length = u64::from_le_bytes(key_length);

                // key
                let mut key = vec![0u8; key_length as usize];
                self.file.read_exact(&mut key).await.unwrap();

                // skip padding
                if key_length % 8 != 0 {
                    let padding = 8 - (key_length % 8);
                    self.file
                        .seek(tokio::io::SeekFrom::Current(padding as i64))
                        .await
                        .unwrap();
                }

                return Some(DataItem::Delete { key });
            }
        }
    }
}
