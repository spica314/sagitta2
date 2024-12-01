use crate::*;
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
};

#[derive(Debug)]
pub struct SSTableReader {
    file: BufReader<File>,
}

impl SSTableReader {
    pub async fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let file = File::open(path).await?;
        let file = BufReader::new(file);
        Ok(SSTableReader { file })
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        // read root index offset
        self.file
            .seek(tokio::io::SeekFrom::Start(24))
            .await
            .unwrap();
        let mut root_index_offset = [0u8; 8];
        self.file.read_exact(&mut root_index_offset).await.unwrap();
        let root_index_offset = u64::from_le_bytes(root_index_offset);

        let mut index_offset = root_index_offset;
        'l1: loop {
            // seek
            self.file
                .seek(tokio::io::SeekFrom::Start(index_offset))
                .await
                .unwrap();

            // get item header
            let mut item_header = [0u8; 8];
            self.file.read_exact(&mut item_header).await.unwrap();
            let item_header = u64::from_le_bytes(item_header);

            // if item header is not INDEX_ITEM_ID, break
            if item_header != INDEX_ITEM_ID as u64 {
                break;
            }

            // item length
            let mut item_length = [0u8; 8];
            self.file.read_exact(&mut item_length).await.unwrap();
            let _item_length = u64::from_le_bytes(item_length);

            // children count
            let mut children_count = [0u8; 8];
            self.file.read_exact(&mut children_count).await.unwrap();
            let children_count = u64::from_le_bytes(children_count);

            // children offsets
            let children_offsets = {
                let mut children_offsets = vec![];
                for _ in 0..children_count {
                    let mut offset = [0u8; 8];
                    self.file.read_exact(&mut offset).await.unwrap();
                    let offset = u64::from_le_bytes(offset);
                    children_offsets.push(offset);
                }
                children_offsets
            };

            // key offsets
            let _key_offsets = {
                let mut key_offsets = vec![];
                for _ in 1..children_count {
                    let mut offset = [0u8; 8];
                    self.file.read_exact(&mut offset).await.unwrap();
                    let offset = u64::from_le_bytes(offset);
                    key_offsets.push(offset);
                }
                key_offsets
            };

            // compare
            for i in 1..children_count {
                let mut key_length = [0u8; 8];
                self.file.read_exact(&mut key_length).await.unwrap();
                let key_length = u64::from_le_bytes(key_length);

                let mut key_i = vec![0u8; key_length as usize];
                self.file.read_exact(&mut key_i).await.unwrap();

                if key < key_i.as_slice() {
                    index_offset = children_offsets[i as usize - 1];
                    continue 'l1;
                }

                // skip padding
                if key_length % 8 != 0 {
                    let padding = 8 - (key_length % 8);
                    self.file
                        .seek(tokio::io::SeekFrom::Current(padding as i64))
                        .await
                        .unwrap();
                }
            }
            index_offset = children_offsets[children_count as usize - 1];
        }

        // data item
        self.file
            .seek(tokio::io::SeekFrom::Start(index_offset))
            .await
            .unwrap();
        let mut item_header = [0u8; 8];
        self.file.read_exact(&mut item_header).await.unwrap();
        let item_header = u64::from_le_bytes(item_header);

        // if item header is not DATA_ITEM_ID, panic
        if item_header != DATA_ITEM_ID as u64 {
            panic!("not data item");
        }

        // key length
        let mut key_length = [0u8; 8];
        self.file.read_exact(&mut key_length).await.unwrap();
        let key_length = u64::from_le_bytes(key_length);

        // value length
        let mut value_length = [0u8; 8];
        self.file.read_exact(&mut value_length).await.unwrap();
        let value_length = u64::from_le_bytes(value_length);

        // key
        let mut item_key = vec![0u8; key_length as usize];
        self.file.read_exact(&mut item_key).await.unwrap();

        if key != item_key.as_slice() {
            return Ok(None);
        }

        // skip padding
        if key_length % 8 != 0 {
            let padding = 8 - (key_length % 8);
            self.file
                .seek(tokio::io::SeekFrom::Current(padding as i64))
                .await
                .unwrap();
        }

        // value
        let mut item_value = vec![0u8; value_length as usize];
        self.file.read_exact(&mut item_value).await.unwrap();

        Ok(Some(item_value))
    }
}
