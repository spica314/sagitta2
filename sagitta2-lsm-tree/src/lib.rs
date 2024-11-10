use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum LSMTreeError {
    Message(String),
}

pub struct LSMTree {
    #[allow(dead_code)]
    base_path: PathBuf,
}

impl LSMTree {
    pub fn new(base_path: PathBuf) -> Self {
        LSMTree { base_path }
    }

    pub async fn put(&self, _key: Vec<u8>, _value: Vec<u8>) -> Result<(), LSMTreeError> {
        Ok(())
    }

    pub async fn get(&self, _key: Vec<u8>) -> Result<Option<Vec<u8>>, LSMTreeError> {
        Ok(None)
    }

    pub async fn delete(&self, _key: Vec<u8>) -> Result<(), LSMTreeError> {
        Ok(())
    }
}
