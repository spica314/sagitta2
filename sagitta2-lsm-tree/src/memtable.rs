use std::collections::BTreeMap;

use crate::DataItem;

#[derive(Debug, Clone, Default)]
pub struct MemTable {
    data: BTreeMap<Vec<u8>, DataItem>,
    estimated_storage_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            data: BTreeMap::new(),
            estimated_storage_size: 0,
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if let Some(value) = self.data.get(&key) {
            if let DataItem::Put {
                value: old_value, ..
            } = value
            {
                self.estimated_storage_size -= old_value.len();
            }
        } else {
            self.estimated_storage_size += key.len();
        }
        self.estimated_storage_size += value.len();

        self.data.insert(key.clone(), DataItem::Put { key, value });
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        if let Some(value) = self.data.get(&key) {
            if let DataItem::Put {
                value: old_value, ..
            } = value
            {
                self.estimated_storage_size -= old_value.len();
            }
        } else {
            self.estimated_storage_size += key.len();
        }

        self.data.insert(key.clone(), DataItem::Delete { key });
    }

    pub fn get(&self, key: &[u8]) -> Option<&DataItem> {
        self.data.get(key)
    }
}
