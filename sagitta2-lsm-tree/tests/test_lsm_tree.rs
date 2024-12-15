use std::collections::BTreeMap;

use sagitta2_lsm_tree::{
    lsm_tree::LSMTree, lsm_tree_config::LSMTreeConfig, sstable_writer::SSTableWriterConfig,
};

fn gen_random_key() -> Vec<u8> {
    let key_length = rand::random::<usize>() % 16 + 1;
    let mut key = vec![0u8; key_length];
    for i in 0..key_length {
        key[i] = rand::random();
    }
    key
}

fn gen_random_value() -> Vec<u8> {
    let value_length = rand::random::<usize>() % 256 + 1;
    let mut value = vec![0u8; value_length];
    for i in 0..value_length {
        value[i] = rand::random();
    }
    value
}

#[tokio::test]
async fn test_lsm_tree_1() {
    let path = tempfile::tempdir().unwrap();
    let config = LSMTreeConfig {
        sstable_writer_config: SSTableWriterConfig {
            index_branching_factor: 4,
            bloom_filter_size: 1024,
        },
    };
    let mut lsm_tree = LSMTree::new(path.path(), config).await;

    let n = 1000;
    let mut expected = BTreeMap::new();

    for _ in 0..10 {
        for _ in 0..n {
            let key = gen_random_key();
            let value = gen_random_value();
            expected.insert(key.clone(), value.clone());
            lsm_tree.put(key.clone(), value.clone()).await.unwrap();
        }

        for (key, value) in &expected {
            let result = lsm_tree.get(key).await.unwrap();
            assert_eq!(result, Some(value.clone()));
        }
    }
}
