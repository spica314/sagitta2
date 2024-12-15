use crate::sstable_writer::SSTableWriterConfig;

#[derive(Debug, Clone)]
pub struct LSMTreeConfig {
    pub sstable_writer_config: SSTableWriterConfig,
}
