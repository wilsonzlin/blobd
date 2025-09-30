use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream::once, StreamExt};
use rocksdb::{BlockBasedIndexType, BlockBasedOptions, Cache, DataBlockIndexType, Options, DB};
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

use crate::{BlobdProvider, CommitObjectInput, CommitObjectOutput, CreateObjectInput, CreateObjectOutput, DeleteObjectInput, InspectObjectInput, InspectObjectOutput, ReadObjectInput, ReadObjectOutput, WriteObjectInput};

pub struct RocksDBStore {
    db: Arc<DB>,
}

impl RocksDBStore {
    pub fn new(path: &str) -> Self {
        let mut opt = Options::default();
        opt.create_if_missing(true);

        // Maximize disk I/O utilization.
        opt.set_max_background_jobs(num_cpus::get() as i32 * 2);
        opt.set_bytes_per_sync(1024 * 1024 * 4);
        
        // Enable BlobDB.
        opt.set_enable_blob_files(true);
        opt.set_min_blob_size(0);
        opt.set_enable_blob_gc(true);

        // Use more RAM for better performance.
        // https://github.com/facebook/rocksdb/wiki/Block-Cache.
        let block_cache = Cache::new_lru_cache(1024 * 1024 * 1024 * 32);
        let mut bbt_opt = BlockBasedOptions::default();
        opt.set_write_buffer_size(1024 * 1024 * 256);

        // Enable partitioned index filters: https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
        // NOTE: We cannot use HashSearch as that requires a prefix extractor.
        bbt_opt.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
        bbt_opt.set_bloom_filter(10.0, false);
        bbt_opt.set_partition_filters(true);
        bbt_opt.set_metadata_block_size(4096);
        bbt_opt.set_cache_index_and_filter_blocks(true);
        bbt_opt.set_pin_top_level_index_and_filter(true);
        bbt_opt.set_pin_l0_filter_and_index_blocks_in_cache(true);

        // Optimize for point lookups.
        // Don't use `optimize_for_point_lookup()`, which just sets a custom BlockBasedOptions; we'll use our own custom options instead.
        // NOTE: We don't enable memtable_whole_key_filtering as that uses a lot more memory for an unknown performance benefit (key lookups in memory should already be fast, and memtables should not be that large).
        // https://github.com/facebook/rocksdb/wiki/BlobDB#performance-tuning
        bbt_opt.set_block_size(1024 * 64);
        bbt_opt.set_block_cache(&block_cache);
        bbt_opt.set_format_version(5);
        // https://github.com/facebook/rocksdb/blob/25b08eb4386768b05a0748bfdb505ab58921281a/options/options.cc#L615.
        bbt_opt.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
        bbt_opt.set_data_block_hash_ratio(0.5);
        // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#ribbon-filter.
        // Don't set this too high, as benefits drop off exponentially while memory increases linearly.
        bbt_opt.set_ribbon_filter(10.0);
        opt.set_block_based_table_factory(&bbt_opt);

        let db = DB::open(&opt, path).unwrap();
        Self { db: Arc::new(db) }
    }
}

#[async_trait]
impl BlobdProvider for RocksDBStore {
    fn metrics(&self) -> Vec<(&'static str, u64)> {
        vec![]
    }

    async fn wait_for_end(&self) {
    }

    async fn create_object(&self, _input: CreateObjectInput) -> CreateObjectOutput {
        CreateObjectOutput {
            token: Arc::new(0),
        }
    }

    async fn write_object<'a>(&'a self, input: WriteObjectInput<'a>) {
        let db = self.db.clone();
        let data = input.data.to_vec();
        spawn_blocking(move || {
            db.put(input.key, data).unwrap();
        }).await.unwrap();
    }

    async fn commit_object(&self, _input: CommitObjectInput) -> CommitObjectOutput {
        CommitObjectOutput {
            object_id: None,
        }
    }
    
    async fn inspect_object(&self, input: InspectObjectInput) -> InspectObjectOutput {
        let db = self.db.clone();
        let len = spawn_blocking(move || {
            db.get_pinned(input.key).unwrap().unwrap().len()
        }).await.unwrap();
        InspectObjectOutput {
            id: None,
            size: len as u64,
        }
    }
    
    async fn read_object(&self, input: ReadObjectInput) -> ReadObjectOutput {
        let db = self.db.clone();
        // Use stream as we calculate TTFB based on time to first chunk, which is incorrect if we simply read all at once.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_blocking(move || {
            let mmap = db.get_pinned(input.key).unwrap().unwrap();
            let slice = &mmap[input.start as usize..input.end.map(|e| e as usize).unwrap_or(mmap.len())];
            // TODO Allow configuring chunk size. We have no idea what the correct value is, as it is likely dynamic.
            for chunk in slice.chunks(512) {
                tx.send(chunk.to_vec()).unwrap();
            }
        }).await.unwrap();
        ReadObjectOutput {
            data_stream: UnboundedReceiverStream::new(rx).boxed(),
        }
    }
    
    async fn delete_object(&self, input: DeleteObjectInput) {
        let db = self.db.clone();
        spawn_blocking(move || {
            db.delete(input.key).unwrap();
        }).await.unwrap();
    }
}