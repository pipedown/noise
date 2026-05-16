use std::sync::Arc;

use noise_storage::{
    compare_keys, compare_keys_multidim, should_drop_key, sum_merge, BackendBatch, BackendDatabase,
    BackendSnapshot, Cursor, CursorBackend, DatabaseConfig, Namespace, SeekFrom, StorageError,
};
use rocksdb::{
    self, BlockBasedIndexType, BlockBasedOptions, CompactionDecision, IteratorMode, MergeOperands,
    Snapshot as RocksDbSnapshot,
};
use self_cell::self_cell;

fn rocksdb_merge_adapter(
    key: &[u8],
    existing: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Vec<u8> {
    let mut op_ref: &mut MergeOperands = operands;
    sum_merge(key, existing, &mut op_ref)
}

fn rocksdb_compaction_adapter(level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
    if should_drop_key(level, key, value) {
        CompactionDecision::Remove
    } else {
        CompactionDecision::Keep
    }
}

pub struct RocksDatabase {
    db: Arc<rocksdb::DB>,
    rtree: rocksdb::ColumnFamily,
}

impl BackendDatabase for RocksDatabase {
    type Batch = RocksBatch;
    type Snapshot = RocksSnapshot;

    fn open(path: &str, config: &DatabaseConfig) -> Result<Self, StorageError> {
        let rocks_options = Self::build_options();
        let rtree_options = Self::build_rtree_options();

        let result = rocksdb::DB::open_cf(&rocks_options, path, &["rtree"], &[&rtree_options]);

        match result {
            Ok(db) => {
                let rtree = db.cf_handle("rtree").unwrap();
                Ok(RocksDatabase {
                    db: Arc::new(db),
                    rtree,
                })
            }
            Err(error) => {
                if !config.create_if_missing {
                    return Err(StorageError::new(error));
                }

                let mut rocks_options = Self::build_options();
                rocks_options.create_if_missing(true);

                let mut db = rocksdb::DB::open(&rocks_options, path).map_err(StorageError::new)?;
                db.create_cf("rtree", &rtree_options)
                    .map_err(StorageError::new)?;

                let rtree = db.cf_handle("rtree").unwrap();
                Ok(RocksDatabase {
                    db: Arc::new(db),
                    rtree,
                })
            }
        }
    }

    fn destroy(path: &str) -> Result<(), StorageError> {
        rocksdb::DB::destroy(&rocksdb::Options::default(), path).map_err(StorageError::new)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .db
            .get(key)
            .map_err(StorageError::new)?
            .map(|v| v.to_vec()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.db
            .put_opt(key, value, &rocksdb::WriteOptions::new())
            .map_err(StorageError::new)
    }

    fn write(&self, batch: RocksBatch) -> Result<(), StorageError> {
        self.db.write(batch.wb).map_err(StorageError::new)
    }

    fn new_batch(&self) -> RocksBatch {
        RocksBatch {
            wb: rocksdb::WriteBatch::default(),
            rtree: self.rtree,
        }
    }

    fn snapshot(&self) -> RocksSnapshot {
        RocksSnapshot::new(self.db.clone(), |db| RocksDbSnapshot::new(db))
    }

    fn iterator(&self) -> Cursor {
        Cursor::new(Box::new(RocksCursor {
            iter: self.db.iterator(IteratorMode::Start),
        }))
    }

    fn compact(&self) {
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
    }
}

impl RocksDatabase {
    fn build_options() -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.set_comparator("noise_cmp", compare_keys);
        opts.set_merge_operator("noise_merge", rocksdb_merge_adapter);
        opts.set_compaction_filter("noise_compact", rocksdb_compaction_adapter);
        opts
    }

    fn build_rtree_options() -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.set_memtable_skip_list_mbb_rep();
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_index_type(BlockBasedIndexType::RtreeSearch);
        opts.set_block_based_table_factory(&block_opts);
        opts.set_comparator("noise_rtree_cmp", compare_keys_multidim);
        opts
    }
}

pub struct RocksBatch {
    wb: rocksdb::WriteBatch,
    rtree: rocksdb::ColumnFamily,
}

impl BackendBatch for RocksBatch {
    fn put(&mut self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        match ns {
            Namespace::Default => self.wb.put(key, value).map_err(StorageError::new),
            Namespace::Multidim => self
                .wb
                .put_cf(self.rtree, key, value)
                .map_err(StorageError::new),
        }
    }

    fn delete(&mut self, ns: Namespace, key: &[u8]) -> Result<(), StorageError> {
        match ns {
            Namespace::Default => self.wb.delete(key).map_err(StorageError::new),
            Namespace::Multidim => self
                .wb
                .delete_cf(self.rtree, key)
                .map_err(StorageError::new),
        }
    }

    fn merge(&mut self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.wb.merge(key, value).map_err(StorageError::new)
    }
}

// `rocksdb::Snapshot<'a>` borrows from `&DB`, but the `BackendSnapshot` trait
// requires `'static`, so we need to store both the snapshot and the `Arc<DB>`
// it borrows from in a single owned struct. `self_cell!` provides a safe
// wrapper for this self-referential pattern (the alternative would be an
// unsafe `mem::transmute` to erase the lifetime).
self_cell!(
    pub struct RocksSnapshot {
        owner: Arc<rocksdb::DB>,
        #[covariant]
        dependent: RocksDbSnapshot,
    }
);

impl BackendSnapshot for RocksSnapshot {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.borrow_dependent()
            .get(key)
            .unwrap()
            .map(|v| v.to_vec())
    }

    fn iterator(&self) -> Cursor {
        Cursor::new(Box::new(RocksCursor {
            iter: self.borrow_dependent().iterator(IteratorMode::Start),
        }))
    }

    fn multidim_iterator(&self, query: &[u8]) -> Cursor {
        Cursor::new(Box::new(RocksCursor {
            iter: self.borrow_dependent().rtree_iterator(query),
        }))
    }
}

struct RocksCursor {
    iter: rocksdb::DBIterator,
}

impl CursorBackend for RocksCursor {
    fn next(&mut self) -> Option<(Box<[u8]>, Box<[u8]>)> {
        self.iter.next()
    }

    fn seek(&mut self, from: SeekFrom) {
        match from {
            SeekFrom::Start => {
                self.iter.set_mode(IteratorMode::Start);
            }
            SeekFrom::Key(key) => {
                self.iter
                    .set_mode(IteratorMode::From(key, rocksdb::Direction::Forward));
            }
        }
    }
}
