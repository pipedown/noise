use std::collections::btree_map;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, RwLock};

use noise_storage::{
    decode_byte_orderable_u64, encode_byte_orderable_u64, get_length_prefixed_slice,
    should_drop_key, sum_merge, BackendBatch, BackendDatabase, BackendSnapshot, Cursor,
    CursorBackend, DatabaseConfig, Namespace, SeekFrom, StorageError,
};
use self_cell::self_cell;

/// The default namespace: raw key bytes to raw value bytes.
type DefaultMap = BTreeMap<Vec<u8>, Vec<u8>>;

// `Arc<BTreeMap>` per namespace gives copy-on-write snapshots: cloning
// `Inner` only bumps refcounts, and `Arc::make_mut` clones the map exactly
// once when a writer races with a live snapshot or cursor. Map keys are
// plain `Vec<u8>` because the byte-wise encoding produced by `KeyBuilder`
// already sorts correctly under lexicographic comparison.
//
// The multidim namespace groups entries by keypath in an outer
// `BTreeMap<prefix, Arc<PerKeypath>>` keyed by the length-prefixed keypath
// every entry in the bucket begins with. The inner `Arc` is what a
// `MultidimCursor` holds: `Cursor` is `'static` and lends its entries, so a
// cursor owns the bucket it scans, and building one is a refcount bump
// rather than a copy of the scan array. It also keeps copy-on-write
// per-keypath, so a write clones the touched bucket and no other.
#[derive(Default, Clone)]
struct Inner {
    default: Arc<DefaultMap>,
    multidim: Arc<BTreeMap<Vec<u8>, Arc<PerKeypath>>>,
}

impl Inner {
    // A cursor over the whole default namespace, sharing the map rather than
    // copying it.
    fn default_cursor(&self) -> Cursor {
        Cursor::new(Box::new(BTreeCursor::unbounded(Arc::clone(&self.default))))
    }
}

/// One keypath's entries, ordered by key.
///
/// A multidim key is the length-prefixed keypath followed by the numeric
/// fields of an [`EntryMbb`]. The keypath is what the bucket is keyed by, so
/// within one bucket those fields are the whole of the key: the bucket holds
/// them decoded, and the cursor puts the bytes back together for the entry it
/// hands out. The encoding is order-preserving, so the decoded entries sort
/// the way their keys do, and `mbbs` is the contiguous array the scan walks.
#[derive(Default, Clone)]
struct PerKeypath {
    mbbs: Vec<EntryMbb>,
    values: Vec<Vec<u8>>,
}

pub struct MemoryDatabase {
    inner: RwLock<Inner>,
}

impl BackendDatabase for MemoryDatabase {
    type Batch = MemoryBatch;
    type Snapshot = MemorySnapshot;

    fn open(_path: &str, _config: &DatabaseConfig) -> Result<Self, StorageError> {
        Ok(Self {
            inner: RwLock::new(Inner::default()),
        })
    }

    fn destroy(_path: &str) -> Result<(), StorageError> {
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.inner.read().unwrap().default.get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut inner = self.inner.write().unwrap();
        Arc::make_mut(&mut inner.default).insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn write(&self, batch: MemoryBatch) -> Result<(), StorageError> {
        let mut inner = self.inner.write().unwrap();
        for op in batch.ops {
            match op {
                BatchOp::Put(Namespace::Default, key, value) => {
                    Arc::make_mut(&mut inner.default).insert(key, value);
                }
                BatchOp::Put(Namespace::Multidim, key, value) => {
                    let (prefix, entry) = split_multidim_key(&key);
                    let map = Arc::make_mut(&mut inner.multidim);
                    let per_keypath = Arc::make_mut(map.entry(prefix.to_vec()).or_default());
                    match per_keypath.mbbs.binary_search(&entry) {
                        // The last write wins.
                        Ok(pos) => per_keypath.values[pos] = value,
                        Err(pos) => {
                            per_keypath.mbbs.insert(pos, entry);
                            per_keypath.values.insert(pos, value);
                        }
                    }
                }
                BatchOp::Delete(Namespace::Default, key) => {
                    Arc::make_mut(&mut inner.default).remove(&key);
                }
                BatchOp::Delete(Namespace::Multidim, key) => {
                    let (prefix, entry) = split_multidim_key(&key);
                    let map = Arc::make_mut(&mut inner.multidim);
                    if let Some(bucket) = map.get_mut(prefix) {
                        let per_keypath = Arc::make_mut(bucket);
                        if let Ok(pos) = per_keypath.mbbs.binary_search(&entry) {
                            per_keypath.mbbs.remove(pos);
                            per_keypath.values.remove(pos);
                        }
                        if per_keypath.mbbs.is_empty() {
                            map.remove(prefix);
                        }
                    }
                }
                BatchOp::Merge(key, value) => {
                    // RocksDB defers merges to read/compaction time; here the
                    // merge is applied eagerly, and the compaction filter with
                    // it, so a count that drops to zero removes the key right
                    // away and `compact` has nothing left to do. There are no
                    // levels to compact either, and the filter ignores the one
                    // it is handed, so it gets the level a flush would carry.
                    let map = Arc::make_mut(&mut inner.default);
                    let existing = map.get(&key).map(Vec::as_slice);
                    let merged = sum_merge(&key, existing, &mut std::iter::once(value.as_slice()));
                    if should_drop_key(0, &key, &merged) {
                        map.remove(&key);
                    } else {
                        map.insert(key, merged);
                    }
                }
            }
        }
        Ok(())
    }

    fn new_batch(&self) -> MemoryBatch {
        MemoryBatch { ops: Vec::new() }
    }

    fn snapshot(&self) -> MemorySnapshot {
        MemorySnapshot {
            inner: self.inner.read().unwrap().clone(),
        }
    }

    fn iterator(&self) -> Cursor {
        self.inner.read().unwrap().default_cursor()
    }

    fn compact(&self) {}
}

enum BatchOp {
    Put(Namespace, Vec<u8>, Vec<u8>),
    Delete(Namespace, Vec<u8>),
    Merge(Vec<u8>, Vec<u8>),
}

pub struct MemoryBatch {
    ops: Vec<BatchOp>,
}

impl BackendBatch for MemoryBatch {
    fn put(&mut self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.ops
            .push(BatchOp::Put(ns, key.to_vec(), value.to_vec()));
        Ok(())
    }

    fn delete(&mut self, ns: Namespace, key: &[u8]) -> Result<(), StorageError> {
        self.ops.push(BatchOp::Delete(ns, key.to_vec()));
        Ok(())
    }

    fn merge(&mut self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.ops.push(BatchOp::Merge(key.to_vec(), value.to_vec()));
        Ok(())
    }
}

pub struct MemorySnapshot {
    inner: Inner,
}

impl BackendSnapshot for MemorySnapshot {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.default.get(key).cloned()
    }

    fn iterator(&self) -> Cursor {
        self.inner.default_cursor()
    }

    fn multidim_iterator(&self, query: &[u8]) -> Cursor {
        let (prefix, rest) = split_keypath(query);
        let query_mbb = read_query_mbb(rest);
        // A keypath with no entries scans an empty bucket rather than being a
        // case of its own.
        let bucket = self.inner.multidim.get(prefix).cloned().unwrap_or_default();
        Cursor::new(Box::new(MultidimCursor::new(bucket, prefix, query_mbb)))
    }
}

// A cursor over one `Arc<BTreeMap>`. The map is a snapshot in its own right:
// writers go through `Arc::make_mut`, so entries never move out from under a
// live cursor and the slices `current` hands out stay valid until the cursor
// itself moves.
//
// `btree_map::Range` borrows the map it iterates, so a cursor that keeps a live
// range has to own the `Arc<BTreeMap>` alongside it. `self_cell!` makes that
// self-reference safe (a cursor returning owned pairs wouldn't need it, but
// `current` lends).
self_cell!(
    struct BTreeCursor {
        owner: Arc<DefaultMap>,
        #[covariant]
        dependent: RangeState,
    }
);

// Holds the live `btree_map::Range` and the entry the cursor is parked on. The
// pair is cached in `current` so `BTreeCursor::current` can hand out slices that
// borrow through the cell (and ultimately from the `Arc<BTreeMap>` owner)
// without touching the range again.
struct RangeState<'a> {
    range: btree_map::Range<'a, Vec<u8>, Vec<u8>>,
    current: Option<(&'a [u8], &'a [u8])>,
}

impl BTreeCursor {
    // Builds a cursor over the entries at or after `start`, parked on the first
    // of them so a fresh cursor is positioned rather than needing to be primed
    // by its caller.
    fn from_key(map: Arc<DefaultMap>, start: &[u8]) -> Self {
        let mut cursor = Self::new(map, |map| RangeState {
            range: map.range::<[u8], _>((Bound::Included(start), Bound::Unbounded)),
            current: None,
        });
        cursor.advance();
        cursor
    }

    // Every key sorts at or above the empty one, so an empty start bound covers
    // the whole map.
    fn unbounded(map: Arc<DefaultMap>) -> Self {
        Self::from_key(map, &[])
    }
}

impl CursorBackend for BTreeCursor {
    fn current(&self) -> Option<(&[u8], &[u8])> {
        self.borrow_dependent().current
    }

    // Steps the inner range on and caches the entry it yields. An exhausted
    // range keeps yielding `None`, so advancing past the end is a no-op.
    fn advance(&mut self) {
        self.with_dependent_mut(|_owner, state| {
            state.current = state
                .range
                .next()
                .map(|(key, value)| (key.as_slice(), value.as_slice()));
        });
    }

    fn seek(&mut self, from: SeekFrom) {
        // A `btree_map::Range` cannot be re-bounded, so seeking builds a fresh
        // one over the same map.
        let map = Arc::clone(self.borrow_owner());
        *self = match from {
            SeekFrom::Start => Self::unbounded(map),
            SeekFrom::Key(key) => Self::from_key(map, key),
        };
    }
}

// Scans one keypath bucket, skipping entries that don't intersect the query.
// `index` is always parked on a match (or past the end), which is what the
// positioned cursor contract requires.
struct MultidimCursor {
    per_keypath: Arc<PerKeypath>,
    query_mbb: QueryMbb,
    index: usize,
    // The key of the entry the cursor is parked on: the bucket's keypath
    // prefix, which never changes, with the entry's numeric fields written
    // back after it. `current` lends this out, so it is rebuilt whenever the
    // position moves.
    key: Vec<u8>,
    prefix_len: usize,
}

impl MultidimCursor {
    fn new(per_keypath: Arc<PerKeypath>, prefix: &[u8], query_mbb: QueryMbb) -> Self {
        // Sized for the whole key up front: the buffer is rebuilt in place on
        // every step, and a cursor that has to grow it does so on its first.
        let mut key = Vec::with_capacity(prefix.len() + ENTRY_MBB_BYTES);
        key.extend_from_slice(prefix);
        let mut cursor = Self {
            per_keypath,
            query_mbb,
            index: 0,
            prefix_len: key.len(),
            key,
        };
        cursor.rewind();
        cursor
    }

    // Positions on the first entry the query can match. Entries sort by seq
    // first, so everything below the query's `seq_min` is out of range and is
    // skipped without being looked at: the engine builds one cursor per call
    // into the bbox filter, each starting at the seq the previous call reached,
    // so a linear walk to that seq would scan the bucket once per call.
    fn rewind(&mut self) {
        let seq_min = self.query_mbb.seq_min;
        self.index = self
            .per_keypath
            .mbbs
            .partition_point(|entry| entry.seq < seq_min);
        self.settle();
    }

    // Moves forward from the current position to the next intersecting entry,
    // staying put if the cursor already sits on one.
    fn settle(&mut self) {
        while self.index < self.per_keypath.mbbs.len()
            && !entry_intersects(&self.per_keypath.mbbs[self.index], &self.query_mbb)
        {
            self.index += 1;
        }
        self.key.truncate(self.prefix_len);
        if let Some(entry) = self.per_keypath.mbbs.get(self.index) {
            write_entry_mbb(&mut self.key, entry);
        }
    }
}

impl CursorBackend for MultidimCursor {
    fn current(&self) -> Option<(&[u8], &[u8])> {
        let value = self.per_keypath.values.get(self.index)?;
        Some((self.key.as_slice(), value.as_slice()))
    }

    fn advance(&mut self) {
        if self.index < self.per_keypath.mbbs.len() {
            self.index += 1;
            self.settle();
        }
    }

    fn seek(&mut self, from: SeekFrom) {
        match from {
            SeekFrom::Start => self.rewind(),
            // The keypath bucket is fixed at construction time, so a key from
            // another keypath has nowhere to seek to. The engine constructs a
            // multidim cursor per query and only walks it forward.
            SeekFrom::Key(_) => {
                unimplemented!("MultidimCursor does not support seeking to a key")
            }
        }
    }
}

// Multi-dimensional key parsing.
// Key format:   `[varint keypath_len][keypath bytes][u64 seq][f64 × 4 mbb]` (40 bytes after keypath)
// Query format: `[varint keypath_len][keypath bytes][u64 seq_min][u64 seq_max][f64 × 4 mbb]` (48 bytes after keypath)
//
// Numeric fields are stored in byte-orderable encoded form (BE u64 on disk).
// Decoded entries keep them as plain `u64` in host byte order: the encoding
// is order-preserving by construction, so `u64::le` over the encoded
// representation gives the same answer as `f64::le` over the decoded values.
// That lets the intersection check skip the IEEE-754 sign-flip transform.
struct QueryMbb {
    seq_min: u64,
    seq_max: u64,
    first_min: u64,
    first_max: u64,
    second_min: u64,
    second_max: u64,
}

/// The width of the [`EntryMbb`] a multidim key ends in: the doc seq followed
/// by a `[min, max]` pair per dimension. A query's fields are eight bytes
/// wider, since its seq is a range too.
const ENTRY_MBB_BYTES: usize = 40;

// The fields are declared in the order they are encoded in, so the derived
// `Ord` — which compares them one after another — is the order the encoded
// keys sort in.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryMbb {
    seq: u64,
    first_min: u64,
    first_max: u64,
    second_min: u64,
    second_max: u64,
}

fn entry_intersects(entry: &EntryMbb, query: &QueryMbb) -> bool {
    query.seq_min <= entry.seq
        && entry.seq <= query.seq_max
        && query.first_min <= entry.first_max
        && entry.first_min <= query.first_max
        && query.second_min <= entry.second_max
        && entry.second_min <= query.second_max
}

// Splits a multidim key or query into the length-prefixed keypath — the byte
// prefix every entry for that keypath shares — and the numeric fields that
// follow it.
fn split_keypath(key: &[u8]) -> (&[u8], &[u8]) {
    let (_keypath, end) = get_length_prefixed_slice(key).expect("malformed multidim keypath");
    key.split_at(end)
}

// Splits a multidim key into the parts a bucket stores it as: the keypath
// prefix it is filed under and the entry the bucket holds.
fn split_multidim_key(key: &[u8]) -> (&[u8], EntryMbb) {
    let (prefix, mbb) = split_keypath(key);
    (prefix, read_entry_mbb(mbb))
}

fn read_entry_mbb(data: &[u8]) -> EntryMbb {
    EntryMbb {
        seq: decode_byte_orderable_u64(&data[..8]),
        first_min: decode_byte_orderable_u64(&data[8..16]),
        first_max: decode_byte_orderable_u64(&data[16..24]),
        second_min: decode_byte_orderable_u64(&data[24..32]),
        second_max: decode_byte_orderable_u64(&data[32..40]),
    }
}

// Writes the numeric fields back in the form they were read from, which is the
// tail of the key the entry was stored under.
fn write_entry_mbb(buf: &mut Vec<u8>, entry: &EntryMbb) {
    encode_byte_orderable_u64(buf, entry.seq);
    encode_byte_orderable_u64(buf, entry.first_min);
    encode_byte_orderable_u64(buf, entry.first_max);
    encode_byte_orderable_u64(buf, entry.second_min);
    encode_byte_orderable_u64(buf, entry.second_max);
}

fn read_query_mbb(data: &[u8]) -> QueryMbb {
    QueryMbb {
        seq_min: decode_byte_orderable_u64(&data[..8]),
        seq_max: decode_byte_orderable_u64(&data[8..16]),
        first_min: decode_byte_orderable_u64(&data[16..24]),
        first_max: decode_byte_orderable_u64(&data[24..32]),
        second_min: decode_byte_orderable_u64(&data[32..40]),
        second_max: decode_byte_orderable_u64(&data[40..48]),
    }
}
