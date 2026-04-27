use std::cmp::Ordering;
use std::fmt;
use std::io::Cursor as IoCursor;
use std::mem;
use std::str;

use varint::{VarintRead, VarintWrite};

// Used by the comparator, merge, and compaction-filter logic below, as well as
// by the main crate's `KeyBuilder` when constructing these keys.

/// for looking up words in fields.
pub const KEY_PREFIX_WORD: char = 'W';
/// for getting the total count of a word in the index. Used for relevancy scoring.
pub const KEY_PREFIX_WORD_COUNT: char = 'C';
/// for getting the total number of field instances in the index. Used for relevancy scoring.
pub const KEY_PREFIX_FIELD_COUNT: char = 'K';
/// number values index
pub const KEY_PREFIX_NUMBER: char = 'f';
/// for true values index
pub const KEY_PREFIX_TRUE: char = 'T';
/// for false value index
pub const KEY_PREFIX_FALSE: char = 'F';
/// for null values index
pub const KEY_PREFIX_NULL: char = 'N';

/// Decodes a signed 32-bit varint from the given bytes.
pub fn convert_bytes_to_i32(bytes: &[u8]) -> i32 {
    let mut vec = Vec::with_capacity(bytes.len());
    vec.extend(bytes.iter());
    let mut read = IoCursor::new(vec);
    read.read_signed_varint_32().unwrap()
}

/// Encodes an i32 as a signed 32-bit varint.
pub fn convert_i32_to_bytes(val: i32) -> Vec<u8> {
    let mut bytes = IoCursor::new(Vec::new());
    assert!(bytes.write_signed_varint_32(val).is_ok());
    bytes.into_inner()
}

/// Return the slice that is prefixed with an unsigned 32-bit varint and the offset after
/// the slice that was read
fn get_length_prefixed_slice(data: &[u8]) -> (&[u8], usize) {
    let mut vec = Vec::with_capacity(data.len());
    vec.extend_from_slice(data);
    let mut cursor = IoCursor::new(vec);
    let size = cursor.read_unsigned_varint_32().unwrap() as usize;
    let slice_end = cursor.position() as usize + size;
    let slice = &data[cursor.position() as usize..slice_end];
    (slice, slice_end)
}

/// splits key into key path, seq and array path
/// ex "W.foo$.bar$.baz!word#123,0,0" -> ("W.foo$.bar$.bar!word", "123", "0,0")
pub fn split_seq_arraypath_from_kp_word_key(str: &str) -> (&str, &str, &str) {
    let n = str
        .rfind('#')
        .expect("kp_word key to contain a '#' separator");
    assert!(
        n != 0,
        "expected kp_word key to have a keypath before the '#' separator"
    );
    assert!(
        n != str.len() - 1,
        "expected kp_word key to have a seq/arraypath suffix after the '#' separator"
    );
    let seq_arraypath_str = &str[(n + 1)..];
    let m = seq_arraypath_str
        .find(',')
        .expect("seq/arraypath suffix to contain a ',' between seq and arraypath");

    (
        &str[..n],
        &seq_arraypath_str[..m],
        &seq_arraypath_str[m + 1..],
    )
}

/// Noise comparator for value keys (operating on byte slices). Value keys are
/// compared using a numeric-aware collation on the seq and arraypath portions.
/// All other keys are compared byte-wise.
#[allow(clippy::collapsible_else_if)]
pub fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    let value_prefixes = [
        KEY_PREFIX_WORD,
        KEY_PREFIX_NUMBER,
        KEY_PREFIX_TRUE,
        KEY_PREFIX_FALSE,
        KEY_PREFIX_NULL,
    ];
    if !(value_prefixes.contains(&(a[0] as char)) && value_prefixes.contains(&(b[0] as char))) {
        return a.cmp(b);
    }

    let akey = unsafe { str::from_utf8_unchecked(a) };
    let bkey = unsafe { str::from_utf8_unchecked(b) };
    let (apath_str, aseq_str, aarraypath_str) = split_seq_arraypath_from_kp_word_key(akey);
    let (bpath_str, bseq_str, barraypath_str) = split_seq_arraypath_from_kp_word_key(bkey);

    match apath_str[0..].cmp(&bpath_str[0..]) {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        Ordering::Equal => {
            let aseq: u64 = aseq_str.parse().unwrap();
            let bseq: u64 = bseq_str.parse().unwrap();
            match aseq.cmp(&bseq) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => {
                    if aarraypath_str.is_empty() || barraypath_str.is_empty() {
                        aarraypath_str.len().cmp(&barraypath_str.len())
                    } else {
                        let mut a_nums = aarraypath_str.split(',');
                        let mut b_nums = barraypath_str.split(',');
                        loop {
                            if let Some(a_num_str) = a_nums.next() {
                                if let Some(b_num_str) = b_nums.next() {
                                    let a_num: u64 = a_num_str.parse().unwrap();
                                    let b_num: u64 = b_num_str.parse().unwrap();
                                    match a_num.cmp(&b_num) {
                                        Ordering::Less => return Ordering::Less,
                                        Ordering::Greater => return Ordering::Greater,
                                        Ordering::Equal => (),
                                    }
                                } else {
                                    //b is shorter than a, so greater
                                    return Ordering::Greater;
                                }
                            } else {
                                if b_nums.next().is_some() {
                                    //a is shorter than b so less
                                    return Ordering::Less;
                                } else {
                                    // same length and must have hit all equal before this,
                                    // so equal
                                    return Ordering::Equal;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Compare function for the keys in the multi-dimensional namespace.
/// The keys have a length prefixed string (the Keypath), followed by the Internal Id and
/// the bounding box around the geometry.
pub fn compare_keys_multidim(aa: &[u8], bb: &[u8]) -> Ordering {
    if aa.is_empty() && bb.is_empty() {
        return Ordering::Equal;
    } else if aa.is_empty() {
        return Ordering::Less;
    } else if bb.is_empty() {
        return Ordering::Greater;
    }

    let (keypath_aa, offset_aa) = get_length_prefixed_slice(aa);
    let (keypath_bb, offset_bb) = get_length_prefixed_slice(bb);

    // The ordering of the keypath doesn't need to be unicode collated. The ordering
    // doesn't really matters, it only matters that it's always the same.
    let keypath_compare = keypath_aa.cmp(keypath_bb);
    if keypath_compare != Ordering::Equal {
        return keypath_compare;
    }

    // Keypaths are the same, compare the Internal Ids value
    let seq_aa = unsafe {
        let array = *(aa[(offset_aa)..].as_ptr() as *const [_; 8]);
        u64::from_ne_bytes(array)
    };
    let seq_bb = unsafe {
        let array = *(bb[(offset_bb)..].as_ptr() as *const [_; 8]);
        u64::from_ne_bytes(array)
    };
    let seq_compare = seq_aa.cmp(&seq_bb);
    if seq_compare != Ordering::Equal {
        return seq_compare;
    }

    // Internal Ids are the same, compare the bounding box
    let bbox_aa = unsafe {
        let array = *(aa[(offset_aa + 8)..].as_ptr() as *const [_; 32]);
        mem::transmute::<[u8; 32], [f64; 4]>(array)
    };
    let bbox_bb = unsafe {
        let array = *(bb[(offset_bb + 8)..].as_ptr() as *const [_; 32]);
        mem::transmute::<[u8; 32], [f64; 4]>(array)
    };

    for (value_aa, value_bb) in bbox_aa.iter().zip(bbox_bb.iter()) {
        let value_compare = value_aa.partial_cmp(value_bb).unwrap();
        if value_compare != Ordering::Equal {
            return value_compare;
        }
    }
    // No early return, the values are fully equal
    Ordering::Equal
}

/// Merge function that sums up the signed 32-bit varint values stored for
/// `KEY_PREFIX_WORD_COUNT` and `KEY_PREFIX_FIELD_COUNT` keys.
pub fn sum_merge(
    new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut dyn Iterator<Item = &[u8]>,
) -> Vec<u8> {
    if !(new_key[0] as char == KEY_PREFIX_FIELD_COUNT
        || new_key[0] as char == KEY_PREFIX_WORD_COUNT)
    {
        panic!("unknown key type to merge!");
    }

    let mut count = if let Some(bytes) = existing_val {
        convert_bytes_to_i32(bytes)
    } else {
        0
    };

    for bytes in operands {
        count += convert_bytes_to_i32(bytes);
    }
    convert_i32_to_bytes(count)
}

/// Compaction-filter predicate: returns `true` if the key should be dropped.
/// Word-count / field-count keys whose merged value has dropped to zero are
/// removed; everything else is kept.
pub fn should_drop_key(_level: u32, key: &[u8], value: &[u8]) -> bool {
    if !(key[0] as char == KEY_PREFIX_WORD_COUNT || key[0] as char == KEY_PREFIX_FIELD_COUNT) {
        return false; // keep
    }
    0 == convert_bytes_to_i32(value) // true = remove
}

/// Configuration for opening a database.
pub struct DatabaseConfig {
    pub create_if_missing: bool,
}

/// Namespace for column family abstraction.
#[derive(Clone, Copy)]
pub enum Namespace {
    Default,
    Multidim,
}

/// Seek position for cursors.
pub enum SeekFrom<'a> {
    Start,
    Key(&'a [u8]),
}

/// Storage error wrapping backend errors.
#[derive(Debug)]
pub struct StorageError(pub String);

impl StorageError {
    /// Build a `StorageError` from any `Display`-able value.
    ///
    /// Intended for use as `.map_err(StorageError::new)` to convert a backend
    /// error into a `StorageError` without naming the backend error type.
    pub fn new(err: impl fmt::Display) -> Self {
        StorageError(err.to_string())
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Storage error: {}", self.0)
    }
}

impl std::error::Error for StorageError {}

pub trait BackendDatabase: Sized {
    type Batch: BackendBatch;
    type Snapshot: BackendSnapshot + 'static;

    fn open(path: &str, config: &DatabaseConfig) -> Result<Self, StorageError>;
    fn destroy(path: &str) -> Result<(), StorageError>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    fn write(&self, batch: Self::Batch) -> Result<(), StorageError>;
    fn new_batch(&self) -> Self::Batch;
    fn snapshot(&self) -> Self::Snapshot;
    fn iterator(&self) -> Cursor;
    fn compact(&self);
}

pub trait BackendBatch {
    fn put(&mut self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    fn delete(&mut self, ns: Namespace, key: &[u8]) -> Result<(), StorageError>;
    fn merge(&mut self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
}

pub trait BackendSnapshot {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn iterator(&self) -> Cursor;
    fn multidim_iterator(&self, query: &[u8]) -> Cursor;
}

/// Key-value pair type returned by cursor iteration.
pub type KvPair = (Box<[u8]>, Box<[u8]>);

pub trait CursorBackend {
    fn next(&mut self) -> Option<KvPair>;
    fn seek(&mut self, from: SeekFrom);
}

pub struct Cursor {
    inner: Box<dyn CursorBackend>,
}

impl Cursor {
    pub fn new(inner: Box<dyn CursorBackend>) -> Cursor {
        Cursor { inner }
    }

    pub fn seek(&mut self, from: SeekFrom) {
        self.inner.seek(from);
    }
}

impl Iterator for Cursor {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
