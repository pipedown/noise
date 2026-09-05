use std::fmt;

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

/// Cassandra ByteComparable BIGINT-style prefix unsigned varint
///
/// The number of leading 1-bits in byte 0 (followed by a separator 0-bit)
/// names the total encoded length; the remaining bits in byte 0 plus the
/// trailing bytes (big-endian) carry the value. The all-ones byte (`0xFF`)
/// is the 9-byte form, where the value occupies the 8 trailing bytes with
/// no separator zero.
///
/// ```text
///   byte 0 pattern         total bytes   value range
///   0xxxxxxx (0x00..0x7F)  1             0..127
///   10xxxxxx (0x80..0xBF)  2             ..2^14-1
///   110xxxxx (0xC0..0xDF)  3             ..2^21-1
///   1110xxxx (0xE0..0xEF)  4             ..2^28-1
///   11110xxx (0xF0..0xF7)  5             ..2^35-1
///   111110xx (0xF8..0xFB)  6             ..2^42-1
///   1111110x (0xFC..0xFD)  7             ..2^49-1
///   11111110 (0xFE)        8             ..2^56-1
///   11111111 (0xFF)        9             ..u64::MAX
/// ```
///
/// Byte-wise lex order matches numeric order: a longer encoding has more
/// leading 1-bits in byte 0 and therefore sorts above any shorter one.
pub fn encode_varint(buf: &mut Vec<u8>, value: u64) {
    // Values too big for 56 bits use the 9-byte form: a 0xFF first byte (eight
    // leading 1-bits, i.e. "eight more bytes follow") then the value as 8
    // big-endian bytes.
    if value >> 56 != 0 {
        buf.push(0xFF);
        buf.extend_from_slice(&value.to_be_bytes());
        return;
    }

    // Number of significant bits in `value` (0 when value is 0).
    let bits = u64::BITS - value.leading_zeros();

    // Bytes needed = ceil(bits / 7), since each byte carries 7 payload bits.
    // `.max(1)` gives value 0 a single byte instead of zero.
    let len = (bits.div_ceil(7) as usize).max(1);

    // Shift the value up so its bytes occupy the top `len` bytes of the word.
    // The encoding is laid out most-significant-byte-first, so the meaningful
    // bytes belong at the high end; this also clears the top bits of byte 0 to
    // make room for the marker.
    let mut v = value << (8 * (8 - len));

    // The 9-byte form (0b1111_1111) is handled separately above.
    const MARKERS: [u8; 8] = [
        0b0000_0000,
        0b1000_0000,
        0b1100_0000,
        0b1110_0000,
        0b1111_0000,
        0b1111_1000,
        0b1111_1100,
        0b1111_1110,
    ];
    let prefix = MARKERS[len - 1];

    // Place the marker in the most-significant byte (bits 56..=63 = output byte 0).
    v |= (prefix as u64) << 56;

    buf.extend_from_slice(&v.to_be_bytes()[..len]);
}

/// Decode a value produced by [`encode_varint`]. Returns the decoded
/// value and the number of bytes consumed, or `None` on truncation.
pub fn decode_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let (&b0, rest) = bytes.split_first()?;

    // The marker is a unary length prefix: `leading_ones` continuation bytes
    // follow the first one. `get` yields None when the input is truncated.
    let leading_ones = b0.leading_ones() as usize;
    let continuation = rest.get(..leading_ones)?;

    // Data bits live in the low `7 - leading_ones` bits of b0 (and none when
    // leading_ones == 8, i.e. b0 == 0xFF). saturating_sub floors the shift at 0
    // so the 0xFF case produces an empty mask instead of underflowing.
    let shift = 7usize.saturating_sub(leading_ones);
    let mask = (1u8 << shift) - 1;

    let mut value = (b0 & mask) as u64;
    for &b in continuation {
        value = (value << 8) | b as u64;
    }

    Some((value, 1 + continuation.len()))
}

/// Append a doc seq followed by an arraypath as concatenated prefix varints.
/// Each varint is self-delimiting, so no separator bytes are required between
/// elements.
pub fn encode_seq_arraypath(buf: &mut Vec<u8>, seq: u64, arraypath: &[u64]) {
    encode_varint(buf, seq);
    for &i in arraypath {
        encode_varint(buf, i);
    }
}

/// Decode a run of varints, one after another, until `bytes` is exhausted.
pub fn decode_varints(mut bytes: &[u8]) -> Vec<u64> {
    let mut values = Vec::new();
    while !bytes.is_empty() {
        let (v, n) = decode_varint(bytes).expect("malformed varint");
        values.push(v);
        bytes = &bytes[n..];
    }
    values
}

/// Decode a sequence of varints produced by [`encode_seq_arraypath`]. The first
/// varint is the doc seq; any remaining bytes are decoded as arraypath
/// elements until the slice is exhausted.
pub fn decode_seq_arraypath(bytes: &[u8]) -> (u64, Vec<u64>) {
    let (seq, n) = decode_varint(bytes).expect("malformed seq varint");
    (seq, decode_varints(&bytes[n..]))
}

/// Append a length-prefixed slice to `buf`. The length is written as a
/// prefix varint (see [`encode_varint`]) and the slice bytes follow
/// verbatim.
pub fn put_length_prefixed_slice(buf: &mut Vec<u8>, slice: &[u8]) {
    encode_varint(buf, slice.len() as u64);
    buf.extend_from_slice(slice);
}

/// Read a slice written by [`put_length_prefixed_slice`]. Returns the slice and
/// the offset just past it, or `None` if the length varint or the payload is
/// truncated.
pub fn get_length_prefixed_slice(bytes: &[u8]) -> Option<(&[u8], usize)> {
    let (len, n) = decode_varint(bytes)?;
    // A corrupt varint can announce a length that runs past `usize`, which is
    // as truncated as any other payload that isn't there.
    let end = n.checked_add(len as usize)?;
    Some((bytes.get(n..end)?, end))
}

/// Encode a `u64` as 8 big-endian bytes. Byte-wise comparison preserves numeric
/// order for non-negative integers.
pub fn encode_byte_orderable_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Decode 8 big-endian bytes into a `u64`. Inverse of [`encode_byte_orderable_u64`].
pub fn decode_byte_orderable_u64(bytes: &[u8]) -> u64 {
    let chunk = bytes.first_chunk::<8>().expect("expected 8 bytes");
    u64::from_be_bytes(*chunk)
}

/// Encode an `f64` as 8 big-endian bytes whose lexicographic order matches
/// IEEE 754 numeric order (NaN handling left to the caller).
///
/// Trick: positive values get their sign bit flipped (so they compare greater
/// than any negative); negative values get every bit flipped (which both
/// inverts the sign and reverses the magnitude ordering, since larger negative
/// magnitudes have larger raw bit patterns).
pub fn encode_byte_orderable_f64(buf: &mut Vec<u8>, value: f64) {
    let bits = value.to_bits();
    let encoded = if bits >> 63 == 0 {
        bits ^ 0x8000_0000_0000_0000
    } else {
        !bits
    };
    buf.extend_from_slice(&encoded.to_be_bytes());
}

/// Encodes an i32 as a zigzag-mapped prefix varint (see
/// [`encode_varint`]): the value is mapped to an unsigned int
/// (0 → 0, -1 → 1, 1 → 2, …) so small magnitudes of either sign stay in the
/// 1-byte class.
pub fn encode_zigzag_i32(val: i32) -> Vec<u8> {
    let zigzag = ((val << 1) ^ (val >> 31)) as u32;
    let mut buf = Vec::with_capacity(5);
    encode_varint(&mut buf, u64::from(zigzag));
    buf
}

/// Decodes an i32 produced by [`encode_zigzag_i32`].
pub fn decode_zigzag_i32(bytes: &[u8]) -> i32 {
    let (zigzag, _) = decode_varint(bytes).expect("malformed zigzag i32 varint");
    let zigzag = zigzag as u32;
    ((zigzag >> 1) as i32) ^ -((zigzag & 1) as i32)
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
        decode_zigzag_i32(bytes)
    } else {
        0
    };

    for bytes in operands {
        count += decode_zigzag_i32(bytes);
    }
    encode_zigzag_i32(count)
}

/// Compaction-filter predicate: returns `true` if the key should be dropped.
/// Word-count / field-count keys whose merged value has dropped to zero are
/// removed; everything else is kept.
pub fn should_drop_key(_level: u32, key: &[u8], value: &[u8]) -> bool {
    if !(key[0] as char == KEY_PREFIX_WORD_COUNT || key[0] as char == KEY_PREFIX_FIELD_COUNT) {
        return false; // keep
    }
    0 == decode_zigzag_i32(value) // true = remove
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

pub trait CursorBackend {
    /// Returns the key/value at the current position without advancing, or
    /// `None` when the cursor is exhausted. The slices borrow the cursor and
    /// are invalidated by the next `advance`/`seek`.
    fn current(&self) -> Option<(&[u8], &[u8])>;
    /// Advances to the next entry. A no-op on an already-exhausted cursor.
    fn advance(&mut self);
    fn seek(&mut self, from: SeekFrom);
}

/// A forward cursor over key/value entries, positioned *on* an entry (or
/// exhausted). It is a lending cursor: [`Self::current`] borrows from the
/// cursor, so the slices are invalidated by the next [`Self::advance`]/
/// [`Self::seek`]. The `&self`/`&mut self` split lets the borrow checker
/// enforce that, and lets `current` serve as a zero-copy one-entry lookahead
/// that survives across stack frames (see `JsonFetcher::do_fetch`): no entry
/// is advanced past until it has been fully consumed.
pub struct Cursor {
    inner: Box<dyn CursorBackend>,
}

impl Cursor {
    pub fn new(inner: Box<dyn CursorBackend>) -> Cursor {
        Cursor { inner }
    }

    /// Positions the cursor and returns the entry it landed on, which is the
    /// first one at or after `from`, or `None` if there is none.
    pub fn seek(&mut self, from: SeekFrom) -> Option<(&[u8], &[u8])> {
        self.inner.seek(from);
        self.inner.current()
    }

    /// The entry at the current position, or `None` when exhausted. Does not
    /// advance, so repeated calls yield the same entry until [`Self::advance`].
    pub fn current(&self) -> Option<(&[u8], &[u8])> {
        self.inner.current()
    }

    /// Advances to the next entry.
    pub fn advance(&mut self) {
        self.inner.advance();
    }

    /// Iterates the remaining entries as owned copies. For scans that keep the
    /// data around anyway; the zero-copy path is [`Self::current`]/
    /// [`Self::advance`], which is what a lending cursor can't hand to
    /// [`Iterator`].
    pub fn entries(&mut self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        std::iter::from_fn(move || {
            let (key, value) = self.current()?;
            let entry = (key.to_vec(), value.to_vec());
            self.advance();
            Some(entry)
        })
    }

    /// Iterates the keys of the remaining entries as owned copies, leaving the
    /// values where they are. Same trade-off as [`Self::entries`].
    pub fn keys(&mut self) -> impl Iterator<Item = Vec<u8>> + '_ {
        std::iter::from_fn(move || {
            let (key, _value) = self.current()?;
            let key = key.to_vec();
            self.advance();
            Some(key)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_boundary_encodings() {
        // Byte 0 is written in binary so the unary length marker and the
        // payload bits it shares the byte with are visible; the trailing
        // payload bytes stay hex.
        let cases: &[(u64, &[u8])] = &[
            (0, &[0b0000_0000]),
            (1, &[0b0000_0001]),
            (127, &[0b0111_1111]),
            (128, &[0b1000_0000, 0x80]),
            ((1 << 14) - 1, &[0b1011_1111, 0xFF]),
            (1 << 14, &[0b1100_0000, 0x40, 0x00]),
            ((1 << 21) - 1, &[0b1101_1111, 0xFF, 0xFF]),
            (1 << 21, &[0b1110_0000, 0x20, 0x00, 0x00]),
            ((1 << 28) - 1, &[0b1110_1111, 0xFF, 0xFF, 0xFF]),
            (1 << 28, &[0b1111_0000, 0x10, 0x00, 0x00, 0x00]),
            (0xFFFF_FFFF, &[0b1111_0000, 0xFF, 0xFF, 0xFF, 0xFF]),
            ((1 << 35) - 1, &[0b1111_0111, 0xFF, 0xFF, 0xFF, 0xFF]),
            (1 << 35, &[0b1111_1000, 0x08, 0x00, 0x00, 0x00, 0x00]),
            ((1 << 42) - 1, &[0b1111_1011, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
            (1 << 42, &[0b1111_1100, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00]),
            (
                (1 << 49) - 1,
                &[0b1111_1101, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                1 << 49,
                &[0b1111_1110, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                (1 << 56) - 1,
                &[0b1111_1110, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                1 << 56,
                &[0b1111_1111, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                u64::MAX,
                &[0b1111_1111, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
        ];
        for (value, expected) in cases {
            let mut buf = Vec::new();
            encode_varint(&mut buf, *value);
            assert_eq!(&buf[..], *expected, "encoding of {} mismatch", value);
        }
    }

    #[test]
    fn varint_roundtrip() {
        // Width classes step every 7 bits, so the interesting values are the
        // largest of each width and the smallest of the next, plus a few
        // in-between magnitudes.
        let samples: &[u64] = &[
            0,
            1,
            127,
            128,
            (1 << 14) - 1,
            1 << 14,
            (1 << 21) - 1,
            1 << 21,
            0xFF_FFFF,
            (1 << 28) - 1,
            1 << 28,
            0xFFFF_FFFF,
            (1 << 35) - 1,
            1 << 35,
            (1 << 42) - 1,
            1 << 42,
            (1 << 49) - 1,
            1 << 49,
            (1 << 56) - 1,
            1 << 56,
            u64::MAX - 1,
            u64::MAX,
        ];
        for &v in samples {
            let mut buf = Vec::new();
            encode_varint(&mut buf, v);
            let (decoded, n) = decode_varint(&buf).expect("decode failed");
            assert_eq!(decoded, v, "roundtrip mismatch");
            assert_eq!(n, buf.len(), "consumed length mismatch");
        }
    }

    #[test]
    fn varint_byte_order_matches_numeric_order() {
        // A longer encoding must always sort above a shorter one, so the width
        // class edges (every 7 bits) are where this property can break.
        let mut samples: Vec<u64> = vec![
            0,
            1,
            5,
            99,
            127,
            128,
            255,
            (1 << 14) - 1,
            1 << 14,
            (1 << 21) - 1,
            1 << 21,
            1_000_000,
            u64::from(u32::MAX),
            1u64 << 32,
            1u64 << 40,
            1u64 << 48,
            1u64 << 56,
            u64::MAX - 1,
            u64::MAX,
        ];
        samples.sort();
        let mut encoded: Vec<(u64, Vec<u8>)> = samples
            .iter()
            .map(|&v| {
                let mut buf = Vec::new();
                encode_varint(&mut buf, v);
                (v, buf)
            })
            .collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let order: Vec<u64> = encoded.into_iter().map(|(v, _)| v).collect();
        assert_eq!(order, samples);
    }

    #[test]
    fn seq_arraypath_roundtrip_and_order() {
        // Listed in ascending order, so each encoding must sort strictly above
        // the one before it.
        let cases: &[(u64, &[u64])] = &[
            (0, &[]),
            (1, &[]),
            (1, &[0]),
            (1, &[0, 0]),
            (1, &[0, 1]),
            (1, &[1]),
            (10, &[]),
            (10, &[5]),
            (10, &[6]),
            (200, &[1, 2, 3]),
            (1_000_000, &[]),
        ];

        let mut previous: Option<Vec<u8>> = None;
        for &(seq, arraypath) in cases {
            let mut encoded = Vec::new();
            encode_seq_arraypath(&mut encoded, seq, arraypath);

            let (decoded_seq, decoded_arraypath) = decode_seq_arraypath(&encoded);
            assert_eq!(decoded_seq, seq, "seq roundtrip mismatch");
            assert_eq!(
                decoded_arraypath, arraypath,
                "arraypath roundtrip mismatch for seq {seq}"
            );

            if let Some(previous) = &previous {
                assert!(
                    previous < &encoded,
                    "{:?} does not sort above the preceding case",
                    (seq, arraypath)
                );
            }
            previous = Some(encoded);
        }
    }

    #[test]
    fn varint_truncation_returns_none() {
        assert!(decode_varint(&[]).is_none());

        // Byte 0 announces a total length the input doesn't have.
        assert!(decode_varint(&[0b1000_0000]).is_none());
        assert!(decode_varint(&[0b1100_0000, 0x00]).is_none());
        assert!(decode_varint(&[0b1111_1110, 0x00]).is_none());
        assert!(decode_varint(&[0b1111_1111, 0x00]).is_none());
    }

    #[test]
    fn zigzag_i32_roundtrip() {
        let samples: &[i32] = &[i32::MIN, -1_000_000, -64, -2, -1, 0, 1, 2, 63, i32::MAX];
        for &v in samples {
            assert_eq!(decode_zigzag_i32(&encode_zigzag_i32(v)), v);
        }
        // Small magnitudes of either sign stay in the 1-byte class.
        assert_eq!(encode_zigzag_i32(0), vec![0x00]);
        assert_eq!(encode_zigzag_i32(-1), vec![0x01]);
        assert_eq!(encode_zigzag_i32(1), vec![0x02]);
        assert_eq!(encode_zigzag_i32(63), vec![0x7E]);
        assert_eq!(encode_zigzag_i32(-64), vec![0x7F]);
    }

    #[test]
    fn length_prefixed_slice_encoding() {
        // A length below 128 is a 1-byte varint, so the prefix is the length.
        let mut buf = Vec::new();
        put_length_prefixed_slice(&mut buf, b"");
        assert_eq!(buf, vec![0]);

        let mut buf = Vec::new();
        put_length_prefixed_slice(&mut buf, b"abc");
        assert_eq!(buf, vec![3, b'a', b'b', b'c']);

        // A length of 240 doesn't fit a 1-byte varint, so the prefix is a
        // marker byte followed by the length.
        let payload = [0xAA; 240];
        let mut expected = vec![0b1000_0000, 240];
        expected.extend_from_slice(&payload);

        let mut buf = Vec::new();
        put_length_prefixed_slice(&mut buf, &payload);
        assert_eq!(buf, expected);
    }

    #[test]
    fn length_prefixed_slice_roundtrip() {
        // The reader must report the offset just past the payload so a caller
        // can keep parsing the rest of the key.
        for payload in [b"".as_slice(), b"abc".as_slice(), &[0xAA; 240]] {
            let mut buf = Vec::new();
            put_length_prefixed_slice(&mut buf, payload);
            // Trailing bytes stand in for the rest of a composite key.
            buf.extend_from_slice(b"trailing");

            let (slice, end) = get_length_prefixed_slice(&buf).expect("decode failed");
            assert_eq!(slice, payload);
            assert_eq!(&buf[end..], b"trailing");
        }
    }

    #[test]
    fn length_prefixed_slice_truncation_returns_none() {
        assert!(get_length_prefixed_slice(&[]).is_none());
        // Announces 3 bytes but only 2 follow.
        assert!(get_length_prefixed_slice(&[3, b'a', b'b']).is_none());
        // Announces a length that doesn't fit in a `usize` at all.
        let mut announces_u64_max = vec![0xFF; 9];
        announces_u64_max.extend_from_slice(b"abc");
        assert!(get_length_prefixed_slice(&announces_u64_max).is_none());
    }

    #[test]
    fn byte_orderable_u64_roundtrip_and_order() {
        let mut samples: Vec<u64> = vec![0, 1, 127, 128, 255, 1 << 20, u64::MAX - 1, u64::MAX];
        samples.sort();
        let mut encoded: Vec<(u64, Vec<u8>)> = samples
            .iter()
            .map(|&v| {
                let mut buf = Vec::new();
                encode_byte_orderable_u64(&mut buf, v);
                assert_eq!(decode_byte_orderable_u64(&buf), v);
                (v, buf)
            })
            .collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let order: Vec<u64> = encoded.into_iter().map(|(v, _)| v).collect();
        assert_eq!(order, samples);
    }

    #[test]
    fn byte_orderable_f64_sorts_numerically() {
        // Negatives are where the all-bits flip matters: raw IEEE-754 bit
        // patterns sort backwards for them.
        let mut samples = vec![
            f64::NEG_INFINITY,
            -1e300,
            -1.5,
            -0.0,
            0.0,
            1.5,
            1e300,
            f64::INFINITY,
        ];
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut encoded: Vec<(f64, Vec<u8>)> = samples
            .iter()
            .map(|&v| {
                let mut buf = Vec::new();
                encode_byte_orderable_f64(&mut buf, v);
                (v, buf)
            })
            .collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let order: Vec<f64> = encoded.into_iter().map(|(v, _)| v).collect();
        assert_eq!(order, samples);
    }
}
