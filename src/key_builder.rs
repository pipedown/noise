extern crate unicode_normalization;

use crate::query::DocResult;
use noise_storage::{
    decode_byte_orderable_u64, decode_seq_arraypath, decode_varint, encode_byte_orderable_f64,
    encode_byte_orderable_u64, encode_seq_arraypath, encode_varint, put_length_prefixed_slice,
    KEY_PREFIX_FIELD_COUNT, KEY_PREFIX_NUMBER, KEY_PREFIX_WORD, KEY_PREFIX_WORD_COUNT,
};
use std::str;

/// For index header. This constant isn't actually used in the code, but provided here for
/// completeness.
pub const _KEY_PREFIX_HEADER: char = 'H';
/// for specific field length. Used for relevancy scoring
pub const KEY_PREFIX_FIELD_LENGTH: char = 'L';
/// for getting the doc seq from it's id.
pub const KEY_PREFIX_ID_TO_SEQ: char = 'I';
/// for getting/scanning the all the seqs
pub const KEY_PREFIX_SEQ: char = 'S';
/// for orignal doc values for retrieving results
pub const KEY_PREFIX_VALUE: char = 'V';

pub enum Segment {
    ObjectKey(String),
    Array(u64),
}

#[derive(Debug, Clone)]
pub struct KeyBuilder {
    keypath: Vec<String>,
    pub arraypath: Vec<u64>,
}

impl KeyBuilder {
    pub fn new() -> KeyBuilder {
        KeyBuilder {
            // Magic reserve number is completely arbitrary
            keypath: Vec::with_capacity(10),
            arraypath: Vec::with_capacity(10),
        }
    }

    pub fn clear(&mut self) {
        self.keypath.clear();
        self.arraypath.clear();
    }

    /// Builds a stemmed word key for the input word and seq, using the key_path and arraypath
    /// built up internally.
    ///
    /// The returned bytes are not valid UTF-8: the seq and arraypath are appended as
    /// order-preserving varints.
    pub fn kp_word_key(&self, word: &str, seq: u64) -> Vec<u8> {
        let mut key = self.get_kp_word_only(word);
        encode_seq_arraypath(&mut key, seq, &self.arraypath);
        key
    }

    /// Appends the keypath segments (concatenated, no delimiters) to `buf`.
    fn append_keypath(&self, buf: &mut Vec<u8>) {
        for segment in &self.keypath {
            buf.extend_from_slice(segment.as_bytes());
        }
    }

    /// Returns the text-only kp_word prefix `W<keypath>!<word>#` as bytes. The trailing `#`
    /// separates the prefix from the binary seq+arraypath suffix that follows in the full key.
    pub fn get_kp_word_only(&self, word: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(100);
        key.push(KEY_PREFIX_WORD as u8);
        self.append_keypath(&mut key);
        key.push(b'!');
        key.extend_from_slice(word.as_bytes());
        key.push(b'#');
        key
    }

    /// Returns the text-only `<prefix><keypath>#` prefix used by non-word value keys
    /// (number, true, false, null, field length) as bytes. Pair with [`encode_seq_arraypath`]
    /// to build the full key.
    pub fn kp_only(&self, prefix: char) -> Vec<u8> {
        let mut key = Vec::with_capacity(100);
        key.push(prefix as u8);
        self.append_keypath(&mut key);
        key.push(b'#');
        key
    }

    pub fn kp_word_count_key(&self, word: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(100);
        key.push(KEY_PREFIX_WORD_COUNT as u8);
        self.append_keypath(&mut key);
        key.push(b'!');
        key.extend_from_slice(word.as_bytes());
        key
    }

    pub fn kp_field_count_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(100);
        key.push(KEY_PREFIX_FIELD_COUNT as u8);
        self.append_keypath(&mut key);
        key
    }

    pub fn id_to_seq_key(id: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(id.len() + 1);
        key.push(KEY_PREFIX_ID_TO_SEQ as u8);
        key.extend_from_slice(id.as_bytes());
        key
    }

    /// Build a sequence key `S<varint seq>`. The varint encoding sorts byte-wise in
    /// numeric order, so iterating `S`-prefixed keys yields seqs in ascending order.
    pub fn seq_key(seq: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(10);
        key.push(KEY_PREFIX_SEQ as u8);
        encode_varint(&mut key, seq);
        key
    }

    /// Decode a sequence key produced by [`Self::seq_key`]. Returns `None` if the key
    /// does not start with the `S` prefix or the varint is malformed.
    pub fn parse_seq_key(key: &[u8]) -> Option<u64> {
        let rest = key.strip_prefix(&[KEY_PREFIX_SEQ as u8])?;
        let (seq, n) = decode_varint(rest)?;
        if n != rest.len() {
            return None;
        }
        Some(seq)
    }

    /// Build key to query the multi-dimensional namespace.
    ///
    /// Layout: `<prefix-varint keypath_len><keypath bytes><BE u64 seq_min>
    /// <BE u64 seq_max><bbox: 4 byte-orderable f64s>`. The keypath length uses
    /// the same Cassandra ByteComparable-style prefix varint as the rocksdb
    /// fork's `GetPrefixLengthPrefixedSlice`. The seq range and bbox are
    /// encoded byte-orderably so lexicographic ordering on the key bytes
    /// matches numeric ordering on the underlying values, preserving the
    /// rtree's per-block spatial clustering on disk under RocksDB's default
    /// byte-wise comparator.
    pub fn multidim_query_key(&self, seq_min: u64, seq_max: u64, bbox: &[u8]) -> Vec<u8> {
        let mut keypath = Vec::with_capacity(100);
        self.append_keypath(&mut keypath);
        let mut key = Vec::new();
        put_length_prefixed_slice(&mut key, &keypath);
        encode_byte_orderable_u64(&mut key, seq_min);
        encode_byte_orderable_u64(&mut key, seq_max);
        key.extend_from_slice(bbox);
        key
    }

    /// Build key for the multi-dimensional namespace.
    /// The structure is a bit different from other keypath. It doesn't have a prefix as those
    /// keys are stored in a separate namespace. The Arraypath is not part of the key, but
    /// stored as value. The sequence number is encoded as integer as it is the first dimension.
    /// The second and third dimensions are the values of the bounding box.
    pub fn multidim_key(&self, seq: u64, bbox: &[u8]) -> Vec<u8> {
        let mut keypath = Vec::with_capacity(100);
        self.append_keypath(&mut keypath);
        let mut key = Vec::new();
        put_length_prefixed_slice(&mut key, &keypath);
        // The Internal Id is always only a single value, hence don't store a range, but only
        // that single value as first dimension. Encoded byte-orderably so the on-disk
        // ordering matches numeric ordering and the rtree clusters spatially.
        encode_byte_orderable_u64(&mut key, seq);
        key.extend_from_slice(bbox);
        key
    }

    /// Encode a `[x_min, y_min, x_max, y_max]` bounding box into the byte layout
    /// the multi-dimensional keys expect: one contiguous `[min, max]` range per
    /// dimension, so the x range comes first and the y range second. Index time
    /// and query time must agree on this layout, so both go through here.
    ///
    /// The values are encoded byte-orderably so the rtree's per-block MBBs
    /// cluster correctly under the default byte-wise comparator.
    pub fn encode_bbox(bbox: [f64; 4]) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(32);
        encode_byte_orderable_f64(&mut encoded, bbox[0]);
        encode_byte_orderable_f64(&mut encoded, bbox[2]);
        encode_byte_orderable_f64(&mut encoded, bbox[1]);
        encode_byte_orderable_f64(&mut encoded, bbox[3]);
        encoded
    }

    /// Returns the doc seq from a key produced by [`Self::multidim_key`]. It is
    /// the first dimension, stored byte-orderably right after the
    /// length-prefixed keypath.
    pub fn multidim_seq_from_bytes(key: &[u8]) -> u64 {
        let (keypath_len, keypath_start) =
            decode_varint(key).expect("malformed multidim keypath length");
        let seq_start = keypath_start + keypath_len as usize;
        decode_byte_orderable_u64(&key[seq_start..])
    }

    /// Build the index key that corresponds to a number primitive.
    pub fn number_key(&self, seq: u64) -> Vec<u8> {
        let mut key = self.kp_only(KEY_PREFIX_NUMBER);
        encode_seq_arraypath(&mut key, seq, &self.arraypath);
        key
    }

    /// Build the index key that corresponds to a true, false or null primitive.
    pub fn bool_null_key(&self, prefix: char, seq: u64) -> Vec<u8> {
        let mut key = self.kp_only(prefix);
        encode_seq_arraypath(&mut key, seq, &self.arraypath);
        key
    }

    /// Builds a field length key for the seq, using the key_path and arraypath
    /// built up internally.
    pub fn kp_field_length_key(&self, seq: u64) -> Vec<u8> {
        let mut key = self.kp_only(KEY_PREFIX_FIELD_LENGTH);
        encode_seq_arraypath(&mut key, seq, &self.arraypath);
        key
    }

    /// Builds a field length key for the DocResult, using the key_path
    /// built up internally and the arraypath from the DocResult.
    pub fn kp_field_length_key_from_doc_result(&self, dr: &DocResult) -> Vec<u8> {
        let mut key = self.kp_only(KEY_PREFIX_FIELD_LENGTH);
        encode_seq_arraypath(&mut key, dr.seq, &dr.arraypath);
        key
    }

    /// Append a DocResult's seq + arraypath onto an existing kp_word prefix as varints.
    pub fn add_doc_result_to_kp_word(buf: &mut Vec<u8>, dr: &DocResult) {
        encode_seq_arraypath(buf, dr.seq, &dr.arraypath);
    }

    /// Builds a value key for seq (value keys are the original json terminal value with
    /// keyed on keypath and arraypath built up internally).
    ///
    /// The returned bytes are not valid UTF-8: the seq is encoded as a varint between
    /// the `V` prefix and the `#` separator that introduces the keypath text.
    pub fn kp_value_key(&self, seq: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(100);
        key.push(KEY_PREFIX_VALUE as u8);
        encode_varint(&mut key, seq);
        key.push(b'#');
        Self::append_kp_value_no_seq(&mut key, &self.keypath, &self.arraypath);
        key
    }

    /// Returns a value key without the doc seq prepended.
    pub fn kp_value_no_seq(&self) -> String {
        let mut buf = Vec::with_capacity(100);
        Self::append_kp_value_no_seq(&mut buf, &self.keypath, &self.arraypath);
        // Keypath segments are UTF-8 and array indices are ASCII digits, so this never fails.
        String::from_utf8(buf).expect("kp_value_no_seq is valid UTF-8")
    }

    /// Returns the keypath portion (no doc seq) from a full V-key produced by
    /// [`Self::kp_value_key`]. The returned slice is the text after the `#`
    /// separator and is valid UTF-8.
    pub fn kp_value_no_seq_from_bytes(key: &[u8]) -> &str {
        debug_assert_eq!(key.first().copied(), Some(KEY_PREFIX_VALUE as u8));
        let (_seq, n) = decode_varint(&key[1..]).expect("malformed V-key seq varint");
        let after_seq = &key[1 + n..];
        debug_assert_eq!(after_seq.first().copied(), Some(b'#'));
        let suffix = &after_seq[1..];
        // The keypath portion is constructed from valid UTF-8 strings, so this is safe.
        unsafe { str::from_utf8_unchecked(suffix) }
    }

    /// parses a kp_value_key and sets the internally elements appropriately
    pub fn parse_kp_value_no_seq(&mut self, mut str: &str) {
        while let Some(tuple) = KeyBuilder::parse_first_kp_value_segment(str) {
            match tuple {
                (Segment::ObjectKey(_key), unescaped) => {
                    str = &str[unescaped.len()..];
                    self.keypath.push(unescaped);
                }
                (Segment::Array(i), unescaped) => {
                    str = &str[unescaped.len()..];
                    self.keypath.push("$".to_string());
                    self.arraypath.push(i);
                }
            }
        }
    }

    /// Build a key to a value from a DocResult
    pub fn kp_value_key_from_doc_result(&self, dr: &DocResult) -> Vec<u8> {
        let mut key = Vec::with_capacity(100);
        key.push(KEY_PREFIX_VALUE as u8);
        encode_varint(&mut key, dr.seq);
        key.push(b'#');
        Self::append_kp_value_no_seq(&mut key, &self.keypath, &dr.arraypath);
        key
    }

    fn append_kp_value_no_seq(buf: &mut Vec<u8>, keypath: &[String], arraypath: &[u64]) {
        let mut i = 0;
        for segment in keypath {
            buf.extend_from_slice(segment.as_bytes());
            if segment == "$" {
                buf.extend_from_slice(arraypath[i].to_string().as_bytes());
                i += 1;
            }
        }
    }

    // Returns true if the prefix is a prefix of the keypath, with a delimiter check on the
    // first byte after the prefix. Both arguments are kp_value_no_seq forms (or full V-keys
    // — comparison is purely byte-wise so either works).
    pub fn is_kp_value_key_prefix(prefix: &[u8], keypath: &[u8]) -> bool {
        match keypath.strip_prefix(prefix) {
            Some(stripped) => match stripped.first() {
                Some(b'.') | Some(b'$') => true,
                Some(_) => false,
                None => true,
            },
            None => false,
        }
    }

    // returns the unescaped segment as Segment and the escaped segment as a String
    pub fn parse_first_kp_value_segment(keypath: &str) -> Option<(Segment, String)> {
        let mut unescaped = String::with_capacity(50);
        // The length of the escaped sequence. It always starts with a dot '.'
        let mut len_bytes = 1;
        let mut chars = keypath.chars();

        // first char must be a . or a $ or we've exceeded the keypath
        match chars.next() {
            Some('.') => {
                loop {
                    match chars.next() {
                        Some(backslash @ '\\') => {
                            len_bytes += backslash.len_utf8();
                            if let Some(c) = chars.next() {
                                len_bytes += c.len_utf8();
                                unescaped.push(c);
                            } else {
                                panic!("Escape char found as last char in keypath");
                            }
                        }
                        Some('.') | Some('$') => {
                            break;
                        }
                        Some(c) => {
                            len_bytes += c.len_utf8();
                            unescaped.push(c);
                        }
                        None => {
                            break;
                        }
                    }
                }
                Some((
                    Segment::ObjectKey(unescaped),
                    keypath[..len_bytes].to_string(),
                ))
            }
            Some('$') => {
                let mut i = String::new();
                for c in chars {
                    if c.is_ascii_digit() {
                        i.push(c);
                    } else {
                        break;
                    }
                }
                Some((
                    Segment::Array(i.parse().unwrap()),
                    keypath[..1 + i.len()].to_string(),
                ))
            }
            Some(_) => None, // we must be past the keypath portion of string. done.
            None => None,
        }
    }

    /// Adds objet key to keypath
    pub fn push_object_key(&mut self, key: &str) {
        let mut escaped_key = String::with_capacity((key.len() * 2) + 1); // max expansion
        escaped_key.push('.');

        for cc in key.chars() {
            // Escape chars that conflict with delimiters
            if "\\$.!#".contains(cc) {
                escaped_key.push('\\');
            }
            escaped_key.push(cc);
        }
        self.keypath.push(escaped_key);
    }

    /// adds array to keypath
    pub fn push_array(&mut self) {
        self.keypath.push("$".to_string());
        self.arraypath.push(0);
    }

    /// adds array with index to keypath
    pub fn push_array_index(&mut self, index: u64) {
        self.keypath.push("$".to_string());
        self.arraypath.push(index);
    }

    /// pops last object key `{"foo":..."}` from keypath. Last segment must be object key
    pub fn pop_object_key(&mut self) {
        debug_assert!(self.keypath.last().unwrap().starts_with('.'));
        self.keypath.pop();
    }

    /// Returns the last array offset in the keypath. Last segment must be an array.
    pub fn peek_array_index(&self) -> u64 {
        debug_assert!(self.keypath.last().unwrap().starts_with('$'));
        *self.arraypath.last().unwrap()
    }

    /// pops last array segment `[N]` from keypath. Last segment must be array.
    pub fn pop_array(&mut self) {
        debug_assert!(self.keypath.last().unwrap() == "$");
        self.arraypath.pop();
        self.keypath.pop();
    }

    /// increments the last array segment by 1. LAst segment must be array,
    pub fn inc_top_array_index(&mut self) {
        if !self.keypath.is_empty() && self.keypath.last().unwrap() == "$" {
            *self.arraypath.last_mut().unwrap() += 1;
        }
    }

    /// Returns the number of arrays in the keypath
    pub fn arraypath_len(&self) -> usize {
        self.arraypath.len()
    }

    /// returns the number of segments in the keypath
    pub fn kp_segments_len(&self) -> usize {
        self.keypath.len()
    }

    /// Decode the seq + arraypath that follow a kp_word prefix. `prefix_len` is the
    /// length of that prefix (up to and including its trailing `#` separator).
    pub fn parse_doc_result_from_kp_word_key(key: &[u8], prefix_len: usize) -> DocResult {
        let (seq, arraypath) = decode_seq_arraypath(&key[prefix_len..]);
        let mut dr = DocResult::new();
        dr.seq = seq;
        dr.arraypath = arraypath;
        dr
    }
}

impl Default for KeyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_byte_orderable_u64, encode_byte_orderable_f64, encode_byte_orderable_u64, KeyBuilder,
    };
    use crate::query::DocResult;
    use noise_storage::encode_seq_arraypath;

    #[test]
    fn test_segments_push() {
        let mut kb = KeyBuilder::new();
        assert_eq!(kb.kp_segments_len(), 0, "No segments so far");

        kb.push_object_key("first");
        assert_eq!(kb.kp_segments_len(), 1, "One segment");

        kb.push_object_key("second");
        assert_eq!(kb.kp_segments_len(), 2, "Two segments");

        kb.push_array();
        assert_eq!(kb.kp_segments_len(), 3, "Three segments ");
    }

    #[test]
    fn test_segments_pop() {
        let mut kb = KeyBuilder::new();
        kb.push_object_key("first");
        kb.push_object_key("second");
        kb.push_array();

        assert_eq!(kb.kp_segments_len(), 3, "three segments");
        let mut expected: Vec<u8> = b"W.first.second$!astemmedword#".to_vec();
        encode_seq_arraypath(&mut expected, 123, &[0]);
        assert_eq!(
            kb.kp_word_key("astemmedword", 123),
            expected,
            "Key for six segments is correct"
        );

        kb.pop_array();
        assert_eq!(kb.kp_segments_len(), 2, "Two segments");

        kb.pop_object_key();
        assert_eq!(kb.kp_segments_len(), 1, "One segment");

        kb.pop_object_key();
        assert_eq!(kb.kp_segments_len(), 0, "No segments so far");
    }

    #[test]
    fn test_doc_result_parse() {
        let prefix = "W.foo$.bar$!word#";
        let mut key = prefix.as_bytes().to_vec();
        encode_seq_arraypath(&mut key, 123, &[1, 0]);
        let mut dr = DocResult::new();
        dr.seq = 123;
        dr.arraypath = vec![1, 0];

        assert!(dr == KeyBuilder::parse_doc_result_from_kp_word_key(&key, prefix.len()));
    }

    #[test]
    fn test_kp_value_key_roundtrips_delimiter_chars_in_object_key() {
        // Object keys can legitimately contain the bytes the key format uses as
        // delimiters (`#`, `.`, `$`, `!`, `\`). Escaping in `push_object_key` must let
        // them round-trip, so e.g. `{"a.b": ...}` stays one segment and isn't confused
        // with a nested `{"a": {"b": ...}}`.
        for object_key in ["fo#o", "a.b", "x$y", "ex!clam", "back\\slash"] {
            let mut kb = KeyBuilder::new();
            kb.push_object_key(object_key);
            let key = kb.kp_value_key(123);

            // Decode the keypath text out of the V-key and re-parse it into a fresh builder.
            let no_seq = KeyBuilder::kp_value_no_seq_from_bytes(&key);
            let mut decoded = KeyBuilder::new();
            decoded.parse_kp_value_no_seq(no_seq);

            // The delimiter byte stays inside a single segment, and re-encoding the
            // round-tripped builder reproduces the exact same key bytes.
            assert_eq!(
                decoded.kp_segments_len(),
                1,
                "`{object_key}` should be one keypath segment"
            );
            assert_eq!(
                decoded.kp_value_key(123),
                key,
                "`{object_key}` did not round-trip"
            );
        }
    }

    #[test]
    fn test_multidim_seq_from_bytes() {
        let mut kb = KeyBuilder::new();
        kb.push_object_key("foo");
        kb.push_object_key("bar");

        let bbox = [0xAAu8; 32];
        for seq in [0, 1, 127, 128, 1_000_000, u64::MAX] {
            let key = kb.multidim_key(seq, &bbox);
            assert_eq!(KeyBuilder::multidim_seq_from_bytes(&key), seq);
        }
    }

    #[test]
    fn test_byte_orderable_u64_roundtrip_and_order() {
        // Listed in ascending order, so each encoding must sort strictly above
        // the one before it.
        let samples = [0, 1, 127, 128, 1_000_000, u64::MAX - 1, u64::MAX];

        let mut previous: Option<Vec<u8>> = None;
        for value in samples {
            let mut encoded = Vec::new();
            encode_byte_orderable_u64(&mut encoded, value);

            assert_eq!(encoded.len(), 8);
            assert_eq!(decode_byte_orderable_u64(&encoded), value);

            if let Some(previous) = &previous {
                assert!(
                    previous < &encoded,
                    "{value} does not sort above the preceding sample"
                );
            }
            previous = Some(encoded);
        }
    }

    #[test]
    fn test_byte_orderable_f64_sorts_numerically() {
        // Listed in ascending numeric order, so each encoding must sort strictly above
        // the one before it. `-0.0` and `0.0` are numerically equal but encode
        // distinctly, with `-0.0` sorting first.
        let samples = [
            -1e308,
            -1.0,
            -0.5,
            -0.0,
            0.0,
            0.5,
            1.0,
            1e308,
            f64::INFINITY,
        ];

        let mut previous: Option<Vec<u8>> = None;
        for value in samples {
            let mut encoded = Vec::new();
            encode_byte_orderable_f64(&mut encoded, value);

            if let Some(previous) = &previous {
                assert!(
                    previous < &encoded,
                    "{value} does not sort above the preceding sample"
                );
            }
            previous = Some(encoded);
        }
    }

    #[test]
    fn test_encode_bbox_layout() {
        // Per dimension one contiguous [min, max] range: x first, then y.
        let bbox = KeyBuilder::encode_bbox([1.0, 2.0, 3.0, 4.0]);
        assert_eq!(bbox.len(), 32);

        let mut expected = Vec::new();
        for v in [1.0, 3.0, 2.0, 4.0] {
            encode_byte_orderable_f64(&mut expected, v);
        }
        assert_eq!(bbox, expected);
    }

    #[test]
    fn test_multidim_seq_from_bytes_long_keypath() {
        // A keypath long enough to need a multi-byte varint length prefix, so
        // the seq offset depends on the prefix width being read back correctly.
        let mut kb = KeyBuilder::new();
        kb.push_object_key(&"a".repeat(300));

        let key = kb.multidim_key(42, &[0xBB; 32]);
        assert_eq!(KeyBuilder::multidim_seq_from_bytes(&key), 42);
    }
}
