use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::{self, str};

use hifijson::num::LexWrite as _;
use hifijson::str::LexAlloc as _;
use hifijson::token::Lex as _;
use hifijson::{Expect, SliceLexer};

use crate::error::Error;
use crate::key_builder::KeyBuilder;
use crate::stems::Stems;
use noise_storage::{encode_varint, encode_zigzag_i32, BackendBatch, Namespace};

/// Converts a JSON number. Its integer part has to fit into a `u64` and its value into a finite
/// `f64`. Negative zero is returned as zero.
pub(crate) fn parse_number(literal: &str) -> Option<f64> {
    let integer_end = literal.find(['.', 'e', 'E']).unwrap_or(literal.len());
    literal[..integer_end]
        .trim_start_matches('-')
        .parse::<u64>()
        .ok()?;
    let number: f64 = literal
        .parse()
        .ok()
        .filter(|value: &f64| value.is_finite())?;
    // `-0.0 == 0.0`, hence this also turns negative zero into zero.
    Some(if number == 0.0 { 0.0 } else { number })
}

/// Key-value pairs read from disk for an existing document. The key is the full V-key
/// (`V<varint seq>#<keypath>`), which is not valid UTF-8 because of the binary varint.
pub(crate) type KeyValues = BTreeMap<Vec<u8>, Vec<u8>>;

/// Key-value pairs collected during shredding. The key is a `kp_value_no_seq` keypath
/// string (text only, no doc seq prefix); the value carries a one-byte type tag.
type ShreddedKeyValues = BTreeMap<String, Vec<u8>>;

// `GeometryCollection` is left out as we will process the individual geometries of it
const GEOJSON_TYPES: &[&str] = &[
    "Point",
    "MultiPoint",
    "LineString",
    "MultiLineString",
    "Polygon",
    "MultiPolygon",
];

enum ObjectKeyTypes<'a> {
    /// _id field
    Id,
    /// Normal key
    Key(&'a str),
    /// No key found
    NoKey,
}

/// The maximal nesting depth of objects and arrays in a document. Shredding recurses once per
/// level, so this limit protects the stack.
const MAX_DEPTH: usize = 128;

#[derive(Debug)]
pub struct Shredder {
    kb: KeyBuilder,
    doc_id: Option<String>,
    object_keys_indexed: Vec<bool>,
    shredded_key_values: ShreddedKeyValues,
    existing_key_value_to_delete: KeyValues,
    // Whether the current object is a GeoJSON geometry or not. It's a counter that increases
    // the more of the geometry was seen. It is increased for the following cases:
    //  - a `type` field with a valid GeoJSON geometry type as value
    //  - a `coordinates` field with an array with numbers. It is not verified if the coordinates
    //    actually match the type.
    // So if it is equal to 2, it is considered being a proper GeoJSON geometry
    maybe_geometry: u8,
    // Currently only 2D GeoJSON is supported. Hence only distinguish between first and other
    // coordinates
    is_first: bool,
    bounding_box: [f64; 4],
}

impl Shredder {
    pub fn new() -> Shredder {
        Shredder {
            kb: KeyBuilder::new(),
            doc_id: None,
            object_keys_indexed: Vec::new(),
            shredded_key_values: BTreeMap::new(),
            existing_key_value_to_delete: BTreeMap::new(),
            maybe_geometry: 0,
            is_first: true,
            bounding_box: [f64::MAX, f64::MAX, f64::MIN, f64::MIN],
        }
    }

    // Based on https://stackoverflow.com/questions/29037033/how-to-slice-a-large-veci32-as-u8
    // (2017-07-26)
    fn as_u8_slice(v: &[u64]) -> &[u8] {
        unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
    }

    fn add_multidim_entries<B: BackendBatch>(
        kb: &mut KeyBuilder,
        bbox: &[u8],
        docseq: u64,
        batch: &mut B,
        delete: bool,
    ) -> Result<(), Error> {
        // Add/delete the key that is used for multi-dimensional lookups
        let key = kb.multidim_key(docseq, bbox);
        if delete {
            batch.delete(Namespace::Multidim, key.as_slice())?;
        } else {
            batch.put(
                Namespace::Multidim,
                key.as_slice(),
                Shredder::as_u8_slice(kb.arraypath.as_slice()),
            )?;
        }

        Ok(())
    }

    fn add_number_entries<B: BackendBatch>(
        kb: &mut KeyBuilder,
        number: &[u8],
        docseq: u64,
        batch: &mut B,
        delete: bool,
    ) -> Result<(), Error> {
        // Add/delete the key that is used for range lookups
        let number_key = kb.number_key(docseq);
        if delete {
            batch.delete(Namespace::Default, &number_key)?;
        } else {
            // The number contains the `f` prefix
            batch.put(Namespace::Default, &number_key, &number[1..])?;
        }

        Ok(())
    }

    fn add_bool_null_entries<B: BackendBatch>(
        kb: &mut KeyBuilder,
        prefix: char,
        docseq: u64,
        batch: &mut B,
        delete: bool,
    ) -> Result<(), Error> {
        let key = kb.bool_null_key(prefix, docseq);
        if delete {
            batch.delete(Namespace::Default, &key)?;
        } else {
            // No need to store any value as the key already contains it
            batch.put(Namespace::Default, &key, &[])?;
        }

        Ok(())
    }

    fn add_stemmed_entries<B: BackendBatch>(
        kb: &mut KeyBuilder,
        text: &str,
        docseq: u64,
        batch: &mut B,
        delete: bool,
    ) -> Result<(), Error> {
        let stems = Stems::new(text);
        let mut word_to_word_positions = HashMap::new();
        let mut total_words: i32 = 0;

        let num = if delete { -1 } else { 1 };
        let one_enc_bytes = encode_zigzag_i32(num);

        for stem in stems {
            total_words += 1;
            let &mut (ref mut word_positions, ref mut count) = word_to_word_positions
                .entry(stem.stemmed)
                .or_insert((Vec::new(), 0));
            if !delete {
                encode_varint(word_positions, u64::from(stem.word_pos));
            }
            *count += 1;
        }

        for (stemmed, (word_positions, count)) in word_to_word_positions {
            let key = kb.kp_word_key(&stemmed, docseq);
            if delete {
                batch.delete(Namespace::Default, &key)?;
            } else {
                batch.put(Namespace::Default, &key, &word_positions)?;
            }

            let key = kb.kp_field_length_key(docseq);
            if delete {
                batch.delete(Namespace::Default, &key)?;
            } else {
                batch.put(Namespace::Default, &key, &encode_zigzag_i32(total_words))?;
            }

            let key = kb.kp_word_count_key(&stemmed);
            if delete {
                batch.merge(&key, &encode_zigzag_i32(-count))?;
            } else {
                batch.merge(&key, &encode_zigzag_i32(count))?;
            }

            let key = kb.kp_field_count_key();
            batch.merge(&key, &one_enc_bytes)?;
        }

        Ok(())
    }

    fn calc_mbb(&mut self, value: f64) {
        if self.maybe_geometry > 0 {
            if self.is_first {
                self.bounding_box[0] = self.bounding_box[0].min(value);
                self.bounding_box[2] = self.bounding_box[0].max(value);
            } else {
                self.bounding_box[1] = self.bounding_box[1].min(value);
                self.bounding_box[3] = self.bounding_box[3].max(value);
            }
            self.is_first = !self.is_first;
        }
    }

    fn add_value(&mut self, code: char, value: &[u8]) -> Result<(), Error> {
        let key = self.kb.kp_value_no_seq();
        let mut buffer = Vec::with_capacity(value.len() + 1);
        buffer.push(code as u8);
        (&mut buffer as &mut dyn Write).write_all(value)?;
        self.shredded_key_values.insert(key, buffer);
        Ok(())
    }

    fn maybe_add_value(
        &mut self,
        key: Option<&str>,
        code: char,
        value: &[u8],
    ) -> Result<(), Error> {
        match self.extract_key(key) {
            ObjectKeyTypes::Id => {
                if code != 's' && self.kb.kp_segments_len() == 1 {
                    //nested fields can be _id, not root fields
                    return Err(Error::Shred(
                        "Expected string for `_id` field, got another type".to_string(),
                    ));
                }
                self.doc_id = Some(unsafe { str::from_utf8_unchecked(value) }.to_string());
                self.kb.pop_object_key();
                self.kb.push_object_key("_id");
                *self.object_keys_indexed.last_mut().unwrap() = true;
                self.add_value(code, value)?;
            }
            ObjectKeyTypes::Key(object_key) => {
                if object_key == "type"
                    && code == 's'
                    && GEOJSON_TYPES.iter().any(|tt| tt.as_bytes() == value)
                {
                    self.maybe_geometry += 1;
                }
                // Pop the dummy object that makes `end_object` happy
                // or the previous object key
                self.kb.pop_object_key();
                self.kb.push_object_key(object_key);
                *self.object_keys_indexed.last_mut().unwrap() = true;
                self.add_value(code, value)?;
            }
            ObjectKeyTypes::NoKey => {
                self.add_value(code, value)?;
                self.kb.inc_top_array_index();
            }
        }
        Ok(())
    }

    // Extract key if it exists and indicates if it's a special type of key
    // It is called when the value is a primitive type. If it is an array or object, then
    // `maybe_push_key` is called.
    fn extract_key<'a>(&self, key: Option<&'a str>) -> ObjectKeyTypes<'a> {
        match key {
            Some(object_key) => {
                if self.kb.kp_segments_len() == 1 && object_key == "_id" {
                    ObjectKeyTypes::Id
                } else {
                    ObjectKeyTypes::Key(object_key)
                }
            }
            None => ObjectKeyTypes::NoKey,
        }
    }

    // If we are inside an object we need to push the key to the key builder
    // Don't push them if they are reserved fields (starting with underscore)
    // It is called when the value is an array or object. If it is a primitive type, then
    // `extract_key` is called.
    fn maybe_push_key(&mut self, key: Option<&str>) -> Result<(), Error> {
        match key {
            Some(object_key) => {
                if self.kb.kp_segments_len() == 1 && object_key == "_id" {
                    return Err(Error::Shred(
                        "Expected string for `_id` field, got another type".to_string(),
                    ));
                } else {
                    // Pop the dummy object that makes `end_object` happy
                    // or the previous object key
                    self.kb.pop_object_key();
                    self.kb.push_object_key(object_key);
                    *self.object_keys_indexed.last_mut().unwrap() = true;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn add_all_to_batch<B: BackendBatch>(
        &mut self,
        seq: u64,
        batch: &mut B,
    ) -> Result<(), Error> {
        for (key, value) in &self.existing_key_value_to_delete {
            self.kb.clear();
            self.kb
                .parse_kp_value_no_seq(KeyBuilder::kp_value_no_seq_from_bytes(key));
            match value[0] as char {
                's' => {
                    let text = unsafe { str::from_utf8_unchecked(&value[1..]) };
                    Shredder::add_stemmed_entries(&mut self.kb, text, seq, batch, true)?;
                }
                'f' => {
                    Shredder::add_number_entries(&mut self.kb, value, seq, batch, true)?;
                }
                'T' | 'F' | 'N' => {
                    Shredder::add_bool_null_entries(
                        &mut self.kb,
                        value[0] as char,
                        seq,
                        batch,
                        true,
                    )?;
                }
                'r' => {
                    Shredder::add_multidim_entries(&mut self.kb, &value[1..], seq, batch, true)?;
                }
                _ => {}
            }
            // If it was a bounding box for the multi-dimensional namespace, it's not part of the
            // shredded original JSON document
            if value[0] as char != 'r' {
                batch.delete(Namespace::Default, key)?;
            }
        }
        self.existing_key_value_to_delete = BTreeMap::new();

        for (key, value) in &self.shredded_key_values {
            self.kb.clear();
            self.kb.parse_kp_value_no_seq(key);
            match value[0] as char {
                's' => {
                    let text = unsafe { str::from_utf8_unchecked(&value[1..]) };
                    Shredder::add_stemmed_entries(&mut self.kb, text, seq, batch, false)?;
                }
                'f' => {
                    Shredder::add_number_entries(&mut self.kb, value, seq, batch, false)?;
                }
                'T' | 'F' | 'N' => {
                    Shredder::add_bool_null_entries(
                        &mut self.kb,
                        value[0] as char,
                        seq,
                        batch,
                        false,
                    )?;
                }
                'r' => {
                    Shredder::add_multidim_entries(&mut self.kb, &value[1..], seq, batch, false)?;
                }
                _ => {}
            }
            // If it was a bounding box for the multi-dimensional namespace, it's not a value meant
            // to be part of the shredded original JSON document
            if value[0] as char != 'r' {
                let key = self.kb.kp_value_key(seq);
                batch.put(Namespace::Default, &key, value.as_ref())?;
            }
        }
        self.shredded_key_values = BTreeMap::new();

        let key = KeyBuilder::id_to_seq_key(self.doc_id.as_ref().unwrap());
        batch.put(Namespace::Default, &key, seq.to_string().as_bytes())?;

        let key = KeyBuilder::seq_key(seq);
        batch.put(Namespace::Default, &key, b"")?;

        Ok(())
    }

    pub fn delete_existing_doc<B: BackendBatch>(
        &mut self,
        docid: &str,
        seq: u64,
        existing: KeyValues,
        batch: &mut B,
    ) -> Result<(), Error> {
        self.doc_id = Some(docid.to_string());
        for (key, value) in existing.into_iter() {
            self.kb.clear();
            self.kb
                .parse_kp_value_no_seq(KeyBuilder::kp_value_no_seq_from_bytes(&key));
            match value[0] as char {
                's' => {
                    let text = unsafe { str::from_utf8_unchecked(&value[1..]) };
                    Shredder::add_stemmed_entries(&mut self.kb, text, seq, batch, true)?;
                }
                'f' => {
                    Shredder::add_number_entries(&mut self.kb, &value, seq, batch, true)?;
                }
                'T' | 'F' | 'N' => {
                    Shredder::add_bool_null_entries(
                        &mut self.kb,
                        value[0] as char,
                        seq,
                        batch,
                        true,
                    )?;
                }
                _ => {}
            }
            batch.delete(Namespace::Default, &key)?;
        }
        let key = KeyBuilder::id_to_seq_key(self.doc_id.as_ref().unwrap());
        batch.delete(Namespace::Default, &key)?;

        let key = KeyBuilder::seq_key(seq);
        batch.delete(Namespace::Default, &key)?;
        Ok(())
    }

    pub fn merge_existing_doc(&mut self, existing: KeyValues) {
        // we found doc with the same id already stored on disk. We need to delete
        // the doc. But any fields that are the same we can just keep around
        // and don't even need to reindex.
        for (existing_key, existing_value) in existing {
            let matches = {
                let key = KeyBuilder::kp_value_no_seq_from_bytes(&existing_key);
                if let Some(new_value) = self.shredded_key_values.get(key) {
                    *new_value == existing_value
                } else {
                    false
                }
            };
            if matches {
                // we don't need to write or index these values, they already exist!
                let key = KeyBuilder::kp_value_no_seq_from_bytes(&existing_key);
                self.shredded_key_values.remove(key).unwrap();
            } else {
                // we need to delete these keys and the index keys assocaited with the valuess
                self.existing_key_value_to_delete
                    .insert(existing_key, existing_value);
            }
        }
    }

    pub fn add_id(&mut self, id: &str) -> Result<(), Error> {
        self.doc_id = Some(id.to_string());
        self.kb.clear();
        self.kb.push_object_key("_id");
        self.add_value('s', id.as_bytes())?;
        Ok(())
    }

    pub fn shred(&mut self, json: &str) -> Result<Option<String>, Error> {
        let mut document = SliceLexer::new(json.as_bytes());
        document
            .exactly_one(SliceLexer::ws_peek, |start, lexer| {
                self.shred_value(MAX_DEPTH, None, start, lexer)
            })
            .map_err(|error| match error {
                Error::Shred(message) => {
                    let position = json.len() - document.as_slice().len();
                    Error::Shred(format!("{} at byte {}", message, position))
                }
                other => other,
            })?;
        Ok(self.doc_id.clone())
    }

    /// Shreds the value whose first byte is `start`, together with all values within it.
    /// `depth` is the number of levels of objects and arrays that may still be opened, including
    /// the value itself if it is one.
    fn shred_value(
        &mut self,
        depth: usize,
        key: Option<&str>,
        start: u8,
        lexer: &mut SliceLexer,
    ) -> Result<(), Error> {
        match start {
            b'{' => {
                let inner_depth = depth.checked_sub(1).ok_or(hifijson::Error::Depth)?;
                self.start_object(key)?;
                lexer
                    .discarded()
                    .seq(b'}', SliceLexer::ws_peek, |key_start, member_lexer| {
                        member_lexer
                            .expect(|_| Some(key_start), b'"')
                            .ok_or(Expect::String)?;
                        let member_key = member_lexer.str_string().map_err(hifijson::Error::Str)?;
                        member_lexer
                            .expect(SliceLexer::ws_peek, b':')
                            .ok_or(Expect::Colon)?;
                        let value_start = member_lexer.ws_peek().ok_or(Expect::Value)?;
                        self.shred_value(inner_depth, Some(&member_key), value_start, member_lexer)
                    })?;
                self.end_object()
            }
            b'[' => {
                let inner_depth = depth.checked_sub(1).ok_or(hifijson::Error::Depth)?;
                self.start_array(key)?;
                lexer
                    .discarded()
                    .seq(b']', SliceLexer::ws_peek, |value_start, member_lexer| {
                        self.shred_value(inner_depth, None, value_start, member_lexer)
                    })?;
                self.end_array()
            }
            b'"' => {
                let value = lexer
                    .discarded()
                    .str_string()
                    .map_err(hifijson::Error::Str)?;
                self.maybe_add_value(key, 's', value.as_bytes())
            }
            b'0'..=b'9' | b'-' => {
                let (literal, _parts) = lexer
                    .num_string()
                    .validated()
                    .map_err(hifijson::Error::Num)?;
                let number = parse_number(literal)
                    .ok_or_else(|| Error::Shred(format!("invalid number {}", literal)))?;
                self.calc_mbb(number);
                self.maybe_add_value(key, 'f', &number.to_le_bytes())
            }
            b'a'..=b'z' => {
                let code = match lexer.null_or_bool().ok_or(Expect::Value)? {
                    Some(true) => 'T',
                    Some(false) => 'F',
                    None => 'N',
                };
                self.maybe_add_value(key, code, &[])
            }
            _ => Err(Expect::Value.into()),
        }
    }

    fn start_object(&mut self, key: Option<&str>) -> Result<(), Error> {
        self.maybe_push_key(key)?;
        // Just push something to make `end_object` happy
        self.kb.push_object_key("");
        self.object_keys_indexed.push(false);
        Ok(())
    }

    fn start_array(&mut self, key: Option<&str>) -> Result<(), Error> {
        if key == Some("coordinates") {
            self.maybe_geometry += 1;
        }
        self.maybe_push_key(key)?;
        self.kb.push_array();
        Ok(())
    }

    fn end_object(&mut self) -> Result<(), Error> {
        self.kb.pop_object_key();
        if self.kb.kp_segments_len() > 0 && !self.object_keys_indexed.pop().unwrap() {
            // this means we never wrote a key because the object was empty.
            // So preserve the empty object by writing a special value.
            // but not for the root object. it will always have _id field added.
            self.add_value('o', &[])?;
        }
        if self.maybe_geometry == 2 {
            let encoded_bbox = KeyBuilder::encode_bbox(self.bounding_box);
            let _ = self.add_value('r', encoded_bbox.as_slice());
        }
        // Reset the values as it either wasn't a valid geometry, or it was already
        // succcessfully processed
        self.maybe_geometry = 0;
        self.bounding_box = [f64::MAX, f64::MAX, f64::MIN, f64::MIN];

        self.kb.inc_top_array_index();
        Ok(())
    }

    fn end_array(&mut self) -> Result<(), Error> {
        let was_empty = self.kb.peek_array_index() == 0;
        self.kb.pop_array();
        if was_empty {
            // this means we never wrote a value because the array was empty.
            // So preserve the empty array by writing a special value.
            self.add_value('a', &[])?;
        }
        self.kb.inc_top_array_index();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str;

    use crate::index::{Index, OpenOptions};
    use crate::json_value::JsonValue;
    use crate::snapshot::JsonFetcher;
    use crate::test_backend::Database;
    use noise_storage::BackendDatabase;

    type Idx = Index<Database>;

    /// Decodes word-index keys into (kp_word_text_prefix, seq, arraypath, positions). The
    /// prefix is the text portion through the trailing `#`; the seq and arraypath are
    /// decoded from the binary suffix.
    fn positions_from_db(db: &Database) -> Vec<(String, u64, Vec<u64>, Vec<u32>)> {
        use noise_storage::decode_seq_arraypath;
        let mut iter = db.iterator();
        iter.entries()
            .filter(|(key, _value)| key.first() == Some(&b'W'))
            .map(|(key, value)| {
                let positions: Vec<u32> = noise_storage::decode_varints(&value)
                    .into_iter()
                    .map(|p| p as u32)
                    .collect();
                // The keypath escapes special bytes with a leading `\`. Scan for the separator
                // `#`, skipping the byte after each `\` so an escaped `#` (or `\`) isn't taken
                // as the separator.
                let mut hash_pos = 0;
                loop {
                    match key[hash_pos] {
                        b'#' => break,
                        b'\\' => hash_pos += 2,
                        _ => hash_pos += 1,
                    }
                }
                let prefix = unsafe { str::from_utf8_unchecked(&key[..=hash_pos]) }.to_string();
                let (seq, arraypath) = decode_seq_arraypath(&key[hash_pos + 1..]);
                (prefix, seq, arraypath, positions)
            })
            .collect()
    }

    fn values_from_db(db: &Database) -> Vec<(u64, String, JsonValue)> {
        let mut iter = db.iterator();
        iter.entries()
            .filter(|(key, _value)| key.first() == Some(&b'V'))
            .map(|(key, value)| {
                let (seq, n) = noise_storage::decode_varint(&key[1..]).unwrap();
                let kp = unsafe { str::from_utf8_unchecked(&key[1 + n + 1..]) }.to_string();
                (seq, kp, JsonFetcher::bytes_to_json_value(&value))
            })
            .collect()
    }

    #[test]
    fn test_shred_nested() {
        let mut shredder = super::Shredder::new();
        let json = r#"{"some": ["array", "data", ["also", "nested"]]}"#;
        let docseq = 123;
        let dbname = "target/tests/test_shred_nested";
        let _ = Idx::drop(dbname);
        let index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = index.db.new_batch();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder.add_all_to_batch(docseq, &mut batch).unwrap();

        index.write_batch(batch).unwrap();
        let result = positions_from_db(&index.db);

        let expected = vec![
            ("W._id!foo#".to_string(), 123, vec![], vec![0]),
            ("W.some$!array#".to_string(), 123, vec![0], vec![0]),
            ("W.some$!data#".to_string(), 123, vec![1], vec![0]),
            ("W.some$$!also#".to_string(), 123, vec![2, 0], vec![0]),
            ("W.some$$!nest#".to_string(), 123, vec![2, 1], vec![0]),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_shred_double_nested() {
        let mut shredder = super::Shredder::new();
        let json = r#"{"a":{"a":"b"}}"#;
        let docseq = 123;
        let dbname = "target/tests/test_shred_double_nested";
        let _ = Idx::drop(dbname);
        let index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = index.db.new_batch();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder.add_all_to_batch(docseq, &mut batch).unwrap();

        index.write_batch(batch).unwrap();
        let result = values_from_db(&index.db);

        let expected = vec![
            (
                123,
                "._id".to_string(),
                JsonValue::String("foo".to_string()),
            ),
            (123, ".a.a".to_string(), JsonValue::String("b".to_string())),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    // NOTE vmx 2016-12-06: This test is intentionally made to fail (hence ignored) as the current
    // current tokenizer does the wrong thing when it comes to numbers within words. It's left
    // here as a reminder to fix that
    #[ignore]
    fn test_shred_objects() {
        let mut shredder = super::Shredder::new();
        let json = r#"{"A":[{"B":"B2VMX two three","C":"..C2"},{"B": "b1","C":"..C2"}]}"#;
        let docseq = 1234;
        let dbname = "target/tests/test_shred_objects";
        let _ = Idx::drop(dbname);

        let index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = index.db.new_batch();
        shredder.shred(json).unwrap();
        shredder.add_all_to_batch(docseq, &mut batch).unwrap();

        index.write_batch(batch).unwrap();
        let result = positions_from_db(&index.db);
        let expected = vec![
            ("W.A$.B!b1#".to_string(), 1234, vec![1], vec![0]),
            ("W.A$.B!b2vmx#".to_string(), 1234, vec![0], vec![0]),
            ("W.A$.B!three#".to_string(), 1234, vec![0], vec![10]),
            ("W.A$.B!two#".to_string(), 1234, vec![0], vec![6]),
            ("W.A$.C!..#".to_string(), 1234, vec![0], vec![0]),
            ("W.A$.C!..#".to_string(), 1234, vec![1], vec![0]),
            ("W.A$.C!c2#".to_string(), 1234, vec![0], vec![2]),
            ("W.A$.C!c2#".to_string(), 1234, vec![1], vec![2]),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_shred_empty_object() {
        let mut shredder = super::Shredder::new();
        let json = r#"{}"#;
        let docseq = 123;
        let dbname = "target/tests/test_shred_empty_object";
        let _ = Idx::drop(dbname);

        let index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = index.db.new_batch();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder.add_all_to_batch(docseq, &mut batch).unwrap();

        index.write_batch(batch).unwrap();
        let result = positions_from_db(&index.db);
        let expected = vec![("W._id!foo#".to_string(), 123, vec![], vec![0])];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_number() {
        let numbers = [
            "0",
            "1.5",
            "-2e3",
            "18446744073709551615",
            "-18446744073709551615.5",
            "1e-400",
        ];
        assert_eq!(
            numbers.map(super::parse_number),
            [
                Some(0.0),
                Some(1.5),
                Some(-2000.0),
                Some(18446744073709551615.0),
                Some(-18446744073709551615.5),
                Some(0.0)
            ]
        );
        for negative_zero in ["-0", "-0.0", "-0e5", "-1e-400"] {
            let number = super::parse_number(negative_zero).unwrap();
            assert_eq!(number.to_bits(), 0.0f64.to_bits());
        }
        let invalid = [
            "18446744073709551616",
            "-18446744073709551616",
            "18446744073709551616.5",
            "1e400",
            "-1e400",
        ];
        for literal in invalid {
            assert_eq!(
                super::parse_number(literal),
                None,
                "{:?} must not parse",
                literal
            );
        }
    }

    #[test]
    fn test_shred_invalid() {
        let invalid = [
            "",
            "   ",
            r#"{"a": 1,}"#,
            r#"{"a" 1}"#,
            r#"{"a": tru}"#,
            r#"{"a": "unterminated"#,
            r#"{"a": [01]}"#,
            r#"{"a": [1 2]}"#,
            r#"{"a": [}"#,
            r#"{"a": 1e400}"#,
            r#"{"a": 1} x"#,
            r#"{"a": 1}{"b": 2}"#,
            "{1: 2}",
        ];
        for json in invalid {
            let result = super::Shredder::new().shred(json);
            assert!(result.is_err(), "{:?} must not shred", json);
        }
    }

    #[test]
    fn test_shred_max_depth() {
        let allowed = format!(
            "{}{}",
            "[".repeat(super::MAX_DEPTH),
            "]".repeat(super::MAX_DEPTH)
        );
        super::Shredder::new().shred(&allowed).unwrap();

        let depth = super::MAX_DEPTH + 1;
        let too_deep = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        let error = super::Shredder::new().shred(&too_deep).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Shred error: maximal depth exceeded at byte {}",
                super::MAX_DEPTH
            )
        );
    }

    #[test]
    fn test_shred_error_position() {
        let cases = [
            // No value at all, or not a value
            ("{", "value or end of sequence expected at byte 1"),
            ("]", "value expected at byte 0"),
            (r#"{"a":}"#, "value expected at byte 5"),
            (r#"{"a": tru}"#, "value expected at byte 7"),
            // The first member of an object or array
            ("{1: 2}", "string expected at byte 1"),
            (r#"{"a": [}"#, "value expected at byte 7"),
            // A member after a comma
            (r#"{"a": 1,}"#, "string expected at byte 8"),
            (r#"{"a": [1,]}"#, "value expected at byte 9"),
            // The key of a member
            (r#"{"a" 1}"#, "colon expected at byte 5"),
            // After a value
            (
                r#"{"a": [1}"#,
                "comma or end of sequence expected at byte 8",
            ),
            (r#"{"a": 1"#, "comma or end of sequence expected at byte 7"),
            (
                r#"{"a": [01]}"#,
                "comma or end of sequence expected at byte 8",
            ),
            // After the document
            (r#"{"a": 1} x"#, "end of file expected at byte 9"),
            (r#"{"a": 1}{"b": 2}"#, "end of file expected at byte 8"),
            // Invalid values
            (r#"{"a": "x"#, "unterminated string at byte 8"),
            (r#"{"a": 1e400}"#, "invalid number 1e400 at byte 11"),
            (
                r#"{"_id": 1}"#,
                "Expected string for `_id` field, got another type at byte 9",
            ),
        ];
        for (json, expected) in cases {
            let error = super::Shredder::new().shred(json).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("Shred error: {}", expected),
                "{:?}",
                json
            );
        }
    }
}
