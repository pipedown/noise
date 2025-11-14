extern crate rocksdb;
extern crate varint;

use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::io::Write;
use std::{self, f64, str};

use self::varint::VarintWrite;
use serde_json::{self, Value as SerdeJsonValue};

use crate::error::Error;
use crate::index::Index;
use crate::key_builder::KeyBuilder;
use crate::stems::Stems;

/// Key-value pairs, where the key is the path to the value, the value is the actual value.
pub(crate) type KeyValues = BTreeMap<String, Vec<u8>>;

// `GeometryCollection` is left out as we will process the individual geometries of it
const GEOJSON_TYPES: &[&str] = &[
    "Point",
    "MultiPoint",
    "LineString",
    "MultiLineString",
    "Polygon",
    "MultiPolygon",
];

enum ObjectKeyTypes {
    /// _id field
    Id,
    /// Normal key
    Key(String),
    /// No key found
    NoKey,
}

#[derive(Debug)]
struct GeometryState {
    seen_type: bool,
    seen_coordinates: bool,
    coordinates_depth: u32,
    is_first: bool,
    bounding_box: [f64; 4],
}

impl GeometryState {
    fn new() -> Self {
        GeometryState {
            seen_type: false,
            seen_coordinates: false,
            coordinates_depth: 0,
            is_first: true,
            bounding_box: [f64::MAX, f64::MAX, f64::MIN, f64::MIN],
        }
    }
}

#[derive(Debug)]
pub struct Shredder {
    kb: KeyBuilder,
    doc_id: Option<String>,
    object_keys_indexed: Vec<bool>,
    shredded_key_values: KeyValues,
    existing_key_value_to_delete: KeyValues,
    geometry_stack: Vec<GeometryState>,
}

impl Shredder {
    pub fn new() -> Shredder {
        Shredder {
            kb: KeyBuilder::new(),
            doc_id: None,
            object_keys_indexed: Vec::new(),
            shredded_key_values: BTreeMap::new(),
            existing_key_value_to_delete: BTreeMap::new(),
            geometry_stack: Vec::new(),
        }
    }

    // Based on https://stackoverflow.com/questions/29037033/how-to-slice-a-large-veci32-as-u8
    // (2017-07-26)
    fn as_u8_slice(v: &[u64]) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                v.as_ptr() as *const u8,
                v.len() * std::mem::size_of::<u64>(),
            )
        }
    }

    fn add_rtree_entries(
        kb: &mut KeyBuilder,
        bbox: &[u8],
        docseq: u64,
        batch: &mut rocksdb::WriteBatch,
        column_family: rocksdb::ColumnFamily,
        delete: bool,
    ) -> Result<(), Error> {
        // Add/delete the key that is used for R-tree lookups
        let rtree_key = kb.rtree_key(docseq, bbox);
        if delete {
            batch.delete_cf(column_family, rtree_key.as_slice())?;
        } else {
            batch.put_cf(
                column_family,
                rtree_key.as_slice(),
                Shredder::as_u8_slice(kb.arraypath.as_slice()),
            )?;
        }

        Ok(())
    }

    fn add_number_entries(
        kb: &mut KeyBuilder,
        number: &[u8],
        docseq: u64,
        batch: &mut rocksdb::WriteBatch,
        delete: bool,
    ) -> Result<(), Error> {
        // Add/delete the key that is used for range lookups
        let number_key = kb.number_key(docseq);
        if delete {
            batch.delete(number_key.as_bytes())?;
        } else {
            // The number contains the `f` prefix
            batch.put(number_key.as_bytes(), &number[1..])?;
        }

        Ok(())
    }

    fn add_bool_null_entries(
        kb: &mut KeyBuilder,
        prefix: char,
        docseq: u64,
        batch: &mut rocksdb::WriteBatch,
        delete: bool,
    ) -> Result<(), Error> {
        let key = kb.bool_null_key(prefix, docseq);
        if delete {
            batch.delete(key.as_bytes())?;
        } else {
            // No need to store any value as the key already contains it
            batch.put(key.as_bytes(), &[])?;
        }

        Ok(())
    }

    fn add_stemmed_entries(
        kb: &mut KeyBuilder,
        text: &str,
        docseq: u64,
        batch: &mut rocksdb::WriteBatch,
        delete: bool,
    ) -> Result<(), Error> {
        let stems = Stems::new(text);
        let mut word_to_word_positions = HashMap::new();
        let mut total_words: i32 = 0;

        let mut one_enc_bytes = Cursor::new(Vec::new());
        let num = if delete { -1 } else { 1 };
        assert!(one_enc_bytes.write_signed_varint_32(num).is_ok());

        for stem in stems {
            total_words += 1;
            let &mut (ref mut word_positions, ref mut count) = word_to_word_positions
                .entry(stem.stemmed)
                .or_insert((Cursor::new(Vec::new()), 0));
            if !delete {
                assert!(word_positions
                    .write_unsigned_varint_32(stem.word_pos)
                    .is_ok());
            }
            *count += 1;
        }

        for (stemmed, (word_positions, count)) in word_to_word_positions {
            let key = kb.kp_word_key(&stemmed, docseq);
            if delete {
                batch.delete(&key.into_bytes())?;
            } else {
                batch.put(&key.into_bytes(), &word_positions.into_inner())?;
            }

            let key = kb.kp_field_length_key(docseq);
            if delete {
                batch.delete(&key.into_bytes())?;
            } else {
                batch.put(&key.into_bytes(), &Index::convert_i32_to_bytes(total_words))?;
            }

            let key = kb.kp_word_count_key(&stemmed);
            if delete {
                batch.merge(&key.into_bytes(), &Index::convert_i32_to_bytes(-count))?;
            } else {
                batch.merge(&key.into_bytes(), &Index::convert_i32_to_bytes(count))?;
            }

            let key = kb.kp_field_count_key();
            batch.merge(&key.into_bytes(), one_enc_bytes.get_ref())?;
        }

        Ok(())
    }

    fn calc_mbb(&mut self, value: f64) {
        if let Some(state) = self.geometry_stack.last_mut() {
            if state.coordinates_depth > 0 {
                if state.is_first {
                    state.bounding_box[0] = state.bounding_box[0].min(value);
                    state.bounding_box[2] = state.bounding_box[0].max(value);
                } else {
                    state.bounding_box[1] = state.bounding_box[1].min(value);
                    state.bounding_box[3] = state.bounding_box[3].max(value);
                }
                state.is_first = !state.is_first;
            }
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

    fn maybe_add_value(&mut self, key: Option<&str>, code: char, value: &[u8]) -> Result<(), Error> {
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
            ObjectKeyTypes::Key(key) => {
                // Pop the dummy object that makes ObjectEnd happy
                // or the previous object key
                self.kb.pop_object_key();
                self.kb.push_object_key(&key);
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
    fn extract_key(&mut self, key: Option<&str>) -> ObjectKeyTypes {
        match key {
            Some(key) => {
                if self.kb.kp_segments_len() == 1 && key == "_id" {
                    ObjectKeyTypes::Id
                } else {
                    ObjectKeyTypes::Key(key.to_string())
                }
            }
            _ => ObjectKeyTypes::NoKey,
        }
    }

    // If we are inside an object we need to push the key to the key builder
    // Don't push them if they are reserved fields (starting with underscore)
    // It is called when the value is an array or object. If it is a primitive type, then
    // `extract_key` is called.
    fn maybe_push_key(&mut self, key: Option<&str>) -> Result<Option<String>, Error> {
        match key {
            Some(key) => {
                if self.kb.kp_segments_len() == 1 && key == "_id" {
                    return Err(Error::Shred(
                        "Expected string for `_id` field, got another type".to_string(),
                    ));
                } else {
                    // Pop the dummy object that makes ObjectEnd happy
                    // or the previous object key
                    self.kb.pop_object_key();
                    self.kb.push_object_key(key);
                    *self.object_keys_indexed.last_mut().unwrap() = true;
                }
                Ok(Some(key.to_string()))
            }
            _ => Ok(None),
        }
    }

    fn walk_value(&mut self, key: Option<&str>, value: &SerdeJsonValue) -> Result<(), Error> {
        match value {
            SerdeJsonValue::Object(map) => self.walk_object(key, map),
            SerdeJsonValue::Array(values) => self.walk_array(key, values),
            SerdeJsonValue::String(text) => {
                if let Some("type") = key {
                    if GEOJSON_TYPES
                        .iter()
                        .any(|&tt| tt == text.as_str())
                    {
                        if let Some(state) = self.geometry_stack.last_mut() {
                            state.seen_type = true;
                        }
                    }
                }
                self.maybe_add_value(key, 's', text.as_bytes())
            }
            SerdeJsonValue::Bool(tf) => {
                let code = if *tf { 'T' } else { 'F' };
                self.maybe_add_value(key, code, &[])
            }
            SerdeJsonValue::Number(num) => {
                if let Some(i) = num.as_i64() {
                    let f = i as f64;
                    self.calc_mbb(f);
                    self.maybe_add_value(key, 'f', &f.to_le_bytes())
                } else if let Some(u) = num.as_u64() {
                    let f = u as f64;
                    self.calc_mbb(f);
                    self.maybe_add_value(key, 'f', &f.to_le_bytes())
                } else if let Some(f) = num.as_f64() {
                    self.calc_mbb(f);
                    self.maybe_add_value(key, 'f', &f.to_le_bytes())
                } else {
                    Err(Error::Shred("Unable to parse number value".to_string()))
                }
            }
            SerdeJsonValue::Null => self.maybe_add_value(key, 'N', &[]),
        }
    }

    fn walk_object(
        &mut self,
        key: Option<&str>,
        map: &serde_json::Map<String, SerdeJsonValue>,
    ) -> Result<(), Error> {
        self.maybe_push_key(key)?;
        self.kb.push_object_key("");
        self.object_keys_indexed.push(false);
        self.geometry_stack.push(GeometryState::new());

        for (child_key, child_value) in map.iter() {
            self.walk_value(Some(child_key), child_value)?;
        }

        self.kb.pop_object_key();
        let indexed = self.object_keys_indexed.pop().unwrap_or(false);
        if self.kb.kp_segments_len() > 0 && !indexed {
            self.maybe_add_value(key, 'o', &[])?;
        }

        if let Some(state) = self.geometry_stack.pop() {
            if state.seen_type && state.seen_coordinates {
                let mut encoded_bbox = Vec::new();
                encoded_bbox.extend_from_slice(&state.bounding_box[0].to_le_bytes());
                encoded_bbox.extend_from_slice(&state.bounding_box[2].to_le_bytes());
                encoded_bbox.extend_from_slice(&state.bounding_box[1].to_le_bytes());
                encoded_bbox.extend_from_slice(&state.bounding_box[3].to_le_bytes());
                let _ = self.add_value('r', encoded_bbox.as_slice());
            }
        }

        self.kb.inc_top_array_index();
        Ok(())
    }

    fn walk_array(&mut self, key: Option<&str>, values: &[SerdeJsonValue]) -> Result<(), Error> {
        self.maybe_push_key(key)?;
        let mut increased_depth = false;
        if let Some(state) = self.geometry_stack.last_mut() {
            if key == Some("coordinates") {
                state.seen_coordinates = true;
                state.coordinates_depth += 1;
                increased_depth = true;
            } else if state.coordinates_depth > 0 {
                state.coordinates_depth += 1;
                increased_depth = true;
            }
        }
        self.kb.push_array();

        if values.is_empty() {
            self.kb.pop_array();
            self.maybe_add_value(key, 'a', &[])?;
            self.kb.inc_top_array_index();
            return Ok(());
        }

        for value in values {
            self.walk_value(None, value)?;
        }

        self.kb.pop_array();
        if increased_depth {
            if let Some(state) = self.geometry_stack.last_mut() {
                if state.coordinates_depth > 0 {
                    state.coordinates_depth -= 1;
                }
            }
        }
        self.kb.inc_top_array_index();
        Ok(())
    }

    pub fn add_all_to_batch(
        &mut self,
        seq: u64,
        batch: &mut rocksdb::WriteBatch,
        rtree: rocksdb::ColumnFamily,
    ) -> Result<(), Error> {
        for (key, value) in &self.existing_key_value_to_delete {
            self.kb.clear();
            self.kb
                .parse_kp_value_no_seq(KeyBuilder::kp_value_no_seq_from_str(key));
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
                    Shredder::add_rtree_entries(
                        &mut self.kb,
                        &value[1..],
                        seq,
                        batch,
                        rtree,
                        true,
                    )?;
                }
                _ => {}
            }
            // If it was a bounding box calculated for the R-tree, it's not part of the
            // shredded original JSON document
            if value[0] as char != 'r' {
                batch.delete(key.as_bytes())?;
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
                    Shredder::add_rtree_entries(
                        &mut self.kb,
                        &value[1..],
                        seq,
                        batch,
                        rtree,
                        false,
                    )?;
                }
                _ => {}
            }
            // If it was a bounding box calculated for the R-tree, it's not a value meant to
            // be part of the shredded original JSON document
            if value[0] as char != 'r' {
                let key = self.kb.kp_value_key(seq);
                batch.put(key.as_bytes(), value.as_ref())?;
            }
        }
        self.shredded_key_values = BTreeMap::new();

        let key = KeyBuilder::id_to_seq_key(self.doc_id.as_ref().unwrap());
        batch.put(&key.into_bytes(), seq.to_string().as_bytes())?;

        let key = KeyBuilder::seq_key(seq);
        batch.put(&key.into_bytes(), b"")?;

        Ok(())
    }

    pub fn delete_existing_doc(
        &mut self,
        docid: &str,
        seq: u64,
        existing: KeyValues,
        batch: &mut rocksdb::WriteBatch,
    ) -> Result<(), Error> {
        self.doc_id = Some(docid.to_string());
        for (key, value) in existing.into_iter() {
            self.kb.clear();
            self.kb
                .parse_kp_value_no_seq(KeyBuilder::kp_value_no_seq_from_str(&key));
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
            batch.delete(key.as_bytes())?;
        }
        let key = KeyBuilder::id_to_seq_key(self.doc_id.as_ref().unwrap());
        batch.delete(&key.into_bytes())?;

        let key = KeyBuilder::seq_key(seq);
        batch.delete(&key.into_bytes())?;
        Ok(())
    }

    pub fn merge_existing_doc(&mut self, existing: KeyValues) {
        // we found doc with the same id already stored on disk. We need to delete
        // the doc. But any fields that are the same we can just keep around
        // and don't even need to reindex.
        for (existing_key, existing_value) in existing {
            let matches = {
                let key = KeyBuilder::kp_value_no_seq_from_str(&existing_key);
                if let Some(new_value) = self.shredded_key_values.get(key) {
                    *new_value == existing_value
                } else {
                    false
                }
            };
            if matches {
                // we don't need to write or index these values, they already exist!
                let key = KeyBuilder::kp_value_no_seq_from_str(&existing_key);
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
        self.doc_id = None;
        self.kb.clear();
        self.object_keys_indexed.clear();
        self.shredded_key_values.clear();
        self.existing_key_value_to_delete.clear();
        self.geometry_stack.clear();

        let value: SerdeJsonValue =
            serde_json::from_str(json).map_err(|err| Error::Shred(err.to_string()))?;
        self.walk_value(None, &value)?;

        Ok(self.doc_id.clone())
    }
}

#[cfg(test)]
mod tests {
    extern crate rocksdb;
    extern crate varint;

    use self::varint::VarintRead;

    use std::io::Cursor;
    use std::str;

    use crate::index::{Index, OpenOptions};
    use crate::json_value::JsonValue;
    use crate::snapshot::JsonFetcher;

    fn positions_from_rocks(rocks: &rocksdb::DB) -> Vec<(String, Vec<u32>)> {
        let mut result = Vec::new();
        for (key, value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            if key[0] as char == 'W' {
                let mut vec = Vec::with_capacity(value.len());
                vec.extend(value.iter());
                let mut bytes = Cursor::new(vec);
                let mut positions = Vec::new();
                while let Ok(pos) = bytes.read_unsigned_varint_32() {
                    positions.push(pos);
                }
                let key_string = unsafe { str::from_utf8_unchecked(&key) }.to_string();
                result.push((key_string, positions));
            }
        }
        result
    }

    fn values_from_rocks(rocks: &rocksdb::DB) -> Vec<(String, JsonValue)> {
        let mut result = Vec::new();
        for (key, value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            if key[0] as char == 'V' {
                let key_string = unsafe { str::from_utf8_unchecked(&key) }.to_string();
                result.push((key_string, JsonFetcher::bytes_to_json_value(&*value)));
            }
        }
        result
    }

    #[test]
    fn test_shred_nested() {
        let mut shredder = super::Shredder::new();
        let json = r#"{"some": ["array", "data", ["also", "nested"]]}"#;
        let docseq = 123;
        let dbname = "target/tests/test_shred_nested";
        let _ = Index::drop(dbname);
        let index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
        let rtree = index.rocks.cf_handle("rtree").unwrap();

        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder
            .add_all_to_batch(docseq, &mut batch, rtree)
            .unwrap();

        index.rocks.write(batch).unwrap();
        let result = positions_from_rocks(&index.rocks);

        let expected = vec![
            ("W._id!foo#123,".to_string(), vec![0]),
            ("W.some$!array#123,0".to_string(), vec![0]),
            ("W.some$!data#123,1".to_string(), vec![0]),
            ("W.some$$!also#123,2,0".to_string(), vec![0]),
            ("W.some$$!nest#123,2,1".to_string(), vec![0]),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_shred_double_nested() {
        let mut shredder = super::Shredder::new();
        let json = r#"{"a":{"a":"b"}}"#;
        let docseq = 123;
        let dbname = "target/tests/test_shred_double_nested";
        let _ = Index::drop(dbname);
        let index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
        let rtree = index.rocks.cf_handle("rtree").unwrap();

        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder
            .add_all_to_batch(docseq, &mut batch, rtree)
            .unwrap();

        index.rocks.write(batch).unwrap();
        let result = values_from_rocks(&index.rocks);

        let expected = vec![
            (
                "V123#._id".to_string(),
                JsonValue::String("foo".to_string()),
            ),
            ("V123#.a.a".to_string(), JsonValue::String("b".to_string())),
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
        let _ = Index::drop(dbname);

        let index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
        let rtree = index.rocks.cf_handle("rtree").unwrap();

        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json).unwrap();
        shredder
            .add_all_to_batch(docseq, &mut batch, rtree)
            .unwrap();

        index.rocks.write(batch).unwrap();
        let result = positions_from_rocks(&index.rocks);
        let expected = vec![
            ("W.A$.B!b1#1234,1".to_string(), vec![0]),
            ("W.A$.B!b2vmx#1234,0".to_string(), vec![0]),
            ("W.A$.B!three#1234,0".to_string(), vec![10]),
            ("W.A$.B!two#1234,0".to_string(), vec![6]),
            ("W.A$.C!..#1234,0".to_string(), vec![0]),
            ("W.A$.C!..#1234,1".to_string(), vec![0]),
            ("W.A$.C!c2#1234,0".to_string(), vec![2]),
            ("W.A$.C!c2#1234,1".to_string(), vec![2]),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_shred_empty_object() {
        let mut shredder = super::Shredder::new();
        let json = r#"{}"#;
        let docseq = 123;
        let dbname = "target/tests/test_shred_empty_object";
        let _ = Index::drop(dbname);

        let index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
        let rtree = index.rocks.cf_handle("rtree").unwrap();

        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder
            .add_all_to_batch(docseq, &mut batch, rtree)
            .unwrap();

        index.rocks.write(batch).unwrap();
        let result = positions_from_rocks(&index.rocks);
        let expected = vec![("W._id!foo#123,".to_string(), vec![0])];
        assert_eq!(result, expected);
    }
}
