extern crate rocksdb;
extern crate rustc_serialize;
extern crate varint;

use std::collections::{HashMap, BTreeMap};
use std::mem::transmute;
use std::io::Write;
use std::str::Chars;
use std::io::Cursor;
use std::str;

use self::varint::VarintWrite;
use self::rustc_serialize::json::{JsonEvent, Parser, StackElement};

use error::Error;
use key_builder::KeyBuilder;
use stems::Stems;
use index::Index;

// Good example of using rustc_serialize: https://github.com/ajroetker/beautician/blob/master/src/lib.rs
// Callback based JSON streaming parser: https://github.com/gyscos/json-streamer.rs
// Another parser pased on rustc_serializ: https://github.com/isagalaev/ijson-rust/blob/master/src/test.rs#L11

enum ObjectKeyTypes {
    /// _id field
    Id,
    /// Normal key
    Key(String),
    /// No key found
    NoKey,
}

#[derive(Debug)]
pub struct Shredder {
    kb: KeyBuilder,
    doc_id: Option<String>,
    object_keys_indexed: Vec<bool>,
    shredded_key_values: BTreeMap<String, Vec<u8>>,
    existing_key_value_to_delete: BTreeMap<String, Vec<u8>>,
}

impl Shredder {
    pub fn new() -> Shredder {
        Shredder {
            kb: KeyBuilder::new(),
            doc_id: None,
            object_keys_indexed: Vec::new(),
            shredded_key_values: BTreeMap::new(),
            existing_key_value_to_delete: BTreeMap::new(),
        }
    }

    fn add_number_entries(kb: &mut KeyBuilder, number: &[u8], docseq: u64,
                          batch: &mut rocksdb::WriteBatch, delete: bool) -> Result<(), Error> {
        // Add/delete the key that is used for range lookups
        let number_key = kb.number_key(docseq);
        if delete {
            try!(batch.delete(&number_key.as_bytes()));
        } else {
            // The number contains the `f` prefix
            try!(batch.put(&number_key.as_bytes(), &number[1..]));
        }

        Ok(())
    }

    fn add_stemmed_entries(kb: &mut KeyBuilder, text: &str, docseq: u64,
            batch: &mut rocksdb::WriteBatch, delete: bool) -> Result<(), Error> {
        let stems = Stems::new(text);
        let mut word_to_word_positions = HashMap::new();
        let mut total_words: i32 = 0;

        let mut one_enc_bytes = Cursor::new(Vec::new());
        let num = if delete {-1} else {1};
        assert!(one_enc_bytes.write_signed_varint_32(num).is_ok());

        for stem in stems {
            total_words += 1;
            let &mut (ref mut word_positions, ref mut count) =
                    word_to_word_positions.entry(stem.stemmed)
                                            .or_insert((Cursor::new(Vec::new()), 0));
            if !delete {
                assert!(word_positions.write_unsigned_varint_32(stem.word_pos).is_ok());
            }
            *count += 1;
        }

        for (stemmed, (word_positions, count)) in word_to_word_positions {
            let key = kb.stemmed_word_key(&stemmed, docseq);
            if delete {
                try!(batch.delete(&key.into_bytes()));
            } else {
                try!(batch.put(&key.into_bytes(), &word_positions.into_inner()));
            } 

            let key = kb.field_length_key(docseq);
            if delete {
                try!(batch.delete(&key.into_bytes()));
            } else {
                try!(batch.put(&key.into_bytes(), &Index::convert_i32_to_bytes(total_words)));
            }
            
            let key = kb.keypathword_count_key(&stemmed);
            if delete {
                try!(batch.merge(&key.into_bytes(), &Index::convert_i32_to_bytes(-count)));
            } else {
                try!(batch.merge(&key.into_bytes(), &Index::convert_i32_to_bytes(count)));
            }

            let key = kb.keypath_count_key();
            try!(batch.merge(&key.into_bytes(), one_enc_bytes.get_ref()));
        }

        Ok(())
    }
    
    fn add_value(&mut self, code: char, value: &[u8]) -> Result<(), Error> {
        let key = self.kb.value_key_path_only();
        let mut buffer = Vec::with_capacity(value.len() + 1);
        buffer.push(code as u8);
        try!((&mut buffer as &mut Write).write_all(value));
        self.shredded_key_values.insert(key, buffer);
        Ok(())
    }

    fn maybe_add_value(&mut self, parser: &Parser<Chars>, code: char, value: &[u8]) -> Result<(), Error> {
        match self.extract_key(parser.stack().top()) {
            ObjectKeyTypes::Id => {
                if code != 's' && self.kb.keypath_segments_len() == 1 {
                    //nested fields can be _id, not root fields
                    return Err(Error::Shred(
                            "Expected string for `_id` field, got another type".to_string()));
                }
                self.doc_id = Some(unsafe{ str::from_utf8_unchecked(value) }.to_string());
                self.kb.pop_object_key();
                self.kb.push_object_key("_id");
                *self.object_keys_indexed.last_mut().unwrap() = true;
                try!(self.add_value(code, &value));
            },
            ObjectKeyTypes::Key(key) => {
                // Pop the dummy object that makes ObjectEnd happy
                // or the previous object key
                self.kb.pop_object_key();
                self.kb.push_object_key(&key);
                *self.object_keys_indexed.last_mut().unwrap() = true;
                try!(self.add_value(code, &value));
            },
            ObjectKeyTypes::NoKey => {
                try!(self.add_value(code, &value));
                self.kb.inc_top_array_offset();
            },
        }
        Ok(())
    }

    // Extract key if it exists and indicates if it's a special type of key
    fn extract_key(&mut self, stack_element: Option<StackElement>) -> ObjectKeyTypes {
        match stack_element {
            Some(StackElement::Key(key)) => {
                if self.kb.keypath_segments_len() == 1 && key == "_id" {
                    ObjectKeyTypes::Id
                } else {
                    ObjectKeyTypes::Key(key.to_string())
                }
            },
            _ => ObjectKeyTypes::NoKey,
        }
    }

    // If we are inside an object we need to push the key to the key builder
    // Don't push them if they are reserved fields (starting with underscore)
    fn maybe_push_key(&mut self, stack_element: Option<StackElement>) -> Result<(), Error> {
        if let Some(StackElement::Key(key)) = stack_element {
            if self.kb.keypath_segments_len() == 1 && key == "_id" {
                return Err(Error::Shred(
                        "Expected string for `_id` field, got another type".to_string()));
            } else {
                // Pop the dummy object that makes ObjectEnd happy
                // or the previous object key
                self.kb.pop_object_key();
                self.kb.push_object_key(key);
                *self.object_keys_indexed.last_mut().unwrap() = true;
            }
        }
        Ok(())
    }

    pub fn add_all_to_batch(&mut self, seq: u64,
                            batch: &mut rocksdb::WriteBatch) -> Result<(), Error> {
        for (key, value) in &self.existing_key_value_to_delete {
            self.kb.clear();
            self.kb.parse_value_key_path_only(KeyBuilder::value_key_path_only_from_str(&key));
            match value[0] as char {
                's' => {
                    let text = unsafe{ str::from_utf8_unchecked(&value[1..]) };
                    try!(Shredder::add_stemmed_entries(&mut self.kb, text, seq, batch, true));
                },
                'f' => {
                    try!(Shredder::add_number_entries(&mut self.kb, &value, seq, batch, true));
                },
                _ => {},
            }
            try!(batch.delete(&key.as_bytes()));
        }
        self.existing_key_value_to_delete = BTreeMap::new();

        for (key, value) in &self.shredded_key_values {
            self.kb.clear();
            self.kb.parse_value_key_path_only(&key);
            match value[0] as char {
                's' => {
                    let text = unsafe{ str::from_utf8_unchecked(&value[1..]) };
                    try!(Shredder::add_stemmed_entries(&mut self.kb, text, seq, batch, false));
                },
                'f' => {
                    try!(Shredder::add_number_entries(&mut self.kb, &value, seq, batch, false));
                },
                _ => {},
            }
            let key = self.kb.value_key(seq);
            try!(batch.put(&key.as_bytes(), &value.as_ref()));
        }
        self.shredded_key_values = BTreeMap::new();

        let key = self.kb.id_to_seq_key(self.doc_id.as_ref().unwrap());
        try!(batch.put(&key.into_bytes(), &seq.to_string().as_bytes()));

        Ok(())
    }

    pub fn delete_existing_doc(&mut self,
                               docid: &str,
                               seq: u64, 
                               existing: BTreeMap<String, Vec<u8>>,
                               batch: &mut rocksdb::WriteBatch) -> Result<(), Error> {
        self.doc_id = Some(docid.to_string());
        for (key, value) in existing.into_iter() {
            self.kb.clear();
            self.kb.parse_value_key_path_only(KeyBuilder::value_key_path_only_from_str(&key));
            match value[0] as char {
                's' => {
                    let text = unsafe{ str::from_utf8_unchecked(&value[1..]) };
                    try!(Shredder::add_stemmed_entries(&mut self.kb, text, seq, batch, true));
                },
                'f' => {
                    try!(Shredder::add_number_entries(&mut self.kb, &value, seq, batch, true));
                },
                _ => {},
            }
            try!(batch.delete(&key.as_bytes()));
        }
        let key = self.kb.id_to_seq_key(self.doc_id.as_ref().unwrap());
        try!(batch.delete(&key.into_bytes()));
        Ok(())
    }

    pub fn merge_existing_doc(&mut self, existing: BTreeMap<String, Vec<u8>>) {
        // we found doc with the same id already stored on disk. We need to delete
        // the doc. But any fields that are the same we can just keep around
        // and don't even need to reindex.
        for (existing_key, existing_value) in existing {
            let matches = {
                let key = KeyBuilder::value_key_path_only_from_str(&existing_key);
                if let Some(new_value) = self.shredded_key_values.get(key) {
                    *new_value == existing_value
                } else {
                    false
                }
            };
            if matches {
                // we don't need to write or index these values, they already exist!
                let key = KeyBuilder::value_key_path_only_from_str(&existing_key);
                self.shredded_key_values.remove(key).unwrap();
            } else {
                // we need to delete these keys and the index keys assocaited with the valuess
                self.existing_key_value_to_delete.insert(existing_key, existing_value);
            }
        }
    }

    pub fn add_id(&mut self, id: &str) -> Result<(), Error> {
        self.doc_id = Some(id.to_string());
        self.kb.clear();
        self.kb.push_object_key("_id");
        try!(self.add_value('s', &id.as_bytes()));
        Ok(())
    }

    pub fn shred(&mut self, json: &str) -> Result<Option<String>, Error> {
        let mut parser = Parser::new(json.chars());
        loop {
            // Get the next token, so that in case of an `ObjectStart` the key is already
            // on the stack.
            match parser.next().take() {
                Some(JsonEvent::ObjectStart) => {
                    try!(self.maybe_push_key(parser.stack().top()));
                    // Just push something to make `ObjectEnd` happy
                    self.kb.push_object_key("");
                    self.object_keys_indexed.push(false);
                },
                Some(JsonEvent::ObjectEnd) => {
                    self.kb.pop_object_key();
                    if !self.object_keys_indexed.pop().unwrap() {
                        // this means we never wrote a key because the object was empty.
                        // So preserve the empty object by writing a special value.
                        try!(self.maybe_add_value(&parser, 'o', &[]));
                    }
                    self.kb.inc_top_array_offset();
                },
                Some(JsonEvent::ArrayStart) => {
                    try!(self.maybe_push_key(parser.stack().top()));
                    self.kb.push_array();
                },
                Some(JsonEvent::ArrayEnd) => {
                    if self.kb.peek_array_offset() == 0 {
                        // this means we never wrote a value because the object was empty.
                        // So preserve the empty array by writing a special value.
                        self.kb.pop_array();
                        try!(self.maybe_add_value(&parser, 'a', &[]));
                    } else {
                        self.kb.pop_array();
                    }
                    self.kb.inc_top_array_offset();
                },
                Some(JsonEvent::StringValue(value)) => {
                    try!(self.maybe_add_value(&parser, 's', &value.as_bytes()));
                },
                Some(JsonEvent::BooleanValue(tf)) => {
                    let code = if tf {'T'} else {'F'};
                    try!(self.maybe_add_value(&parser, code, &[]));
                },
                Some(JsonEvent::I64Value(i)) => {
                    let f = i as f64;
                    let bytes = unsafe{ transmute::<f64, [u8; 8]>(f) };
                    try!(self.maybe_add_value(&parser, 'f', &bytes[..]));
                },
                Some(JsonEvent::U64Value(u)) => {
                    let f = u as f64;
                    let bytes = unsafe{ transmute::<f64, [u8; 8]>(f) };
                    try!(self.maybe_add_value(&parser, 'f', &bytes[..]));
                },
                Some(JsonEvent::F64Value(f)) => {
                    let bytes = unsafe{ transmute::<f64, [u8; 8]>(f) };
                    try!(self.maybe_add_value(&parser, 'f', &bytes[..]));
                },
                Some(JsonEvent::NullValue) => {
                    try!(self.maybe_add_value(&parser, 'N', &[]));
                },
                Some(JsonEvent::Error(error)) => {
                    return Err(Error::Shred(error.to_string()));
                },
                None => {
                    break;
                }
            };
        }
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

    use index::{Index, OpenOptions};
    use json_value::JsonValue;
    use snapshot::JsonFetcher;

    fn positions_from_rocks(rocks: &rocksdb::DB) -> Vec<(String, Vec<u32>)> {
        let mut result = Vec::new();
        for (key, value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            if key[0] as char == 'W' {
                let mut vec = Vec::with_capacity(value.len());
                vec.extend(value.into_iter());
                let mut bytes = Cursor::new(vec);
                let mut positions = Vec::new();
                while let Ok(pos) = bytes.read_unsigned_varint_32() {
                    positions.push(pos);
                }
                let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
                result.push((key_string, positions));
            }
        }
        result
    }

    fn values_from_rocks(rocks: &rocksdb::DB) -> Vec<(String, JsonValue)> {
        let mut result = Vec::new();
        for (key, value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            if key[0] as char == 'V' {
                let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
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
        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder.add_all_to_batch(docseq, &mut batch).unwrap();

        let dbname = "target/tests/test_shred_nested";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();

        rocks.write(batch).unwrap();
        let result = positions_from_rocks(&rocks);

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
        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder.add_all_to_batch(docseq, &mut batch).unwrap();

        let dbname = "target/tests/test_shred_double_nested";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();

        rocks.write(batch).unwrap();
        let result = values_from_rocks(&rocks);

        let expected = vec![
            ("V123#._id".to_string(), JsonValue::String("foo".to_string())),
            ("V123#.a.a".to_string(), JsonValue::String("b".to_string()))
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
        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json).unwrap();
        shredder.add_all_to_batch(docseq, &mut batch).unwrap();

        let dbname = "target/tests/test_shred_objects";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();

        rocks.write(batch).unwrap();
        let result = positions_from_rocks(&rocks);
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
        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json).unwrap();
        shredder.add_id("foo").unwrap();
        shredder.add_all_to_batch(docseq, &mut batch).unwrap();

        let dbname = "target/tests/test_shred_empty_object";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        
        let rocks = &index.rocks.unwrap();

        rocks.write(batch).unwrap();
        let result = positions_from_rocks(&rocks);
        let expected = vec![("W._id!foo#123,".to_string(), vec![0])];
        assert_eq!(result, expected);
    }
}
