extern crate rocksdb;
extern crate rustc_serialize;
extern crate varint;

use std::collections::HashMap;
use std::mem::transmute;
use std::io::Write;
use std::str::Chars;
use std::io::Cursor;

use self::varint::VarintWrite;
use self::rustc_serialize::json::{JsonEvent, Parser, StackElement};

use error::Error;
use key_builder::KeyBuilder;
use stems::Stems;
use index::Index;

// Good example of using rustc_serialize: https://github.com/ajroetker/beautician/blob/master/src/lib.rs
// Callback based JSON streaming parser: https://github.com/gyscos/json-streamer.rs
// Another parser pased on rustc_serializ: https://github.com/isagalaev/ijson-rust/blob/master/src/test.rs#L11

type ArrayOffsets = Vec<u64>;

enum ObjectKeyTypes {
    /// _id field
    Id,
    /// Normal key
    Key(String),
    /// Reserved key starting with underscore
    Ignore,
    /// No key found
    NoKey,
}

pub trait Indexable {

}


#[derive(Debug)]
pub struct Shredder {
    kb: KeyBuilder,
    // Top-level fields prefixed with an underscore are ignored
    ignore_children: usize,
    doc_id: String,
}


impl Shredder {
    pub fn new() -> Shredder {
        Shredder{
            kb: KeyBuilder::new(),
            ignore_children: 0,
            doc_id: String::new(),
        }
    }

    fn add_entries(&mut self, text: &String, docseq: u64, batch: &mut rocksdb::WriteBatch) ->
            Result<(), Error> {
        let stems = Stems::new(text.as_str());
        let mut word_to_word_positions = HashMap::new();
        let mut total_words: u32 = 0;

        let mut one_enc_bytes = Cursor::new(Vec::new());
        assert!(one_enc_bytes.write_unsigned_varint_32(1).is_ok());
        for stem in stems {
            total_words += 1;
            let &mut (ref mut word_positions, ref mut count) = word_to_word_positions.entry(stem.stemmed)
                                                           .or_insert((Cursor::new(Vec::new()), 0));
            assert!(word_positions.write_unsigned_varint_32(stem.word_pos).is_ok());
            *count += 1;
        }

        for (stemmed, (word_positions, count)) in word_to_word_positions {
            let key = self.kb.stemmed_word_key(&stemmed, docseq);
            try!(batch.put(&key.into_bytes(), &word_positions.into_inner()));

            let key = self.kb.field_length_key(docseq);
            try!(batch.put(&key.into_bytes(), &Index::convert_u32_to_bytes(total_words)));
            
            let key = self.kb.keypathword_count_key(&stemmed);
            try!(batch.merge(&key.into_bytes(), &Index::convert_u32_to_bytes(count)));

            let key = self.kb.keypath_count_key();
            try!(batch.merge(&key.into_bytes(), one_enc_bytes.get_ref()));
        }

        let key = self.kb.value_key(docseq);
        let mut buffer = String::with_capacity(text.len() + 1);
        buffer.push('s');
        buffer.push_str(&text);

        try!(batch.put(&key.into_bytes(), &buffer.as_bytes()));

        Ok(())
    }
    
    fn add_value(&mut self, code: char, value: &[u8],
                 docseq: u64, batch: &mut rocksdb::WriteBatch) -> Result<(), Error> {
        let key = self.kb.value_key(docseq);
        let mut buffer = Vec::with_capacity(value.len() + 1);
        buffer.push(code as u8);
        try!((&mut buffer as &mut Write).write_all(&value));

        try!(batch.put(&key.into_bytes(), &buffer.as_ref()));

        Ok(())
    }

    fn maybe_add_value(&mut self, parser: &Parser<Chars>, code: char, value: &[u8],
                       docseq: u64, batch: &mut rocksdb::WriteBatch) -> Result<(), Error> {
        if self.ignore_children == 0 {
            match self.extract_key(parser.stack().top()) {
                ObjectKeyTypes::Id => {
                    return Err(Error::Shred(
                            "Expected string for `_id` field, got another type".to_string()));
                },
                ObjectKeyTypes::Key(key) => {
                    // Pop the dummy object that makes ObjectEnd happy
                    // or the previous object key
                    self.kb.pop_object_key();
                    self.kb.push_object_key(&key);

                    try!(self.add_value(code, &value, docseq, batch));
                },
                ObjectKeyTypes::NoKey => {
                    try!(self.add_value(code, &value, docseq, batch));
                    self.kb.inc_top_array_offset();
                },
                ObjectKeyTypes::Ignore => {
                    self.ignore_children = 1;
                },
            }
        }
        Ok(())
    }
    // Extract key if it exists and indicates if it's a special type of key
    fn extract_key(&mut self, stack_element: Option<StackElement>) -> ObjectKeyTypes {
        match stack_element {
            Some(StackElement::Key(key)) => {
                if self.kb.keypath_segments_len() == 1 && key.starts_with("_") {
                    if key == "_id" {
                        ObjectKeyTypes::Id
                    } else {
                        ObjectKeyTypes::Ignore
                    }
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
            if self.kb.keypath_segments_len() == 1 && key.starts_with("_") {
                if key == "_id" {
                    return Err(Error::Shred(
                        "Expected string for `_id` field, got another type".to_string()));
                } else {
                    self.ignore_children = 1;
                }
            } else {
                // Pop the dummy object that makes ObjectEnd happy
                // or the previous object key
                self.kb.pop_object_key();
                self.kb.push_object_key(key);
            }
        }
        Ok(())
    }

    pub fn shred(&mut self, json: &str, docseq: u64, batch: &mut rocksdb::WriteBatch) ->
            Result<String, Error> {
        let mut parser = Parser::new(json.chars());
        let mut token = parser.next();

        // this will keep track of objects where encountered keys.
        // if we didn't encounter keys then the top most element will be false.
        let mut object_keys_indexed = Vec::new();
        loop {
            // Get the next token, so that in case of an `ObjectStart` the key is already
            // on the stack.
            match token.take() {
                Some(JsonEvent::ObjectStart) => {
                    if self.ignore_children > 0 {
                        self.ignore_children += 1;
                    }
                    else {
                        try!(self.maybe_push_key(parser.stack().top()));
                        // Just push something to make `ObjectEnd` happy
                        self.kb.push_object_key("");
                        object_keys_indexed.push(false);
                    }
                },
                Some(JsonEvent::ObjectEnd) => {
                    if self.ignore_children > 0 {
                        self.ignore_children -= 1;
                    } else {
                        self.kb.pop_object_key();
                        if !object_keys_indexed.pop().unwrap() {
                            // this means we never wrote a key because the object was empty.
                            // So preserve the empty object by writing a special value.
                            try!(self.maybe_add_value(&parser, 'o', &[], docseq, batch));
                        }
                        self.kb.inc_top_array_offset();
                    }
                },
                Some(JsonEvent::ArrayStart) => {
                    if self.ignore_children > 0 {
                        self.ignore_children += 1;
                    } else {
                        try!(self.maybe_push_key(parser.stack().top()));
                        self.kb.push_array();
                    }
                },
                Some(JsonEvent::ArrayEnd) => {
                    if self.ignore_children > 0 {
                        self.ignore_children -= 1;
                    } else {
                        if self.kb.peek_array_offset() == 0 {
                            // this means we never wrote a value because the object was empty.
                            // So preserve the empty array by writing a special value.
                            self.kb.pop_array();
                            try!(self.maybe_add_value(&parser, 'a', &[], docseq, batch));
                        } else {
                            self.kb.pop_array();
                        }
                        self.kb.inc_top_array_offset();
                    }
                },
                Some(JsonEvent::StringValue(value)) => {
                    // No children to ignore
                    if self.ignore_children == 0 {
                        match self.extract_key(parser.stack().top()) {
                            ObjectKeyTypes::Id => {
                                self.doc_id = value.clone();
                                self.kb.pop_object_key();
                                self.kb.push_object_key("_id");
                                *object_keys_indexed.last_mut().unwrap() = true;

                                try!(self.add_entries(&value, docseq, batch));
                            },
                            ObjectKeyTypes::Key(key) => {
                                // Pop the dummy object that makes ObjectEnd happy
                                // or the previous object key
                                self.kb.pop_object_key();
                                self.kb.push_object_key(&key);
                                *object_keys_indexed.last_mut().unwrap() = true;

                                try!(self.add_entries(&value, docseq, batch));
                            },
                            ObjectKeyTypes::NoKey => {
                                try!(self.add_entries(&value, docseq, batch));
                                self.kb.inc_top_array_offset();
                            },
                            ObjectKeyTypes::Ignore => {
                                self.ignore_children = 1;
                            },
                        }
                    }
                },
                Some(JsonEvent::BooleanValue(tf)) => {
                    let code = if tf {'T'} else {'F'};
                    try!(self.maybe_add_value(&parser, code, &[], docseq, batch));
                },
                Some(JsonEvent::I64Value(i)) => {
                    let f = i as f64;
                    let bytes = unsafe{ transmute::<f64, [u8; 8]>(f) };
                    try!(self.maybe_add_value(&parser, 'f', &bytes[..], docseq, batch));
                },
                Some(JsonEvent::U64Value(u)) => {
                    let f = u as f64;
                    let bytes = unsafe{ transmute::<f64, [u8; 8]>(f) };
                    try!(self.maybe_add_value(&parser, 'f', &bytes[..], docseq, batch));
                },
                Some(JsonEvent::F64Value(f)) => {
                    let bytes = unsafe{ transmute::<f64, [u8; 8]>(f) };
                    try!(self.maybe_add_value(&parser, 'f', &bytes[..], docseq, batch));
                },
                Some(JsonEvent::NullValue) => {
                    try!(self.maybe_add_value(&parser, 'N', &[], docseq, batch));
                },
                Some(JsonEvent::Error(error)) => {
                    return Err(Error::Shred(error.to_string()));
                },
                None => {
                    break;
                }
            };

            token = parser.next();
        }
        Ok(self.doc_id.clone())
   }
}

/*
#[cfg(test)]
mod tests {
    extern crate rocksdb;
    use std::str;
    use records_capnp;
    use super::{WordInfo};
    use index::{Index, OpenOptions};

    fn wordinfos_from_rocks(rocks: &rocksdb::DB) -> Vec<(String, Vec<WordInfo>)> {
        let mut result = Vec::new();
        for (key, value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            if key[0] as char == 'W' {
                let mut ref_value = &*value;
                let message_reader = ::capnp::serialize_packed::read_message(
                    &mut ref_value, ::capnp::message::ReaderOptions::new()).unwrap();
                let payload = message_reader.get_root::<records_capnp::payload::Reader>().unwrap();

                let mut wordinfos = Vec::new();
                for wi in payload.get_wordinfos().unwrap().iter() {
                    wordinfos.push(WordInfo{
                        word_pos: wi.get_word_pos(),
                        suffix_text: wi.get_suffix_text().unwrap().to_string(),
                        suffix_offset: wi.get_suffix_offset(),
                    });
                }
                let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
                result.push((key_string, wordinfos));
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
        shredder.shred(json, docseq, &mut batch).unwrap();

        let dbname = "target/tests/test_shred_netsted";
        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();

        rocks.write(batch).unwrap();
        let result = wordinfos_from_rocks(&rocks);

        let expected = vec![
            ("W.some$!array#123,0".to_string(), vec![
                WordInfo { word_pos: 0, suffix_text: "".to_string(), suffix_offset: 5 }]),
            ("W.some$!data#123,1".to_string(), vec![
                WordInfo { word_pos: 0, suffix_text: "".to_string(), suffix_offset: 4 }]),
            ("W.some$$!also#123,2,0".to_string(), vec![
                WordInfo { word_pos: 0, suffix_text: "".to_string(), suffix_offset: 4 }]),
            ("W.some$$!nest#123,2,1".to_string(), vec![
                WordInfo { word_pos: 0, suffix_text: "ed".to_string(), suffix_offset: 4 }]),
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
        shredder.shred(json, docseq, &mut batch).unwrap();

        let dbname = "target/tests/test_shred_objects";
        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();

        rocks.write(batch).unwrap();
        let result = wordinfos_from_rocks(&rocks);
        println!("result: {:?}", result);
        let expected = vec![
            ("W.A$.B!b1#1234,1".to_string(), vec![
                WordInfo { word_pos: 0, suffix_text: "".to_string(), suffix_offset: 2 }]),
            ("W.A$.B!b2vmx#1234,0".to_string(), vec![
                WordInfo { word_pos: 0, suffix_text: "B2 VMX ".to_string(),
                           suffix_offset: 0 }]),
            ("W.A$.B!three#1234,0".to_string(), vec![
                WordInfo { word_pos: 10, suffix_text: "".to_string(), suffix_offset: 15 }]),
            ("W.A$.B!two#1234,0".to_string(), vec![
                WordInfo { word_pos: 6, suffix_text: " ".to_string(), suffix_offset: 9 }]),
            ("W.A$.C!..#1234,0".to_string(), vec![
                WordInfo { word_pos: 0, suffix_text: "".to_string(), suffix_offset: 2 }]),
            ("W.A$.C!..#1234,1".to_string(), vec![
                WordInfo { word_pos: 0, suffix_text: "".to_string(), suffix_offset: 2 }]),
            ("W.A$.C!c2#1234,0".to_string(), vec![
                WordInfo { word_pos: 2, suffix_text: "C2".to_string(), suffix_offset: 2 }]),
            ("W.A$.C!c2#1234,1".to_string(), vec![
                WordInfo { word_pos: 2, suffix_text: "C2".to_string(), suffix_offset: 2 }]),
            ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_shred_empty_object() {
        let mut shredder = super::Shredder::new();
        let json = r#"{}"#;
        let docseq = 123;
        let mut batch = rocksdb::WriteBatch::default();
        shredder.shred(json, docseq, &mut batch).unwrap();

        let dbname = "target/tests/test_shred_empty_object";
        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        
        let rocks = &index.rocks.unwrap();

        rocks.write(batch).unwrap();
        let result = wordinfos_from_rocks(&rocks);
        assert!(result.is_empty());
    }
} */
