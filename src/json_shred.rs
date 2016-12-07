extern crate rocksdb;
extern crate rustc_serialize;

use std::collections::HashMap;

use self::rustc_serialize::json::{JsonEvent, Parser, StackElement};

use error::Error;
use key_builder::KeyBuilder;
use records_capnp::payload;
use stems::Stems;

// Good example of using rustc_serialize: https://github.com/ajroetker/beautician/blob/master/src/lib.rs
// Callback based JSON streaming parser: https://github.com/gyscos/json-streamer.rs
// Another parser pased on rustc_serializ: https://github.com/isagalaev/ijson-rust/blob/master/src/test.rs#L11


#[derive(Debug, PartialEq)]
struct WordInfo {
    //offset in the text field where the stemmed text starts
    stemmed_offset: u64,

    // the suffix of the stemmed text. When applied over stemmed, the original
    // text is returned.
    suffix_text: String,

    // the start of the suffixText
    suffix_offset: u64,
}

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
    ignore_children: u64,
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
        let mut word_to_word_infos = HashMap::new();

        for stem in stems {
            let word_infos = word_to_word_infos.entry(stem.stemmed).or_insert(Vec::new());
            word_infos.push(WordInfo{
                stemmed_offset: stem.stemmed_offset as u64,
                suffix_text: stem.suffix.to_string(),
                suffix_offset: stem.suffix_offset as u64,
            });
        }
        for (stemmed, word_infos) in word_to_word_infos {
            let mut message = ::capnp::message::Builder::new_default();
            {
                let capn_payload = message.init_root::<payload::Builder>();
                let mut capn_wordinfos = capn_payload.init_wordinfos(word_infos.len() as u32);
                for (pos, word_info) in word_infos.iter().enumerate() {
                    let mut capn_wordinfo = capn_wordinfos.borrow().get(pos as u32);
                    capn_wordinfo.set_stemmed_offset(word_info.stemmed_offset);
                    capn_wordinfo.set_suffix_text(&word_info.suffix_text);
                    capn_wordinfo.set_suffix_offset(word_info.suffix_offset);
                }
            }

            let mut bytes = Vec::new();
            ::capnp::serialize_packed::write_message(&mut bytes, &message).unwrap();
            let key = self.kb.stemmed_word_key(&stemmed, docseq);
            try!(batch.put(&key.into_bytes(), &bytes));

        }
        let key = self.kb.value_key(docseq);
        try!(batch.put(&key.into_bytes(), &text.as_bytes()));

        Ok(())
    }

    // Extract key if it exists and indicates if it's a special type of key
    fn extract_key(&mut self, stack_element: Option<StackElement>) -> ObjectKeyTypes {
        if self.kb.last_pushed_keypath_is_object_key() {
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
        } else {
            ObjectKeyTypes::NoKey
        }
    }

    // If we are inside an object we need to push the key to the key builder
    // Don't push them if they are reserved fields (starting with underscore)
    fn maybe_push_key(&mut self, stack_element: Option<StackElement>) -> Result<(), Error> {
        if self.kb.last_pushed_keypath_is_object_key() {
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
        }
        Ok(())
    }

    pub fn shred(&mut self, json: &str, docseq: u64, batch: &mut rocksdb::WriteBatch) ->
            Result<String, Error> {
        let mut parser = Parser::new(json.chars());
        let mut token = parser.next();
        loop {
            // Get the next token, so that in case of an `ObjectStart` the key is already
            // on the stack.
            match token.take() {
                Some(JsonEvent::ObjectStart) => {
                    if self.ignore_children > 0 {
                        self.ignore_children += 1;
                    }
                    else {
                        // Just push something to make `ObjectEnd` happy
                        self.kb.push_object_key("");
                    }
                },
                Some(JsonEvent::ObjectEnd) => {
                    if self.ignore_children > 0 {
                        self.ignore_children -= 1;
                    } else {
                        self.kb.pop_object_key();
                        self.kb.inc_top_array_offset();
                    }
                },
                Some(JsonEvent::ArrayStart) => {
                    try!(self.maybe_push_key(parser.stack().top()));
                    if self.ignore_children > 0 {
                        self.ignore_children += 1;
                    } else {
                        self.kb.push_array();
                    }
                },
                Some(JsonEvent::ArrayEnd) => {
                    if self.ignore_children > 0 {
                        self.ignore_children -= 1;
                    } else {
                        self.kb.pop_array();
                        self.kb.inc_top_array_offset();
                    }
                },
                Some(JsonEvent::StringValue(value)) => {
                    // No children to ignore
                    if self.ignore_children == 0 {
                        match self.extract_key(parser.stack().top()) {
                            ObjectKeyTypes::Id => self.doc_id = value,
                            ObjectKeyTypes::Key(key) => {
                                // Pop the dummy object that makes ObjectEnd happy
                                // or the previous object key
                                self.kb.pop_object_key();
                                self.kb.push_object_key(&key);

                                try!(self.add_entries(&value, docseq, batch));
                                self.kb.inc_top_array_offset();
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
                not_implemented => {
                    panic!("Not yet implemented other JSON types! {:?}", not_implemented);
                }
            };

            token = parser.next();
            if token == None {
                break;
            }
        }
        Ok(self.doc_id.clone())
   }
}


#[cfg(test)]
mod tests {
    extern crate rocksdb;
    use std::str;
    use records_capnp;
    use super::{WordInfo};

    fn wordinfos_from_rocks(rocks: rocksdb::DB) -> Vec<(String, Vec<WordInfo>)> {
        let mut result = Vec::new();
        for (key, value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            let mut ref_value = &*value;
            let message_reader = ::capnp::serialize_packed::read_message(
                &mut ref_value, ::capnp::message::ReaderOptions::new()).unwrap();
            let payload = message_reader.get_root::<records_capnp::payload::Reader>().unwrap();

            let mut wordinfos = Vec::new();
            for wi in payload.get_wordinfos().unwrap().iter() {
                wordinfos.push(WordInfo{
                    stemmed_offset: wi.get_stemmed_offset(),
                    suffix_text: wi.get_suffix_text().unwrap().to_string(),
                    suffix_offset: wi.get_suffix_offset(),
                });
            }
            let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
            result.push((key_string, wordinfos));
        }
        result
    }


    #[test]
    fn test_shred_nested() {
        let mut shredder = super::Shredder::new();
        let json = r#"{"some": ["array", "data", ["also", "nested"]]}"#;
        let docseq = 123;
        let batch = rocksdb::WriteBatch::default();
        shredder.shred(json, docseq, &batch).unwrap();

        let rocks = rocksdb::DB::open_default("target/tests/test_shred_netsted").unwrap();
        rocks.write(batch).unwrap();
        let result = wordinfos_from_rocks(rocks);

        let expected = vec![
            ("W.some$!array#123,0".to_string(), vec![
                WordInfo { stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 5 }]),
            ("W.some$!data#123,1".to_string(), vec![
                WordInfo { stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 4 }]),
            ("W.some$$!also#123,2,0".to_string(), vec![
                WordInfo { stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 4 }]),
            ("W.some$$!nest#123,2,1".to_string(), vec![
                WordInfo { stemmed_offset: 0, suffix_text: "ed".to_string(), suffix_offset: 4 }]),
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
        let batch = rocksdb::WriteBatch::default();
        shredder.shred(json, docseq, &batch).unwrap();

        let rocks = rocksdb::DB::open_default("target/tests/test_shred_objects").unwrap();
        rocks.write(batch).unwrap();
        let result = wordinfos_from_rocks(rocks);
        println!("result: {:?}", result);
        let expected = vec![
            ("W.A$.B!b1#1234,1".to_string(), vec![
                WordInfo { stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 2 }]),
            ("W.A$.B!b2vmx#1234,0".to_string(), vec![
                WordInfo { stemmed_offset: 0, suffix_text: "B2 VMX ".to_string(),
                           suffix_offset: 0 }]),
            ("W.A$.B!three#1234,0".to_string(), vec![
                WordInfo { stemmed_offset: 10, suffix_text: "".to_string(), suffix_offset: 15 }]),
            ("W.A$.B!two#1234,0".to_string(), vec![
                WordInfo { stemmed_offset: 6, suffix_text: " ".to_string(), suffix_offset: 9 }]),
            ("W.A$.C!..#1234,0".to_string(), vec![
                WordInfo { stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 2 }]),
            ("W.A$.C!..#1234,1".to_string(), vec![
                WordInfo { stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 2 }]),
            ("W.A$.C!c2#1234,0".to_string(), vec![
                WordInfo { stemmed_offset: 2, suffix_text: "C2".to_string(), suffix_offset: 2 }]),
            ("W.A$.C!c2#1234,1".to_string(), vec![
                WordInfo { stemmed_offset: 2, suffix_text: "C2".to_string(), suffix_offset: 2 }]),
            ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_shred_empty_object() {
        let mut shredder = super::Shredder::new();
        let json = r#"{}"#;
        let docseq = 123;
        let batch = rocksdb::WriteBatch::default();
        shredder.shred(json, docseq, &batch).unwrap();

        let rocks = rocksdb::DB::open_default("target/tests/test_shred_empty_object").unwrap();
        rocks.write(batch).unwrap();
        let result = wordinfos_from_rocks(rocks);
        assert!(result.is_empty());
    }
}
