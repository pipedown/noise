extern crate rocksdb;
extern crate rustc_serialize;

use std::collections::HashMap;

use self::rustc_serialize::json::{JsonEvent, Parser, StackElement};

use error::Error;
use key_builder::{KeyBuilder, SegmentType};
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

#[derive(Debug)]
pub struct Shredder {
    keybuilder: KeyBuilder,
    path_array_offsets: ArrayOffsets,
    // Top-level fields prefixed with an underscore are ignored
    ignore_children: u64,
    doc_id: String,
}


impl Shredder {
    pub fn new() -> Shredder {
        Shredder{
            keybuilder: KeyBuilder::new(),
            path_array_offsets: Vec::new(),
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
            self.keybuilder.push_word(&stemmed);
            self.keybuilder.push_doc_seq(docseq);
            self.keybuilder.push_array_path(&self.path_array_offsets);

            let mut bytes = Vec::new();
            ::capnp::serialize_packed::write_message(&mut bytes, &message).unwrap();
            try!(batch.put(&self.keybuilder.key().into_bytes(), &bytes));

            self.keybuilder.pop_array_path();
            self.keybuilder.pop_doc_seq();
            self.keybuilder.pop_word();
        }
        Ok(())
    }


    fn inc_top_array_offset(&mut self) {
        // we encounter a new element. if we are a child element of an array
        // increment the offset. If we aren't (we are the root value or a map
        // value) we don't increment
        if let Some(SegmentType::Array) = self.keybuilder.last_pushed_segment_type() {
            if let Some(last) = self.path_array_offsets.last_mut() {
                *last += 1;
            }
        }
    }

    // Extract key if it exists and indicates if it's a special type of key
    fn extract_key(&mut self, stack_element: Option<StackElement>) -> ObjectKeyTypes {
        if self.keybuilder.last_pushed_segment_type().unwrap() == SegmentType::ObjectKey {
            match stack_element {
                Some(StackElement::Key(key)) => {
                    if self.keybuilder.segments.len() == 1 && key.starts_with("_") {
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
        if self.keybuilder.last_pushed_segment_type().unwrap() == SegmentType::ObjectKey {
            if let Some(StackElement::Key(key)) = stack_element {
                if self.keybuilder.segments.len() == 1 && key.starts_with("_") {
                    if key == "_id" {
                        return Err(Error::Shred(
                            "Expected string for `_id` field, got another type".to_string()));
                    } else {
                        self.ignore_children = 1;
                    }
                } else {
                    // Pop the dummy object that makes ObjectEnd happy
                    // or the previous object key
                    self.keybuilder.pop_object_key();
                    self.keybuilder.push_object_key(key.to_string());
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
                        self.keybuilder.push_object_key("".to_string());
                    }
                },
                Some(JsonEvent::ObjectEnd) => {
                    if self.ignore_children > 0 {
                        self.ignore_children -= 1;
                    } else {//if !self.keybuilder.segments.is_empty() {
                        self.keybuilder.pop_object_key();
                        self.inc_top_array_offset();
                    }
                },
                Some(JsonEvent::ArrayStart) => {
                    try!(self.maybe_push_key(parser.stack().top()));
                    if self.ignore_children > 0 {
                        self.ignore_children += 1;
                    } else {
                        self.keybuilder.push_array();
                        self.path_array_offsets.push(0);
                    }
                },
                Some(JsonEvent::ArrayEnd) => {
                    if self.ignore_children > 0 {
                        self.ignore_children -= 1;
                    } else {
                        self.path_array_offsets.pop();
                        self.keybuilder.pop_array();
                        self.inc_top_array_offset();
                    }
                },
                Some(JsonEvent::StringValue(value)) => {
                    // No children to ignore
                    if self.ignore_children == 0 {
                        println!("stringvalue: {:?}", value);
                        match self.extract_key(parser.stack().top()) {
                            ObjectKeyTypes::Id => self.doc_id = value,
                            ObjectKeyTypes::Key(key) => {
                                // Pop the dummy object that makes ObjectEnd happy
                                // or the previous object key
                                self.keybuilder.pop_object_key();
                                self.keybuilder.push_object_key(key.to_string());

                                try!(self.add_entries(&value, docseq, batch));
                                self.inc_top_array_offset();
                            },
                            ObjectKeyTypes::NoKey => {
                                try!(self.add_entries(&value, docseq, batch));
                                self.inc_top_array_offset();
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

/*

#[cfg(test)]
mod tests {
    
    use super::{WordInfo};
    #[test]
    
    fn test_shred_nested() {
        let mut shredder = super::Shredder::new();
        //let json = r#"{"hello": {"my": "world!"}, "anumber": 2}"#;
        //let json = r#"{"A":[{"B":"B2VMX two three","C":"C2"},{"B": "b1","C":"C2"}]}"#;
        //let json = r#"{"A":[[[{"B": "string within deeply nested array should be stemmed"}]]]}"#;
        //let json = r#"[{"A": 1, "B": 2, "C": 3}]"#;
        //let json = r#"{"foo": {"bar": 1}}"#;
        let json = r#"{"some": ["array", "data", ["also", "nested"]]}"#;
        let docseq = 123;
        shredder.shred(json, docseq).unwrap();
        let expected = vec![
            ("W.some$!array#123,0", vec![
                (vec![0], vec![WordInfo {
                    stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 5 }])]),
            ("W.some$!data#123,1", vec![
                (vec![1], vec![WordInfo {
                    stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 4 }])]),
            ("W.some$$!also#123,2,0", vec![
                (vec![2, 0], vec![WordInfo {
                    stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 4 }])]),
            ("W.some$$!nest#1232,1", vec![
                (vec![2, 1], vec![WordInfo {
                    stemmed_offset: 0, suffix_text: "ed".to_string(), suffix_offset: 4 }])]),
            ];
        compare_shredded(&shredder.map, &expected);
    }

    #[test]
    fn test_shred_objects() {
        let mut shredder = super::Shredder::new();
        let json = r#"{"A":[{"B":"B2VMX two three","C":"..C2"},{"B": "b1","C":"..C2"}]}"#;
        let docseq = 1234;
        shredder.shred(json, docseq).unwrap();
        let expected = vec![
            ("W.A$.B!b1#1234", vec![
                (vec![0], vec![
                    WordInfo {
                        stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 2 }])]),
            ("W.A$.B!b2vmx#1234", vec![
                (vec![0], vec![
                    WordInfo {
                        stemmed_offset: 0, suffix_text: "B2VMX ".to_string(), suffix_offset: 0 }])]),
            ("W.A$.B!three#1234", vec![
                (vec![0], vec![WordInfo {
                    stemmed_offset: 10, suffix_text: "".to_string(), suffix_offset: 15 }])]),
            ("W.A$.B!two#1234", vec![
                (vec![0], vec![WordInfo {
                    stemmed_offset: 6, suffix_text: " ".to_string(), suffix_offset: 9 }])]),
            ("W.A$.C!..#1234", vec![
                (vec![0], vec![
                    WordInfo {
                        stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 2 },
                    WordInfo {
                        stemmed_offset: 0, suffix_text: "".to_string(), suffix_offset: 2 }])]),
            ("W.A$.C!c2#1234", vec![
                (vec![0], vec![
                    WordInfo {
                        stemmed_offset: 2, suffix_text: "C2".to_string(), suffix_offset: 2 },
                    WordInfo {
                        stemmed_offset: 2, suffix_text: "C2".to_string(), suffix_offset: 2 }])]),
            ];
        compare_shredded(&shredder.map, &expected);
    }

    fn compare_shredded(result_map: &WordPathInfoMap,
                        expected: &Vec<(&str, Vec<(Vec<u64>, Vec<WordInfo>)>)>) {
        // HashMap have an arbitrary order of the elements
        let mut result: Vec<(&String, &ArrayOffsetsToWordInfo)> = result_map.into_iter().collect();
        result.sort_by(|a, b| Ord::cmp(&a.0, &b.0));
        for (ii, &(key, values)) in result.iter().enumerate() {
            assert_eq!(key, expected[ii].0);
            let mut wordinfos: Vec<(&Vec<u64>, &Vec<WordInfo>)> = values.iter().collect();
            wordinfos.sort_by_key(|item| item.0);
            for (jj, wordinfo) in wordinfos.iter().enumerate() {
                assert_eq!(wordinfo.0, &expected[ii].1[jj].0);
                assert_eq!(wordinfo.1, &expected[ii].1[jj].1);
            }
        }
    }

    #[test]
    fn test_shred_empty_object() {
        let mut shredder = super::Shredder::new();
        let json = r#"{}"#;
        let docseq = 123;
        shredder.shred(json, docseq).unwrap();
        assert!(shredder.map.is_empty());
    } 
}
*/
