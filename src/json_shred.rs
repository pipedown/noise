extern crate rustc_serialize;

use std::collections::HashMap;

use self::rustc_serialize::json::{JsonEvent, Parser, StackElement};

use key_builder::{KeyBuilder, SegmentType};
use stems::Stems;

// Good example of using rustc_serialize: https://github.com/ajroetker/beautician/blob/master/src/lib.rs
// Callback based JSON streaming parser: https://github.com/gyscos/json-streamer.rs
// Another parser pased on rustc_serializ: https://github.com/isagalaev/ijson-rust/blob/master/src/test.rs#L11


#[derive(Debug)]
struct WordInfo {
    //offset in the text field where the stemmed text starts
    stemmed_offset: usize,

    // the suffix of the stemmed text. When applied over stemmed, the original
    // text is returned.
    suffix_text: String,

    // the start of the suffixText
    suffix_offset: usize,
}

type ArrayOffsets = Vec<usize>;
type ArrayOffsetsToWordInfo = HashMap<ArrayOffsets, Vec<WordInfo>>;
type WordPathInfoMap = HashMap<String, ArrayOffsetsToWordInfo>;

#[derive(Debug)]
struct Shredder {
    keybuilder: KeyBuilder,
    map: WordPathInfoMap,
    path_array_offsets: ArrayOffsets,
}


impl Shredder {
    fn new() -> Shredder {
        Shredder{
            keybuilder: KeyBuilder::new(),
            map: WordPathInfoMap::new(),
            path_array_offsets: Vec::new(),
        }
    }
    fn add_entries(&mut self, text: String, docseq: u64) {
        let stems = Stems::new(text.as_str());
        for stem in stems {
            self.keybuilder.push_word(stem.stemmed.to_string());
            self.keybuilder.push_doc_seq(docseq);
            let map_path_array_offsets = self.map.entry(self.keybuilder.key())
                                                        .or_insert(ArrayOffsetsToWordInfo::new());
            let map_word_infos = map_path_array_offsets.entry(self.path_array_offsets.clone())
                .or_insert(Vec::new());
            map_word_infos.push(WordInfo{
                stemmed_offset: stem.stemmed_offset,
                suffix_text: stem.suffix.to_string(),
                suffix_offset: stem.suffix_offset,
            });
            self.keybuilder.pop_doc_seq();
            self.keybuilder.pop_word();
        }
        println!("add_entries: map: {:?}", self.map);
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


    fn shred(&mut self, json: &str, docseq: u64) {
        println!("{}", json);
        let mut parser = Parser::new(json.chars());
        let mut token = parser.next();

        loop {
            // Get the next token, so that in case of an `ObjectStart` the key is already
            // on the stack.
            let nexttoken = parser.next();

            match token.take() {
                Some(JsonEvent::ObjectStart) => {
                    match parser.stack().top() {
                        Some(StackElement::Key(key)) => {
                            println!("object start: {:?}", key);
                            self.keybuilder.push_object_key(key.to_string());
                            self.inc_top_array_offset();
                        },
                        _ => {
                            panic!("XXX This is probably an object end");
                        }
                    }
                },
                Some(JsonEvent::ObjectEnd) => {
                    self.keybuilder.pop_object_key();
                },
                Some(JsonEvent::ArrayStart) => {
                    println!("array start");
                    self.keybuilder.push_array();
                    //self.inc_top_array_offset();
                    self.path_array_offsets.push(0);
                },
                Some(JsonEvent::ArrayEnd) => {
                    self.path_array_offsets.pop();
                    self.keybuilder.pop_array();
                },
                Some(JsonEvent::StringValue(value)) => {
                    self.add_entries(value, docseq);
                    self.inc_top_array_offset();
                    //self.keybuilder.pop_object_key();
                },
                not_implemented => {
                    panic!("Not yet implemented other JSON types! {:?}", not_implemented);
                }
            };

            token = nexttoken;
            if token == None {
                break;
            }
        }
        println!("keybuilder: {}", self.keybuilder.key());
        println!("shredder: keys:");
        for key in self.map.keys() {
            println!("  {}", key);
        }
    }
}


#[cfg(test)]
mod tests {
    //use super::*;

    #[test]
    fn test_shred() {
        let mut shredder = super::Shredder::new();
        //let json = r#"{"hello": {"my": "world!"}, "anumber": 2}"#;
        //let json = r#"{"A":[{"B":"B2VMX two three","C":"C2"},{"B": "b1","C":"C2"}]}"#;
        //let json = r#"{"A":[[[{"B": "string within deeply nested array should be stemmed"}]]]}"#;
        //let json = r#"[{"A": 1, "B": 2, "C": 3}]"#;
        //let json = r#"{"foo": {"bar": 1}}"#;
        let json = r#"{"some": ["array", "data", ["also", "nested"]]}"#;
        let docseq = 123;
        shredder.shred(json, docseq);
    }
}
