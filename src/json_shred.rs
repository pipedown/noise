extern crate rustc_serialize;

use std::collections::HashMap;

use self::rustc_serialize::json::{JsonEvent, Parser, StackElement};
use key_builder::{KeyBuilder, SegmentType};

// Good example of using rustc_serialize: https://github.com/ajroetker/beautician/blob/master/src/lib.rs
// Callback based JSON streaming parser: https://github.com/gyscos/json-streamer.rs
// Another parser pased on rustc_serializ: https://github.com/isagalaev/ijson-rust/blob/master/src/test.rs#L11


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

struct Shredder {
    keybuilder: KeyBuilder,
    map: WordPathInfoMap,
    path_array_offsets: Vec<usize>,
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
        // TODO vmx 2016-09-29: Do the stemming. For now we just split at whitespaces
        let words = text.split(" ");
        for word in words {
            // TODO vmx 2016-09-29: Set the `path_array_offset` properly
            let path_array_offset = ArrayOffsetsToWordInfo::new();
            println!("word: {}", word);
            self.keybuilder.push_word(word.to_string());
            self.keybuilder.push_doc_seq(docseq);
            self.map.insert(self.keybuilder.key(), path_array_offset);
            self.keybuilder.pop_doc_seq();
            self.keybuilder.pop_word();
        }
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
        loop {
            let token = match parser.next() {
                Some(tt) => tt,
                None => break
            };

            println!("token: {:?}: {:?}", token, parser.stack().top());
            match parser.stack().top() {
                Some(StackElement::Key(key)) => {
                    println!("key: {}", key);
                    match token {
                        JsonEvent::ObjectStart => {
                            self.inc_top_array_offset();
                            self.keybuilder.push_object_key(key.to_string());
                        },
                        JsonEvent::ObjectEnd => {
                            self.keybuilder.pop_object_key();
                        },
                        JsonEvent::ArrayStart => {
                            self.inc_top_array_offset();
                            self.keybuilder.push_object_key(key.to_string());
                            self.keybuilder.push_array();
                        },
                        JsonEvent::ArrayEnd => {
                            self.keybuilder.pop_array();
                        },
                        JsonEvent::StringValue(value) => {
                            self.inc_top_array_offset();
                            // TODO vmx 2016-09-22:  AddEntries()
                            self.keybuilder.push_object_key(key.to_string());
                            println!("about to add entries: keybuilder: {}", self.keybuilder.key());
                            self.add_entries(value, docseq);
                            self.keybuilder.pop_object_key();
                        },
                        _ => {},
                    }
                },
                _ => {},
            }
            println!("keybuilder: {}", self.keybuilder.key());
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
        let json = r#"{"A":[{"B":"B2VMX","C":"C2"},{"B": "b1","C":"C2"}]}"#;
        let docseq = 123;
        shredder.shred(json, docseq);
    }
}
