//extern crate serde_json;
extern crate rustc_serialize;

//use std::fmt;
use std::collections::HashMap;

//use self::serde_json::{StreamDeserializer, Value};
use self::rustc_serialize::json::{JsonEvent, Parser, Stack, StackElement};
use key_builder::{KeyBuilder, SegmentType};

//struct PrintableStack(Stack);
//pub struct Stack {
//    stack: Vec<InternalStackElement>,
//    str_buffer: Vec<u8>,
//}


//impl fmt::Debug for PrintableStack {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        write!(f, "yay")
//    }
//}

// Good example of using rustc_serialize: https://github.com/ajroetker/beautician/blob/master/src/lib.rs
// Callback based JSON streaming parser: https://github.com/gyscos/json-streamer.rs
// Another parser pased on rustc_serializ: https://github.com/isagalaev/ijson-rust/blob/master/src/test.rs#L11


struct word_info {
    //offset in the text field where the stemmed text starts
    stemmed_offset: usize,

    // the suffix of the stemmed text. When applied over stemmed, the original
    // text is returned.
    suffix_text: String,

    // the start of the suffixText
    suffix_offset: usize,
}

type array_offsets = Vec<usize>;
type array_offsets_to_word_info = HashMap<array_offsets, Vec<word_info>>;
type word_path_info_map = HashMap<String, array_offsets_to_word_info>;
                                          
struct Shredder<'a> {
    json: &'a str,
    docseq: u64,
    map: word_path_info_map,
    path_array_offsets: Vec<usize>,
}


impl<'a> Shredder<'a> {
    fn add_entries(&mut self, kb: &mut KeyBuilder, text: String) {
        // TODO vmx 2016-09-29: Do the stemming. For now we just split at whitespaces
        let words = text.split(" ");
        for word in words {
            // TODO vmx 2016-09-29: Set the `path_array_offset` properly
            let path_array_offset = array_offsets_to_word_info::new();
            println!("word: {}", word);
            kb.push_word(word.to_string());
            kb.push_doc_seq(self.docseq);
            self.map.insert(kb.key(), path_array_offset);
            kb.pop_doc_seq();
            kb.pop_word();
        }
    }

    fn inc_top_array_offset(&mut self, kb: &KeyBuilder) {
        // we encounter a new element. if we are a child element of an array
        // increment the offset. If we aren't (we are the root value or a map
        // value) we don't increment
        if let Some(SegmentType::Array) = kb.last_pushed_segment_type() {
            if let Some(last) = self.path_array_offsets.last_mut() {
                *last += 1;
            }
        }
    }
}


// TODO vmx 2009-09-29: Make it a metho of `Shredder`
fn shred() {
    //let json = r#"{"hello": {"my": "world!"}, "anumber": 2}"#;
    let json = r#"{"A":[{"B":"B2VMX","C":"C2"},{"B": "b1","C":"C2"}]}"#;
    let docseq = 123;
    println!("{}", json);
    let mut shredder = Shredder{
        json: json,
        docseq: docseq,
        map: word_path_info_map::new(),
        path_array_offsets: Vec::new(),
    };

    let mut kb = KeyBuilder::new();
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
                        shredder.inc_top_array_offset(&kb);
                        kb.push_object_key(key.to_string());
                    },
                    JsonEvent::ObjectEnd => {
                        kb.pop_object_key();
                    },      
                    JsonEvent::ArrayStart => {
                        shredder.inc_top_array_offset(&kb);
                        kb.push_object_key(key.to_string());
                        kb.push_array();
                    },
                    JsonEvent::ArrayEnd => {
                        kb.pop_array();
                    },
                    JsonEvent::StringValue(value) => {
                        shredder.inc_top_array_offset(&kb);
                        // TODO vmx 2016-09-22:  AddEntries()
                        kb.push_object_key(key.to_string());
                        println!("about to add entries: keybuilder: {}", kb.key());
                        shredder.add_entries(&mut kb, value);
                        kb.pop_object_key();
                    },
                    _ => {},
                }
            },
            _ => {},
        }
        println!("keybuilder: {}", kb.key());
        //match token {
        //    JsonEvent::ObjectStart => {
        //        // push a dummy value
        //        kb.push_object_key("".to_string());
        //    },
        //    //JsonString:
        //    //    //let value = parser.next();
        //    //    //let key = parser.stack().top().unwrap();
        //    //    //let key = match parser.stack().top().unwrap() {
        //    //    match parser.stack().top() {
        //    //        Some(StackElement::Index(_)) => { println!("index!") },
        //    //        Some(StackElement::Key(key)) => { println!("Key: {}!", key) },
        //    //        None => { println!("none") },
        //    //    };
        //    //    //println!("key-value: {:?} {:?}", key, value);
        //    //    //kb.push_object_key(key);
        //    //},
        //    _ => {}
        //}
        
    }
    println!("keybuilder: {}", kb.key());
    println!("shredder: keys:");
    for key in shredder.map.keys() {
        println!("  {}", key);
    }
    //let mut parsed: StreamDeserializer<Value, _> = StreamDeserializer::new(
    //    json.as_bytes().iter().map(|byte| Ok(*byte)));
    //println!("{:?}", parsed.next().unwrap().next().unwrap());
    //    //let stream = "{\"x\":40}\n{\"x\":".to_string();
    ////let mut parsed: StreamDeserializer<Value, _> = StreamDeserializer::new(
    ////    stream.as_bytes().iter().map(|byte| Ok(*byte))
    ////);
}


#[cfg(test)]
mod tests {
    //use super::*;

    #[test]
    fn test_shred() {
        super::shred();
    }
}
