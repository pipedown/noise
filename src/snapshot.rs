use rocksdb::{self, DBIterator, Snapshot as RocksSnapshot, IteratorMode};

extern crate varint;

use std::io::Cursor;
use std::str;
use std::mem::transmute;
use std::iter::Peekable;
use std::f32;

use key_builder::{KeyBuilder, Segment};
use query::{DocResult, QueryScoringInfo};
use index::Index;
use returnable::{PathSegment, ReturnPath};
use json_value::JsonValue;
use self::varint::VarintRead;


pub struct Snapshot<'a> {
    rocks: RocksSnapshot<'a>,
}

impl<'a> Snapshot<'a> {
    pub fn new(rocks: RocksSnapshot) -> Snapshot {
        Snapshot { rocks: rocks }
    }

    pub fn new_term_doc_result_iterator(&self, term: &str, kb: &KeyBuilder) -> DocResultIterator {
        DocResultIterator {
            iter: self.rocks.iterator(IteratorMode::Start),
            keypathword: kb.get_kp_word_only(&term),
        }

    }

    pub fn get(&self, key: &[u8]) -> Option<rocksdb::DBVector> {
        self.rocks.get(key).unwrap()
    }

    pub fn new_scorer(&self, term: &str, kb: &KeyBuilder, boost: f32) -> Scorer {
        Scorer {
            iter: self.rocks.iterator(IteratorMode::Start),
            idf: f32::NAN,
            boost: boost,
            kb: kb.clone(),
            term: term.to_string(),
            term_ordinal: 0,
        }
    }

    pub fn new_json_fetcher(&self) -> JsonFetcher {
        JsonFetcher { iter: self.rocks.iterator(IteratorMode::Start) }
    }

    pub fn new_iterator(&self) -> DBIterator {
        self.rocks.iterator(IteratorMode::Start)
    }

    pub fn new_rtree_iterator(&self, query: &[u8]) -> DBIterator {
        self.rocks.rtree_iterator(query)
    }

    pub fn new_all_docs_iterator(&self) -> AllDocsIterator {
        let mut iter = self.rocks.iterator(IteratorMode::Start);
        iter.set_mode(IteratorMode::From(b"S", rocksdb::Direction::Forward));
        AllDocsIterator { iter: iter }
    }
}

pub struct DocResultIterator {
    iter: DBIterator,
    keypathword: String,
}

impl DocResultIterator {
    pub fn advance_gte(&mut self, start: &DocResult) {
        KeyBuilder::add_doc_result_to_kp_word(&mut self.keypathword, &start);
        // Seek in index to >= entry
        self.iter
            .set_mode(IteratorMode::From(self.keypathword.as_bytes(), rocksdb::Direction::Forward));
        KeyBuilder::truncate_to_kp_word(&mut self.keypathword);
    }

    pub fn next(&mut self) -> Option<(DocResult, TermPositions)> {
        if let Some((key, value)) = self.iter.next() {
            if !key.starts_with(self.keypathword.as_bytes()) {
                // we passed the key path we are interested in. nothing left to do */
                return None;
            }

            let key_str = unsafe { str::from_utf8_unchecked(&key) };
            let dr = KeyBuilder::parse_doc_result_from_kp_word_key(&key_str);

            Some((dr, TermPositions { pos: value.into_vec() }))
        } else {
            None
        }
    }
}


pub struct TermPositions {
    pos: Vec<u8>,
}

impl TermPositions {
    pub fn positions(self) -> Vec<u32> {
        let mut bytes = Cursor::new(self.pos);
        let mut positions = Vec::new();
        while let Ok(pos) = bytes.read_unsigned_varint_32() {
            positions.push(pos);
        }
        positions
    }
}

pub struct Scorer {
    iter: DBIterator,
    idf: f32,
    boost: f32,
    kb: KeyBuilder,
    term: String,
    term_ordinal: usize,
}

impl Scorer {
    pub fn init(&mut self, qsi: &mut QueryScoringInfo) {
        let key = self.kb.kp_word_count_key(&self.term);
        let doc_freq = if let Some(bytes) = self.get_value(&key) {
            Index::convert_bytes_to_i32(bytes.as_ref()) as f32
        } else {
            0.0
        };

        let key = self.kb.kp_field_count_key();
        let num_docs = if let Some(bytes) = self.get_value(&key) {
            Index::convert_bytes_to_i32(bytes.as_ref()) as f32
        } else {
            0.0
        };

        self.idf = 1.0 + (num_docs / (doc_freq + 1.0)).ln();
        self.term_ordinal = qsi.num_terms;
        qsi.num_terms += 1;
        qsi.sum_of_idt_sqs += self.idf * self.idf;
    }

    pub fn get_value(&mut self, key: &str) -> Option<Box<[u8]>> {
        self.iter
            .set_mode(IteratorMode::From(key.as_bytes(), rocksdb::Direction::Forward));
        if let Some((ret_key, ret_value)) = self.iter.next() {
            if ret_key.len() == key.len() && ret_key.starts_with(key.as_bytes()) {
                Some(ret_value)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn add_match_score(&mut self, num_matches: u32, dr: &mut DocResult) {
        if self.should_score() {
            let key = self.kb.kp_field_length_key_from_doc_result(dr);
            let total_field_words = if let Some(bytes) = self.get_value(&key) {
                Index::convert_bytes_to_i32(bytes.as_ref()) as f32
            } else {
                panic!("Couldn't find field length for a match!! WHAT!");
            };

            let tf: f32 = (num_matches as f32).sqrt();
            let norm = 1.0 / (total_field_words as f32).sqrt();
            let score = self.idf * self.idf * tf * norm * self.boost;
            dr.add_score(self.term_ordinal, score);
        }
    }

    pub fn should_score(&self) -> bool {
        !self.idf.is_nan()
    }
}


pub struct JsonFetcher {
    iter: DBIterator,
}

impl JsonFetcher {
    pub fn fetch(&mut self,
                 seq: u64,
                 mut kb_base: &mut KeyBuilder,
                 rp: &ReturnPath)
                 -> Option<JsonValue> {
        JsonFetcher::descend_return_path(&mut self.iter, seq, &mut kb_base, &rp, 0)
    }

    pub fn bytes_to_json_value(bytes: &[u8]) -> JsonValue {
        match bytes[0] as char {
            's' => {
                let string = unsafe { str::from_utf8_unchecked(&bytes[1..]) }.to_string();
                JsonValue::String(string)
            }
            'f' => {
                assert!(bytes.len() == 9);
                let mut bytes2: [u8; 8] = [0; 8];
                for (n, b) in bytes[1..9].iter().enumerate() {
                    bytes2[n] = *b;
                }
                let double: f64 = unsafe { transmute(bytes2) };
                JsonValue::Number(double)
            }
            'T' => JsonValue::True,
            'F' => JsonValue::False,
            'N' => JsonValue::Null,
            'o' => JsonValue::Object(vec![]),
            'a' => JsonValue::Array(vec![]),
            what => panic!("unexpected type tag in value: {}", what),
        }
    }

    fn return_array(mut array: Vec<(u64, JsonValue)>) -> JsonValue {
        array.sort_by_key(|tuple| tuple.0);
        JsonValue::Array(array.into_iter().map(|(_i, json)| json).collect())
    }

    fn descend_return_path(iter: &mut DBIterator,
                           seq: u64,
                           kb: &mut KeyBuilder,
                           rp: &ReturnPath,
                           mut rp_index: usize)
                           -> Option<JsonValue> {

        while let Some(segment) = rp.nth(rp_index) {
            rp_index += 1;
            match segment {
                &PathSegment::ObjectKey(ref string) => {
                    kb.push_object_key(string);
                }
                &PathSegment::ArrayAll => {
                    let mut i = 0;
                    let mut vec = Vec::new();
                    loop {
                        kb.push_array_index(i);
                        i += 1;
                        if let Some(json) = JsonFetcher::descend_return_path(iter,
                                                                             seq,
                                                                             &mut kb.clone(),
                                                                             rp,
                                                                             rp_index) {
                            vec.push(json);
                            kb.pop_array();
                        } else {
                            // we didn't get a value, is it because the array ends or the
                            // full path isn't there? check as there might be more array elements
                            // with a full path that does match.
                            let value_key = kb.kp_value_key(seq);
                            kb.pop_array();

                            // Seek in index to >= entry
                            iter.set_mode(IteratorMode::From(value_key.as_bytes(),
                                                             rocksdb::Direction::Forward));

                            if let Some((key, _value)) = iter.next() {
                                if key.starts_with(value_key.as_bytes()) {
                                    // yes it exists. loop again.
                                    continue;
                                }
                            }

                            if vec.is_empty() {
                                return None;
                            } else {
                                return Some(JsonValue::Array(vec));
                            }
                        }
                    }
                }
                &PathSegment::Array(ref index) => {
                    kb.push_array_index(*index);
                }
            }
        }

        let value_key = kb.kp_value_key(seq);

        // Seek in index to >= entry
        iter.set_mode(IteratorMode::From(value_key.as_bytes(), rocksdb::Direction::Forward));

        let (key, value) = match iter.next() {
            Some((key, value)) => (key, value),
            None => return None,
        };

        if !KeyBuilder::is_kp_value_key_prefix(&value_key, unsafe { str::from_utf8_unchecked(&key) }) {
            return None;
        }
        Some(JsonFetcher::do_fetch(&mut iter.peekable(), &value_key, key, value))
    }

    /// When do_fetch is called it means we know we are going to find a value because
    /// we prefix matched the keypath. What we are doing here is parsing the remaining
    /// keypath to figure out the nested structure of the remaining keypath. So we
    /// depth first recursively parse the keypath and return the value and inserting into
    /// containers (arrays or objects) then iterate keys until the keypath no longer matches.
    fn do_fetch(iter: &mut Peekable<&mut DBIterator>,
                value_key: &str,
                mut key: Box<[u8]>,
                mut value: Box<[u8]>)
                -> JsonValue {

        if key.len() == value_key.len() {
            // we have a key match!
            return JsonFetcher::bytes_to_json_value(value.as_ref());
        }
        let segment = {
            let key_str = unsafe { str::from_utf8_unchecked(&key) };
            let remaining = &key_str[value_key.len()..];
            KeyBuilder::parse_first_kp_value_segment(&remaining)
        };

        match segment {
            Some((Segment::ObjectKey(mut unescaped), escaped)) => {
                let mut object: Vec<(String, JsonValue)> = Vec::new();

                let mut value_key_next = value_key.to_string() + &escaped;
                loop {
                    let json_val = JsonFetcher::do_fetch(iter, &value_key_next, key, value);
                    object.push((unescaped, json_val));

                    let segment = match iter.peek() {
                        Some(&(ref k, ref _v)) => {
                            let key = unsafe { str::from_utf8_unchecked(k) };
                            if !KeyBuilder::is_kp_value_key_prefix(value_key, key) {
                                return JsonValue::Object(object);
                            }

                            let key_str = unsafe { str::from_utf8_unchecked(&k) };
                            let remaining = &key_str[value_key.len()..];

                            KeyBuilder::parse_first_kp_value_segment(&remaining)
                        }
                        None => return JsonValue::Object(object),
                    };

                    if let Some((Segment::ObjectKey(unescaped2), escaped2)) = segment {
                        unescaped = unescaped2;
                        // advance the peeked iter
                        match iter.next() {
                            Some((k, v)) => {
                                key = k;
                                value = v;
                            }
                            None => panic!("couldn't advanced already peeked iter"),
                        };
                        value_key_next.truncate(value_key.len());
                        value_key_next.push_str(&escaped2);
                    } else {
                        return JsonValue::Object(object);
                    }
                }
            }
            Some((Segment::Array(mut i), escaped)) => {
                // we use a tuple with ordinal because we encounter
                // elements in lexical sorting order instead of ordinal order
                let mut array: Vec<(u64, JsonValue)> = Vec::new();

                let mut value_key_next = value_key.to_string() + &escaped;
                loop {
                    let json_val = JsonFetcher::do_fetch(iter, &value_key_next, key, value);
                    array.push((i, json_val));

                    let segment = match iter.peek() {
                        Some(&(ref k, ref _v)) => {
                            let key = unsafe { str::from_utf8_unchecked(k) };
                            if !KeyBuilder::is_kp_value_key_prefix(value_key, key) {
                                return JsonFetcher::return_array(array);
                            }

                            let key_str = unsafe { str::from_utf8_unchecked(&k) };
                            let remaining = &key_str[value_key.len()..];

                            KeyBuilder::parse_first_kp_value_segment(&remaining)
                        }
                        None => return JsonFetcher::return_array(array),
                    };

                    if let Some((Segment::Array(i2), escaped2)) = segment {
                        i = i2;
                        // advance the already peeked iter
                        match iter.next() {
                            Some((k, v)) => {
                                key = k;
                                value = v;
                            }
                            None => panic!("couldn't advanced already peeked iter"),
                        };
                        value_key_next.truncate(value_key.len());
                        value_key_next.push_str(&escaped2);
                    } else {
                        return JsonFetcher::return_array(array);
                    }
                }
            }
            None => {
                let key_str = unsafe { str::from_utf8_unchecked(&key) };
                panic!("somehow couldn't parse key segment {} {}",
                       value_key,
                       key_str);
            }
        }
    }
}

pub struct AllDocsIterator {
    iter: DBIterator,
}

impl AllDocsIterator {
    pub fn next(&mut self) -> Option<DocResult> {
        match self.iter.next() {
            Some((k, _v)) => {
                let key = unsafe { str::from_utf8_unchecked(&k) };
                if let Some(seq) = KeyBuilder::parse_seq_key(key) {
                    let mut dr = DocResult::new();
                    dr.seq = seq;
                    Some(dr)
                } else {
                    None
                }
            }
            None => None,
        }
    }
}
