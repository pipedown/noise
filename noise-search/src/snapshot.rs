use std::f32;
use std::str;

use crate::json_value::JsonValue;
use crate::key_builder::{KeyBuilder, Segment};
use crate::query::{DocResult, QueryScoringInfo};
use crate::returnable::{PathSegment, ReturnPath};
use noise_storage::{decode_varints, decode_zigzag_i32, BackendSnapshot, Cursor, SeekFrom};

pub struct Snapshot<S: BackendSnapshot> {
    snap: S,
}

impl<S: BackendSnapshot> Snapshot<S> {
    pub fn new(snap: S) -> Snapshot<S> {
        Snapshot { snap }
    }

    pub fn new_term_doc_result_iterator(&self, term: &str, kb: &KeyBuilder) -> DocResultIterator {
        DocResultIterator {
            iter: self.snap.iterator(),
            keypathword: kb.get_kp_word_only(term),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.snap.get(key)
    }

    pub fn new_scorer(&self, term: &str, kb: &KeyBuilder, boost: f32) -> Scorer {
        Scorer {
            iter: self.snap.iterator(),
            idf: f32::NAN,
            boost,
            kb: kb.clone(),
            term: term.to_string(),
            term_ordinal: 0,
        }
    }

    pub fn new_json_fetcher(&self) -> JsonFetcher {
        JsonFetcher {
            iter: self.snap.iterator(),
        }
    }

    pub fn new_iterator(&self) -> Cursor {
        self.snap.iterator()
    }

    pub fn new_multidim_iterator(&self, query: &[u8]) -> Cursor {
        self.snap.multidim_iterator(query)
    }

    pub fn new_all_docs_iterator(&self) -> AllDocsIterator {
        let mut iter = self.snap.iterator();
        iter.seek(SeekFrom::Key(b"S"));
        AllDocsIterator { iter }
    }
}

pub struct DocResultIterator {
    iter: Cursor,
    keypathword: Vec<u8>,
}

impl DocResultIterator {
    pub fn advance_gte(&mut self, start: &DocResult) {
        let mut seek_key = self.keypathword.clone();
        KeyBuilder::add_doc_result_to_kp_word(&mut seek_key, start);
        self.iter.seek(SeekFrom::Key(&seek_key));
    }

    pub fn next(&mut self) -> Option<(DocResult, TermPositions)> {
        let (key, value) = self.iter.current()?;
        if !key.starts_with(&self.keypathword) {
            // we passed the key path we are interested in. nothing left to do
            return None;
        }
        let dr = KeyBuilder::parse_doc_result_from_kp_word_key(key, self.keypathword.len());
        let pos = value.to_vec();
        self.iter.advance();
        Some((dr, TermPositions { pos }))
    }
}

pub struct TermPositions {
    pos: Vec<u8>,
}

impl TermPositions {
    pub fn positions(self) -> Vec<u32> {
        decode_varints(&self.pos)
            .into_iter()
            .map(|p| p as u32)
            .collect()
    }
}

pub struct Scorer {
    iter: Cursor,
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
            decode_zigzag_i32(bytes.as_ref()) as f32
        } else {
            0.0
        };

        let key = self.kb.kp_field_count_key();
        let num_docs = if let Some(bytes) = self.get_value(&key) {
            decode_zigzag_i32(bytes.as_ref()) as f32
        } else {
            0.0
        };

        self.idf = 1.0 + (num_docs / (doc_freq + 1.0)).ln();
        self.term_ordinal = qsi.num_terms;
        qsi.num_terms += 1;
        qsi.sum_of_idt_sqs += self.idf * self.idf;
    }

    pub fn get_value(&mut self, key: &[u8]) -> Option<Box<[u8]>> {
        if let Some((ret_key, ret_value)) = self.iter.seek(SeekFrom::Key(key)) {
            if ret_key.len() == key.len() && ret_key.starts_with(key) {
                Some(Box::from(ret_value))
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
                decode_zigzag_i32(bytes.as_ref()) as f32
            } else {
                panic!("Couldn't find field length for a match!! WHAT!");
            };

            let tf: f32 = (num_matches as f32).sqrt();
            let norm = 1.0 / total_field_words.sqrt();
            let score = self.idf * self.idf * tf * norm * self.boost;
            dr.add_score(self.term_ordinal, score);
        }
    }

    pub fn should_score(&self) -> bool {
        !self.idf.is_nan()
    }
}

pub struct JsonFetcher {
    iter: Cursor,
}

/// The keypath of `key` beyond the `value_key` prefix. A V-key is a binary seq followed by
/// the keypath text, so everything past a V-key prefix is valid UTF-8 by construction.
fn keypath_text<'a>(key: &'a [u8], value_key: &[u8]) -> &'a str {
    unsafe { str::from_utf8_unchecked(&key[value_key.len()..]) }
}

impl JsonFetcher {
    pub fn fetch(
        &mut self,
        seq: u64,
        kb_base: &mut KeyBuilder,
        rp: &ReturnPath,
    ) -> Option<JsonValue> {
        JsonFetcher::descend_return_path(&mut self.iter, seq, kb_base, rp, 0)
    }

    pub fn bytes_to_json_value(bytes: &[u8]) -> JsonValue {
        match bytes[0] as char {
            's' => {
                let string = unsafe { str::from_utf8_unchecked(&bytes[1..]) }.to_string();
                JsonValue::String(string)
            }
            'f' => {
                assert!(bytes.len() == 9);
                let double = f64::from_ne_bytes(bytes[1..9].try_into().unwrap());
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

    fn descend_return_path(
        iter: &mut Cursor,
        seq: u64,
        kb: &mut KeyBuilder,
        rp: &ReturnPath,
        mut rp_index: usize,
    ) -> Option<JsonValue> {
        while let Some(segment) = rp.nth(rp_index) {
            rp_index += 1;
            match *segment {
                PathSegment::ObjectKey(ref string) => {
                    kb.push_object_key(string);
                }
                PathSegment::ArrayAll => {
                    let mut i = 0;
                    let mut vec = Vec::new();
                    loop {
                        kb.push_array_index(i);
                        i += 1;
                        if let Some(json) = JsonFetcher::descend_return_path(
                            iter,
                            seq,
                            &mut kb.clone(),
                            rp,
                            rp_index,
                        ) {
                            vec.push(json);
                            kb.pop_array();
                        } else {
                            // we didn't get a value, is it because the array ends or the
                            // full path isn't there? check as there might be more array elements
                            // with a full path that does match.
                            let value_key = kb.kp_value_key(seq);
                            kb.pop_array();

                            // Seek in index to >= entry
                            if let Some((key, _value)) = iter.seek(SeekFrom::Key(&value_key)) {
                                if key.starts_with(&value_key) {
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
                PathSegment::Array(ref index) => {
                    kb.push_array_index(*index);
                }
            }
        }

        let value_key = kb.kp_value_key(seq);

        // Seek in index to >= entry
        let (key, _value) = iter.seek(SeekFrom::Key(&value_key))?;
        if !KeyBuilder::is_kp_value_key_prefix(&value_key, key) {
            // the cursor landed past the keypath, there is no value to fetch
            return None;
        }

        // The cursor is still on that entry, which is where `do_fetch` starts reading.
        Some(JsonFetcher::do_fetch(iter, &value_key))
    }

    /// When do_fetch is called it means we know we are going to find a value because
    /// we prefix matched the keypath. What we are doing here is parsing the remaining
    /// keypath to figure out the nested structure of the remaining keypath. So we
    /// depth first recursively parse the keypath and return the value and inserting into
    /// containers (arrays or objects) then iterate keys until the keypath no longer matches.
    ///
    /// The cursor is read in place: on entry it is positioned on the first index entry of
    /// this subtree, and on return it has been advanced just past the subtree (a scalar leaf
    /// advances once; a container leaves the cursor wherever its deepest leaf did). No entry
    /// is advanced past until it has been fully consumed, so the cursor's own position is the
    /// one-entry lookahead that the whole recursion shares.
    fn do_fetch(iter: &mut Cursor, value_key: &[u8]) -> JsonValue {
        let (key, value) = iter.current().expect("cursor not to be exhausted");
        if key.len() == value_key.len() {
            // we have a key match! Consume the leaf and move past it.
            let json = JsonFetcher::bytes_to_json_value(value);
            iter.advance();
            return json;
        }
        // The segment is owned, so the cursor borrow ends here and the recursion below is
        // free to advance the cursor.
        let segment = KeyBuilder::parse_first_kp_value_segment(keypath_text(key, value_key));

        match segment {
            Some((Segment::ObjectKey(unescaped), escaped)) => {
                JsonFetcher::fetch_object(iter, value_key, unescaped, escaped)
            }
            Some((Segment::Array(index), escaped)) => {
                JsonFetcher::fetch_array(iter, value_key, index, escaped)
            }
            None => panic!("somehow couldn't parse key segment {:?}", value_key),
        }
    }

    /// The first keypath segment of the entry the cursor is on, relative to `value_key`, or
    /// `None` if the cursor is exhausted or has moved past the `value_key` subtree.
    fn current_segment(iter: &Cursor, value_key: &[u8]) -> Option<(Segment, String)> {
        let (key, _value) = iter.current()?;
        if !KeyBuilder::is_kp_value_key_prefix(value_key, key) {
            return None;
        }
        KeyBuilder::parse_first_kp_value_segment(keypath_text(key, value_key))
    }

    /// Fetches the object at `value_key`, whose first key is the already-parsed segment
    /// `unescaped`/`escaped`. Ends at the first entry that isn't another key of this object.
    fn fetch_object(
        iter: &mut Cursor,
        value_key: &[u8],
        mut unescaped: String,
        mut escaped: String,
    ) -> JsonValue {
        let mut object: Vec<(String, JsonValue)> = Vec::new();
        let mut child_key = value_key.to_vec();
        loop {
            // `child_key` is reused across the keys, so reset it to `value_key` before
            // appending this key's segment
            child_key.truncate(value_key.len());
            child_key.extend_from_slice(escaped.as_bytes());
            object.push((unescaped, JsonFetcher::do_fetch(iter, &child_key)));

            // `do_fetch` left the cursor on the first entry beyond the child's subtree.
            match JsonFetcher::current_segment(iter, value_key) {
                Some((Segment::ObjectKey(next_unescaped), next_escaped)) => {
                    unescaped = next_unescaped;
                    escaped = next_escaped;
                }
                _ => return JsonValue::Object(object),
            }
        }
    }

    /// Fetches the array at `value_key`, whose first element is the already-parsed segment
    /// `index`/`escaped`. Ends at the first entry that isn't another element of this array.
    fn fetch_array(
        iter: &mut Cursor,
        value_key: &[u8],
        mut index: u64,
        mut escaped: String,
    ) -> JsonValue {
        // we keep the ordinal because we encounter elements in lexical sorting order
        // instead of ordinal order, `return_array` sorts them
        let mut array: Vec<(u64, JsonValue)> = Vec::new();
        let mut child_key = value_key.to_vec();
        loop {
            // `child_key` is reused across the elements, so reset it to `value_key` before
            // appending this element's segment
            child_key.truncate(value_key.len());
            child_key.extend_from_slice(escaped.as_bytes());
            array.push((index, JsonFetcher::do_fetch(iter, &child_key)));

            // `do_fetch` left the cursor on the first entry beyond the element's subtree.
            match JsonFetcher::current_segment(iter, value_key) {
                Some((Segment::Array(next_index), next_escaped)) => {
                    index = next_index;
                    escaped = next_escaped;
                }
                _ => return JsonFetcher::return_array(array),
            }
        }
    }
}

pub struct AllDocsIterator {
    iter: Cursor,
}

impl AllDocsIterator {
    pub fn next(&mut self) -> Option<DocResult> {
        let (key, _value) = self.iter.current()?;
        let seq = KeyBuilder::parse_seq_key(key)?;
        self.iter.advance();
        let mut dr = DocResult::new();
        dr.seq = seq;
        Some(dr)
    }
}
