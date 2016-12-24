use query::DocResult;
use std::str;

pub enum Segment {
    ObjectKey(String),
    Array(u64),
} 

#[derive(Debug, Clone)]
pub struct KeyBuilder {
    keypath: Vec<String>,
    arraypath: Vec<u64>,
}

impl KeyBuilder {
    pub fn new() -> KeyBuilder {
        KeyBuilder{
             // Magic reserve number is completely arbitrary
            keypath: Vec::with_capacity(10),
            arraypath: Vec::with_capacity(10),
        }
    }

    pub fn get_keypathword_only(&self, word: &str) -> String {
        let mut string = String::with_capacity(100);
        string.push('W');
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('!');
        string.push_str(word);
        string.push('#');
        string
    }

    /// Builds a stemmed word key for the input word and seq, using the key_path and arraypath
    /// built up internally.
    pub fn stemmed_word_key(&self, word: &str, seq: u64) -> String {
        let mut string = self.get_keypathword_only(&word);
        string.push_str(seq.to_string().as_str());

        KeyBuilder::add_arraypath(&mut string, &self.arraypath);
        string
    }

    /// Adds DocResult seq and array path an already created keypathword.
    pub fn add_doc_result_to_keypathword(keypathword: &mut String, dr: &DocResult) {
        keypathword.push_str(dr.seq.to_string().as_str());
        KeyBuilder::add_arraypath(keypathword, &dr.arraypath);
    }

    pub fn truncate_to_keypathword(stemmed_word_key: &mut String) {
        let n = stemmed_word_key.rfind("#").unwrap();
        stemmed_word_key.truncate(n + 1);
    }


    /// Builds a value key for seq (value keys are the original json terminal value with
    /// keyed on keypath and arraypath built up internally).
    pub fn value_key(&self, seq: u64) -> String {
        let mut string = String::with_capacity(100);
        string.push('V');
        string.push_str(&seq.to_string());
        string.push('#');
        let mut i = 0;
        for segment in &self.keypath {
            string.push_str(&segment);
            if segment == "$" {
                string.push_str(&self.arraypath[i].to_string());
                i += 1;
            }
        }
        string
    }

    fn add_arraypath(string: &mut String, arraypath: &Vec<u64>) {
        if arraypath.is_empty() {
            string.push(',');
        } else {
            for i in arraypath {
                string.push(',');
                string.push_str(i.to_string().as_str());
            }
        }
    }

    // returns the unescaped segment as Segment and the escaped segment as a slice 
    pub fn parse_first_key_value_segment(keypath: &str) -> Option<(Segment, String)> {

        let mut unescaped = String::with_capacity(50);
        let mut len_bytes = 1;
        let mut chars = keypath.chars();

        // first char must be a . or a $ or we've exceeded the keypath
        match chars.next() {
            Some('.') => {
                loop {
                    match chars.next() {
                        Some('\\') => {
                            if let Some(c) = chars.next() {
                                len_bytes += c.len_utf8();
                                unescaped.push(c);
                            } else {
                                panic!("Escape char found as last char in keypath");
                            }
                        },
                        Some('.') | Some('$') => {
                            break;
                        },
                        Some(c) => {
                            len_bytes += c.len_utf8();
                            unescaped.push(c);
                        }
                        None => {
                            break;
                        }
                    }
                }
                Some((Segment::ObjectKey(unescaped), keypath[..len_bytes].to_string()))
            },
            Some('$') => {
                let mut i = String::new();
                for c in chars {
                    if c >= '0' && c <= '9' {
                        i.push(c);
                    } else {
                        break;
                    }
                }
                Some((Segment::Array(i.parse().unwrap()), keypath[..1+i.len()].to_string()))
            },
            Some(_) => None, // we must be past the keypath portion of string. done. 
            None => None,
        }
    }  

    pub fn push_object_key(&mut self, key: &str) {
        let mut escaped_key = String::with_capacity((key.len() * 2) + 1); // max expansion
        escaped_key.push('.');
        for cc in key.chars() {
            // Escape chars that conflict with delimiters
            if "\\$.!#".contains(cc) {
                escaped_key.push('\\');
            }
            escaped_key.push(cc);
        }
        self.keypath.push(escaped_key);
    }

    pub fn push_array(&mut self) {
        self.keypath.push("$".to_string());
        self.arraypath.push(0);
    }

    pub fn push_array_index(&mut self, index: u64) {
        self.keypath.push("$".to_string());
        self.arraypath.push(index);
    }

    pub fn pop_object_key(&mut self) {
        debug_assert!(self.keypath.last().unwrap().starts_with("."));
        self.keypath.pop();
    }

    pub fn peek_object_key(&self) -> String {
        debug_assert!(self.keypath.last().unwrap().starts_with("."));
        let x = KeyBuilder::parse_first_key_value_segment(&self.keypath.last().unwrap());
        if let Some((Segment::ObjectKey(key), _unescaped)) = x {
            key
        } else {
            panic!("peek_object_key is messed up yo!");
        }
    }

    pub fn peek_array_offset(&self) -> u64 {
        debug_assert!(self.keypath.last().unwrap().starts_with("$"));
        self.arraypath.last().unwrap().clone()
    }

    pub fn pop_array(&mut self) {
        debug_assert!(self.keypath.last().unwrap() == "$");
        self.arraypath.pop();
        self.keypath.pop();
    }

    pub fn inc_top_array_offset(&mut self) {
        if self.keypath.len() > 0 && self.keypath.last().unwrap() == "$" {
            *self.arraypath.last_mut().unwrap() += 1;
        }
    }

    pub fn arraypath_len(&self) -> usize {
        self.arraypath.len()
    }

    pub fn last_pushed_keypath_is_object_key(&self) -> bool {
        self.keypath.last().unwrap().starts_with(".")
    }

    pub fn keypath_segments_len(&self) -> usize {
        self.keypath.len()
    }

    /* splits key into key path, seq and array path
        ex "W.foo$.bar$.baz!word#123,0,0" -> ("W.foo$.bar$.bar!word", "123", "0,0") */
    fn split_keypath_seq_arraypath_from_key(str: &str) -> (&str, &str, &str) {
        let n = str.rfind("#").unwrap();
        assert!(n != 0);
        assert!(n != str.len() - 1);
        let seq_arraypath_str = &str[(n + 1)..];
        let m = seq_arraypath_str.find(",").unwrap();

        (&str[..n], &seq_arraypath_str[..m], &seq_arraypath_str[m + 1..])
    }

    /* parses a seq and array path portion (ex "123,0,0,10) of a key into a doc result */
    pub fn parse_doc_result_from_key(str: &str) -> DocResult {
        let mut dr = DocResult::new();
        let (_path_str, seq_str, arraypath_str) = KeyBuilder::split_keypath_seq_arraypath_from_key(&str);
        dr.seq = seq_str.parse().unwrap(); 
        if !arraypath_str.is_empty() {
            for numstr in arraypath_str.split(",") {
                dr.arraypath.push(numstr.parse().unwrap());
            }
        }
        dr
    }

    pub fn compare_keys(akey: &str, bkey: &str) -> i32 {
        use std::cmp::Ordering;
        assert!(akey.starts_with('W'));
        assert!(bkey.starts_with('W'));
        let (apath_str, aseq_str, aarraypath_str) = KeyBuilder::split_keypath_seq_arraypath_from_key(&akey);
        let (bpath_str, bseq_str, barraypath_str) = KeyBuilder::split_keypath_seq_arraypath_from_key(&bkey);

        match apath_str[1..].cmp(&bpath_str[1..]) {
            Ordering::Less    =>  -1,
            Ordering::Greater =>   1,
            Ordering::Equal   => {
                let aseq: u64 = aseq_str.parse().unwrap();
                let bseq: u64 = bseq_str.parse().unwrap();;
                if aseq < bseq {
                    -1
                } else if aseq > bseq {
                    1
                } else {
                    match aarraypath_str.cmp(barraypath_str) {
                        Ordering::Less    => -1,
                        Ordering::Greater =>  1,
                        Ordering::Equal   =>  0,
                    }
                }
            },
        }
    }
    
}


#[cfg(test)]
mod tests {
    use super::{KeyBuilder};
    use query::DocResult;


    #[test]
    fn test_segments_push() {
        let mut kb = KeyBuilder::new();
        assert_eq!(kb.keypath_segments_len(), 0, "No segments so far");

        kb.push_object_key("first");
        assert_eq!(kb.keypath_segments_len(), 1, "One segment");

        kb.push_object_key("second");
        assert_eq!(kb.keypath_segments_len(), 2, "Two segments");

        kb.push_array();
        assert_eq!(kb.keypath_segments_len(), 3, "Three segments ");
    }

    #[test]
    fn test_segments_pop() {
        let mut kb = KeyBuilder::new();
        kb.push_object_key("first");
        kb.push_object_key("second");
        kb.push_array();

        assert_eq!(kb.keypath_segments_len(), 3, "three segments");
        assert_eq!(kb.stemmed_word_key("astemmedword", 123), "W.first.second$!astemmedword#123,0",
                   "Key for six segments is correct");
        

        kb.pop_array();
        assert_eq!(kb.keypath_segments_len(), 2, "Two segments");

        kb.pop_object_key();
        assert_eq!(kb.keypath_segments_len(), 1, "One segment");

        kb.pop_object_key();
        assert_eq!(kb.keypath_segments_len(), 0, "No segments so far");
    }

    #[test]
    fn test_last_pushed_segment_type() {
        let mut kb = KeyBuilder::new();
        assert_eq!(kb.keypath_segments_len(), 0, "No segments");

        kb.push_object_key("first");
        assert!(kb.last_pushed_keypath_is_object_key(), "Last segment is an object key");

        kb.push_object_key("second");
        assert!(kb.last_pushed_keypath_is_object_key(), "Last segment is an object key");

        kb.push_array();
        assert!(!kb.last_pushed_keypath_is_object_key(), "Last segment is an array");
;
    }

    #[test]
    fn test_doc_result_parse() {
        let key = "W.foo$.bar$!word#123,1,0".to_string();
        let (keypathstr, seqstr, arraypathstr) = KeyBuilder::split_keypath_seq_arraypath_from_key(&key);
        assert_eq!(keypathstr, "W.foo$.bar$!word");
        assert_eq!(seqstr, "123");
        assert_eq!(arraypathstr, "1,0");

        // make sure escaped commas and # in key path don't cause problems
        let key1 = "W.foo\\#$.bar\\,$!word#123,2,0".to_string();
        let (keypathstr1, seqstr1, arraypathstr1) = KeyBuilder::split_keypath_seq_arraypath_from_key(&key1);
        assert_eq!(keypathstr1, "W.foo\\#$.bar\\,$!word");
        assert_eq!(seqstr1, "123");
        assert_eq!(arraypathstr1, "2,0");

        let mut dr = DocResult::new();
        dr.seq = 123;
        dr.arraypath = vec![1,0];
        
        assert!(dr == KeyBuilder::parse_doc_result_from_key(&key));
    }
}
