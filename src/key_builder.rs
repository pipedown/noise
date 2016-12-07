use query::DocResult;
use std::str;


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

    /// Builds a stemmed word key for the input word and seq, using the key_path and arraypath
    /// built up internally.
    pub fn stemmed_word_key(&self, word: &str, seq: u64) -> String {
        self.stemmed_word_key_internal(word, seq, &self.arraypath)
    }

    /// Builds a stemmed word key for the input word and doc result, using the key_path built up
    /// internally but ignoring the internal array path. Instead uses the array path from the
    /// DocResult 
    pub fn stemmed_word_key_from_doc_result(&self, word: &str, dr: &DocResult) -> String {
        self.stemmed_word_key_internal(word, dr.seq, &dr.arraypath)
    }

    fn stemmed_word_key_internal(&self, word: &str, seq: u64, arraypath: &Vec<u64>) -> String {
        let mut string = String::with_capacity(100);
        string.push('W');
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('!');
        string.push_str(word);
        string.push('#');
        string.push_str(seq.to_string().as_str());

        self.add_arraypath(&mut string, &arraypath);
        string
    }

    /// Builds a value key for seq (value keys are the original json terminal value with
    /// keyed on keypath and arraypath built up internally).
    pub fn value_key(&self, seq: u64) -> String {
        let mut string = String::with_capacity(100);
        string.push('V');
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('#');
        string.push_str(&seq.to_string());

        self.add_arraypath(&mut string, &self.arraypath);
        string
    }

    fn add_arraypath(&self, string: &mut String, arraypath: &Vec<u64>) {
        if arraypath.is_empty() {
            string.push(',');
        } else {
            for i in arraypath {
                string.push(',');
                string.push_str(i.to_string().as_str());
            }
        }

    }

    pub fn push_object_key(&mut self, key: &str) {
        let mut escaped_key = String::with_capacity((key.len() * 2) + 1); // max expansion
        escaped_key.push('.');
        for cc in key.chars() {
            // Escape chars that conflict with delimiters
            if "\\$.!#,".contains(cc) {
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

    pub fn pop_object_key(&mut self) {
        debug_assert!(self.keypath.last().unwrap().starts_with("."));
        self.keypath.pop();
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

    pub fn get_keypathword_only(&self, stemmed: &str) -> String {
        let mut key = self.stemmed_word_key(stemmed, 0);
        let n = key.rfind("#").unwrap();
        key.truncate(n + 1);
        key
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
