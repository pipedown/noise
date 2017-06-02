extern crate unicode_normalization;

use query::DocResult;
use std::str;
use std::cmp::Ordering;


/// for looking up words in fields.
pub const KEY_PREFIX_WORD: char = 'W';
/// for getting the total count of a word in the index. Used for relevancy scoring.
pub const KEY_PREFIX_WORD_COUNT: char = 'C';
/// for getting the total number of field instances in the index. Used for relevancy scoring.
pub const KEY_PREFIX_FIELD_COUNT: char = 'k';
/// for specific field length. Used for relevancy scoring
pub const KEY_PREFIX_FIELD_LENGTH: char = 'L';
/// for getting the doc seq from it's id.
pub const KEY_PREFIX_ID_TO_SEQ: char = 'I';
/// for getting/scanning the all the seqs
pub const KEY_PREFIX_SEQ: char = 'S';
/// number values index
pub const KEY_PREFIX_NUMBER: char = 'f';
/// for true values index
pub const KEY_PREFIX_TRUE: char = 'T';
/// for false value index
pub const KEY_PREFIX_FALSE: char = 'F';
/// for null values index
pub const KEY_PREFIX_NULL: char = 'N';
/// for orignal doc values for retrieving results
pub const KEY_PREFIX_VALUE: char = 'V';


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
        KeyBuilder {
            // Magic reserve number is completely arbitrary
            keypath: Vec::with_capacity(10),
            arraypath: Vec::with_capacity(10),
        }
    }

    pub fn clear(&mut self) {
        self.keypath.clear();
        self.arraypath.clear();
    }

    /// Builds a stemmed word key for the input word and seq, using the key_path and arraypath
    /// built up internally.
    pub fn kp_word_key(&self, word: &str, seq: u64) -> String {
        let mut string = self.get_kp_word_only(&word);
        string.push_str(seq.to_string().as_str());

        KeyBuilder::add_arraypath(&mut string, &self.arraypath);
        string
    }

    pub fn get_kp_word_only(&self, word: &str) -> String {
        let mut string = String::with_capacity(100);
        string.push(KEY_PREFIX_WORD);
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('!');
        string.push_str(word);
        string.push('#');
        string
    }

    pub fn kp_word_count_key(&self, word: &str) -> String {
        let mut string = String::with_capacity(100);
        string.push(KEY_PREFIX_WORD_COUNT);
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('!');
        string.push_str(word);
        string
    }

    pub fn kp_field_count_key(&self) -> String {
        let mut string = String::with_capacity(100);
        string.push(KEY_PREFIX_FIELD_COUNT);
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string
    }

    pub fn id_to_seq_key(id: &str) -> String {
        let mut str = String::with_capacity(id.len() + 1);
        str.push(KEY_PREFIX_ID_TO_SEQ);
        str.push_str(&id);
        str
    }

    pub fn seq_key(seq: u64) -> String {
        let seq = seq.to_string();
        let mut str = String::with_capacity(seq.len() + 1);
        str.push(KEY_PREFIX_SEQ);
        str.push_str(&seq);
        str
    }

    pub fn parse_seq_key(key: &str) -> Option<u64> {
        if key.starts_with("S") {
            Some(key[1..].parse().unwrap())
        } else {
            None
        }
    }

    /// Build the index key that corresponds to a number primitive
    pub fn number_key(&self, seq: u64) -> String {
        let mut string = String::with_capacity(100);
        string.push(KEY_PREFIX_NUMBER);
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('#');
        string.push_str(&seq.to_string());

        KeyBuilder::add_arraypath(&mut string, &self.arraypath);
        string
    }

    /// Build the index key that corresponds to a true, false or null primitive
    pub fn bool_null_key(&self, prefix: char, seq: u64) -> String {
        let mut string = String::with_capacity(100);
        string.push(prefix);
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('#');
        string.push_str(&seq.to_string());

        KeyBuilder::add_arraypath(&mut string, &self.arraypath);
        string
    }

    /// Builds a field length key for the seq, using the key_path and arraypath
    /// built up internally.
    pub fn kp_field_length_key(&self, seq: u64) -> String {
        let mut string = String::with_capacity(100);
        string.push(KEY_PREFIX_FIELD_LENGTH);
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('#');
        string.push_str(seq.to_string().as_str());

        KeyBuilder::add_arraypath(&mut string, &self.arraypath);
        string
    }

    /// Builds a field length key for the DocResult, using the key_path
    /// built up internally and the arraypath from the DocResult.
    pub fn kp_field_length_key_from_doc_result(&self, dr: &DocResult) -> String {
        let mut string = String::with_capacity(100);
        string.push(KEY_PREFIX_FIELD_LENGTH);
        for segment in &self.keypath {
            string.push_str(&segment);
        }
        string.push('#');
        string.push_str(dr.seq.to_string().as_str());

        KeyBuilder::add_arraypath(&mut string, &dr.arraypath);
        string
    }

    /// Adds DocResult seq and array path an already created kp_word.
    pub fn add_doc_result_to_kp_word(keypathword: &mut String, dr: &DocResult) {
        keypathword.push_str(dr.seq.to_string().as_str());
        KeyBuilder::add_arraypath(keypathword, &dr.arraypath);
    }

    /// Truncates key to keypath only
    pub fn truncate_to_kp_word(stemmed_word_key: &mut String) {
        let n = stemmed_word_key.rfind("#").unwrap();
        stemmed_word_key.truncate(n + 1);
    }


    /// Builds a value key for seq (value keys are the original json terminal value with
    /// keyed on keypath and arraypath built up internally).
    pub fn kp_value_key(&self, seq: u64) -> String {
        let mut string = String::with_capacity(100);
        string.push(KEY_PREFIX_VALUE);
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

    /// Returns a value key without the doc seq prepended.
    pub fn kp_value_no_seq(&self) -> String {
        let mut string = String::with_capacity(100);
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

    /// Returns a value key without the doc seq prepended.
    pub fn kp_value_no_seq_from_str(str: &str) -> &str {
        &str[str.find('#').unwrap() + 1..]
    }

    /// parses a kp_value_key and sets the internally elements appropriately
    pub fn parse_kp_value_no_seq(&mut self, mut str: &str) {
        while let Some(tuple) = KeyBuilder::parse_first_kp_value_segment(str) {
            match tuple {
                (Segment::ObjectKey(_key), unescaped) => {
                    str = &str[unescaped.len()..];
                    self.keypath.push(unescaped);
                }
                (Segment::Array(i), unescaped) => {
                    str = &str[unescaped.len()..];
                    self.keypath.push("$".to_string());
                    self.arraypath.push(i);
                }
            }
        }
    }

    /// Build a key to a value from a DocResult
    pub fn kp_value_key_from_doc_result(&self, dr: &DocResult) -> String {
        let mut string = String::with_capacity(100);
        string.push(KEY_PREFIX_VALUE);
        string.push_str(&dr.seq.to_string());
        string.push('#');
        let mut i = 0;
        for segment in &self.keypath {
            string.push_str(&segment);
            if segment == "$" {
                string.push_str(&dr.arraypath[i].to_string());
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

    // Returns true if the prefix str is a prefix of the true keypath
    pub fn is_kp_value_key_prefix(prefix: &str, keypath: &str) -> bool {
        if keypath.starts_with(prefix) {
            match keypath[prefix.len()..].chars().next() {
                Some('.') => true,
                Some('$') => true,
                Some(_) => false,
                None => true,
            }
        } else {
            false
        }
    }

    // returns the unescaped segment as Segment and the escaped segment as a slice
    pub fn parse_first_kp_value_segment(keypath: &str) -> Option<(Segment, String)> {

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
                        }
                        Some('.') | Some('$') => {
                            break;
                        }
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
            }
            Some('$') => {
                let mut i = String::new();
                for c in chars {
                    if c >= '0' && c <= '9' {
                        i.push(c);
                    } else {
                        break;
                    }
                }
                Some((Segment::Array(i.parse().unwrap()), keypath[..1 + i.len()].to_string()))
            }
            Some(_) => None, // we must be past the keypath portion of string. done.
            None => None,
        }
    }

    /// Adds objet key to keypath
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

    /// adds array to keypath
    pub fn push_array(&mut self) {
        self.keypath.push("$".to_string());
        self.arraypath.push(0);
    }

    /// adds array with index to keypath
    pub fn push_array_index(&mut self, index: u64) {
        self.keypath.push("$".to_string());
        self.arraypath.push(index);
    }

    /// pops last object key `{"foo":..."}` from keypath. Last segment must be object key
    pub fn pop_object_key(&mut self) {
        debug_assert!(self.keypath.last().unwrap().starts_with("."));
        self.keypath.pop();
    }

    /// Returns the last array offset in the keypath. Last segment must be an array.
    pub fn peek_array_index(&self) -> u64 {
        debug_assert!(self.keypath.last().unwrap().starts_with("$"));
        self.arraypath.last().unwrap().clone()
    }

    /// pops last array segment `[N]` from keypath. Last segment must be array.
    pub fn pop_array(&mut self) {
        debug_assert!(self.keypath.last().unwrap() == "$");
        self.arraypath.pop();
        self.keypath.pop();
    }

    /// increments the last array segment by 1. LAst segment must be array,
    pub fn inc_top_array_index(&mut self) {
        if self.keypath.len() > 0 && self.keypath.last().unwrap() == "$" {
            *self.arraypath.last_mut().unwrap() += 1;
        }
    }

    /// Returns the number of arrays in the keypath
    pub fn arraypath_len(&self) -> usize {
        self.arraypath.len()
    }

    /// returns the number of segments in the keypath
    pub fn kp_segments_len(&self) -> usize {
        self.keypath.len()
    }

    /// splits key into key path, seq and array path
    /// ex "W.foo$.bar$.baz!word#123,0,0" -> ("W.foo$.bar$.bar!word", "123", "0,0")
    fn split_seq_arraypath_from_kp_word_key(str: &str) -> (&str, &str, &str) {
        let n = str.rfind("#").unwrap();
        assert!(n != 0);
        assert!(n != str.len() - 1);
        let seq_arraypath_str = &str[(n + 1)..];
        let m = seq_arraypath_str.find(",").unwrap();

        (&str[..n], &seq_arraypath_str[..m], &seq_arraypath_str[m + 1..])
    }

    /// parses a seq and array path portion (ex "123,0,0,10) of a key into a doc result
    pub fn parse_doc_result_from_kp_word_key(str: &str) -> DocResult {
        let mut dr = DocResult::new();
        let (_path_str, seq_str, arraypath_str) =
            KeyBuilder::split_seq_arraypath_from_kp_word_key(&str);
        dr.seq = seq_str.parse().unwrap();
        if !arraypath_str.is_empty() {
            for numstr in arraypath_str.split(",") {
                dr.arraypath.push(numstr.parse().unwrap());
            }
        }
        dr
    }

    /// used to collate relevant keys that need specific sorting.
    pub fn compare_keys(akey: &str, bkey: &str) -> Ordering {
        debug_assert!(akey.starts_with(KEY_PREFIX_WORD) || akey.starts_with(KEY_PREFIX_NUMBER) ||
                      akey.starts_with(KEY_PREFIX_TRUE) ||
                      akey.starts_with(KEY_PREFIX_FALSE) ||
                      akey.starts_with(KEY_PREFIX_NULL));
        debug_assert!(bkey.starts_with(KEY_PREFIX_WORD) || bkey.starts_with(KEY_PREFIX_NUMBER) ||
                      bkey.starts_with(KEY_PREFIX_TRUE) ||
                      bkey.starts_with(KEY_PREFIX_FALSE) ||
                      bkey.starts_with(KEY_PREFIX_NULL));
        let (apath_str, aseq_str, aarraypath_str) =
            KeyBuilder::split_seq_arraypath_from_kp_word_key(&akey);
        let (bpath_str, bseq_str, barraypath_str) =
            KeyBuilder::split_seq_arraypath_from_kp_word_key(&bkey);

        match apath_str[0..].cmp(&bpath_str[0..]) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                let aseq: u64 = aseq_str.parse().unwrap();
                let bseq: u64 = bseq_str.parse().unwrap();
                if aseq < bseq {
                    Ordering::Less
                } else if aseq > bseq {
                    Ordering::Greater
                } else {
                    if aarraypath_str.is_empty() || barraypath_str.is_empty() {
                        aarraypath_str.len().cmp(&barraypath_str.len())
                    } else {
                        let mut a_nums = aarraypath_str.split(",");
                        let mut b_nums = barraypath_str.split(",");
                        loop {
                            if let Some(ref a_num_str) = a_nums.next() {
                                if let Some(ref b_num_str) = b_nums.next() {
                                    let a_num: u64 = a_num_str.parse().unwrap();
                                    let b_num: u64 = b_num_str.parse().unwrap();
                                    match a_num.cmp(&b_num) {
                                        Ordering::Less => return Ordering::Less,
                                        Ordering::Greater => return Ordering::Greater,
                                        Ordering::Equal => (),
                                    }
                                } else {
                                    //b is shorter than a, so greater
                                    return Ordering::Greater;
                                }
                            } else {
                                if b_nums.next().is_some() {
                                    //a is shorter than b so less
                                    return Ordering::Less;
                                } else {
                                    // same length and must have hit all equal before this,
                                    // so equal
                                    return Ordering::Equal;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::KeyBuilder;
    use query::DocResult;


    #[test]
    fn test_segments_push() {
        let mut kb = KeyBuilder::new();
        assert_eq!(kb.kp_segments_len(), 0, "No segments so far");

        kb.push_object_key("first");
        assert_eq!(kb.kp_segments_len(), 1, "One segment");

        kb.push_object_key("second");
        assert_eq!(kb.kp_segments_len(), 2, "Two segments");

        kb.push_array();
        assert_eq!(kb.kp_segments_len(), 3, "Three segments ");
    }

    #[test]
    fn test_segments_pop() {
        let mut kb = KeyBuilder::new();
        kb.push_object_key("first");
        kb.push_object_key("second");
        kb.push_array();

        assert_eq!(kb.kp_segments_len(), 3, "three segments");
        assert_eq!(kb.kp_word_key("astemmedword", 123),
                   "W.first.second$!astemmedword#123,0",
                   "Key for six segments is correct");


        kb.pop_array();
        assert_eq!(kb.kp_segments_len(), 2, "Two segments");

        kb.pop_object_key();
        assert_eq!(kb.kp_segments_len(), 1, "One segment");

        kb.pop_object_key();
        assert_eq!(kb.kp_segments_len(), 0, "No segments so far");
    }

    #[test]
    fn test_doc_result_parse() {
        let key = "W.foo$.bar$!word#123,1,0".to_string();
        let (keypathstr, seqstr, arraypathstr) =
            KeyBuilder::split_seq_arraypath_from_kp_word_key(&key);
        assert_eq!(keypathstr, "W.foo$.bar$!word");
        assert_eq!(seqstr, "123");
        assert_eq!(arraypathstr, "1,0");

        // make sure escaped commas and # in key path don't cause problems
        let key1 = "W.foo\\#$.bar\\,$!word#123,2,0".to_string();
        let (keypathstr1, seqstr1, arraypathstr1) =
            KeyBuilder::split_seq_arraypath_from_kp_word_key(&key1);
        assert_eq!(keypathstr1, "W.foo\\#$.bar\\,$!word");
        assert_eq!(seqstr1, "123");
        assert_eq!(arraypathstr1, "2,0");

        let mut dr = DocResult::new();
        dr.seq = 123;
        dr.arraypath = vec![1, 0];

        assert!(dr == KeyBuilder::parse_doc_result_from_kp_word_key(&key));
    }
}
