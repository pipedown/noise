use query::DocResult;
use std::str;

//#[derive(PartialEq, Eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum SegmentType {
    // BuildState is really simple state tracker to prevent misuse of api
    ObjectKey,
    Array,
    Word,
    DocSeq,
    ArrayPath,
}

#[derive(Debug, Clone)]
pub struct Segment {
    type_: SegmentType,
    offset: usize,
}

#[derive(Debug, Clone)]
pub struct KeyBuilder {
    pub array_depth: usize,
    pub segments: Vec<Segment>,
    fullkey: String,
}


impl KeyBuilder {
    pub fn new() -> KeyBuilder {
        let mut kb = KeyBuilder{
            array_depth: 0,
             // Magic reserve numbers that are completely arbitrary
            segments: Vec::with_capacity(10),
            fullkey: String::with_capacity(100),
        };
        // First char is keyspace identifier. W means Word keyspace
        kb.fullkey.push('W');
        return kb;
    }

    // NOTE vmx 2016-10-28: This one is just a port of the C++ prototype, but not yet needed here
    //fn segments_count(&self) -> usize {
    //    self.segments.len()
    //}

    pub fn key(&self) -> String {
        self.fullkey.clone()
    }

    pub fn push_object_key(&mut self, key: String) {
        debug_assert!(self.segments.len() == 0 ||
                      self.segments.last().unwrap().type_ == SegmentType::ObjectKey ||
                      self.segments.last().unwrap().type_ == SegmentType::Array);
        self.segments.push(Segment{ type_: SegmentType::ObjectKey, offset: self.fullkey.len() });
        self.fullkey.push('.');
        for cc in key.chars() {
            // Escape chars that conflict with delimiters
            if "\\$.!#".contains(cc) {
                self.fullkey.push('\\');
            }
            self.fullkey.push(cc);
        }
    }

    pub fn push_array(&mut self) {
        debug_assert!(self.segments.len() == 0 ||
                      self.segments.last().unwrap().type_ == SegmentType::ObjectKey ||
                      self.segments.last().unwrap().type_ == SegmentType::Array);
        self.segments.push(Segment{ type_: SegmentType::Array, offset: self.fullkey.len() });
        self.fullkey.push('$');
        self.array_depth += 1;
    }

    pub fn push_word(&mut self, stemmed_word: &str) {
        debug_assert!(self.segments.len() > 0);
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::ObjectKey ||
                      self.segments.last().unwrap().type_ == SegmentType::Array);
        self.segments.push(Segment{ type_: SegmentType::Word, offset: self.fullkey.len() });
        self.fullkey.push('!');
        self.fullkey += stemmed_word;
    }

    pub fn push_doc_seq(&mut self, seq: u64) {
        debug_assert!(self.segments.len() > 0);
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::Word);
        self.segments.push(Segment{ type_: SegmentType::DocSeq, offset: self.fullkey.len() });
        self.fullkey.push('#');
        self.fullkey.push_str(seq.to_string().as_str());
    }

    pub fn push_array_path(&mut self, path: &Vec<u64>) {
        debug_assert!(self.segments.len() > 0);
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::DocSeq);
        self.segments.push(Segment{ type_: SegmentType::ArrayPath, offset: self.fullkey.len() });
        if path.is_empty() {
            self.fullkey.push(',');
        }
        for i in path {
            self.fullkey.push(',');
            self.fullkey.push_str(i.to_string().as_str());
        }
    }

    pub fn pop_object_key(&mut self) {
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::ObjectKey);
        self.fullkey.truncate(self.segments.last().unwrap().offset);
        self.segments.pop();
    }

    pub fn pop_array(&mut self) {
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::Array);
        self.fullkey.truncate(self.segments.last().unwrap().offset);
        self.array_depth -= 1;
        self.segments.pop();
    }

    pub fn pop_word(&mut self) {
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::Word);
        self.fullkey.truncate(self.segments.last().unwrap().offset);
        self.segments.pop();
    }

    pub fn pop_doc_seq(&mut self) {
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::DocSeq);
        self.fullkey.truncate(self.segments.last().unwrap().offset);
        self.segments.pop();
    }

    pub fn pop_array_path(&mut self) {
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::ArrayPath);
        self.fullkey.truncate(self.segments.last().unwrap().offset);
        self.segments.pop();
    }

    pub fn last_pushed_segment_type(&self) -> Option<SegmentType> {
        self.segments.last().and_then(|segment| Some(segment.type_.clone()))
    }

    /* splits key into key path, seq and array path
        ex "W.foo$.bar$.baz!word#123,0,0" -> ("W.foo$.bar$.bar!word", "123", "0,0") */
    fn split_keypath_seq_arraypath_from_key(str: &str) -> (&str, &str, &str) {
        let n = str.rfind("#").unwrap();
        assert!(n != 0);
        assert!(n != str.len() - 1);
        let seq_array_path_str = &str[(n + 1)..];
        let m = seq_array_path_str.find(",").unwrap();

        (&str[..n], &seq_array_path_str[..m], &seq_array_path_str[m + 1..])
    }

    /* parses a seq and array path portion (ex "123,0,0,10) of a key into a doc result */
    pub fn parse_doc_result_from_key(str: &str) -> DocResult {

        let mut dr = DocResult::new();
        let (_path_str, seq_str, array_path_str) = KeyBuilder::split_keypath_seq_arraypath_from_key(&str);
        dr.seq = seq_str.parse().unwrap();
        if !array_path_str.is_empty() {
            for numstr in array_path_str.split(",") {
                dr.array_path.push(numstr.parse().unwrap());
            }
        }
        dr
    }

    pub fn compare_keys(akey: &str, bkey: &str) -> i32 {
        use std::cmp::Ordering;
        assert!(akey.starts_with('W'));
        assert!(bkey.starts_with('W'));
        let (apath_str, aseq_str, aarray_path_str) = KeyBuilder::split_keypath_seq_arraypath_from_key(&akey);
        let (bpath_str, bseq_str, barray_path_str) = KeyBuilder::split_keypath_seq_arraypath_from_key(&bkey);

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
                    match aarray_path_str.cmp(barray_path_str) {
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
    use super::{KeyBuilder, SegmentType};
    use query::DocResult;

    #[test]
    fn test_new_key_builder() {
        let kb = KeyBuilder::new();
        assert_eq!(kb.key(), "W", "Initial value is set");
    }

    #[test]
    fn test_segments_push() {
        let mut kb = KeyBuilder::new();
        assert_eq!(kb.segments.len(), 0, "No segments so far");
        assert_eq!(kb.key(), "W", "Key for segments is correct");

        kb.push_object_key("first".to_string());
        assert_eq!(kb.segments.len(), 1, "One segment");
        assert_eq!(kb.key(), "W.first", "Key for one segments is correct");

        kb.push_object_key("second".to_string());
        assert_eq!(kb.segments.len(), 2, "Two segments");
        assert_eq!(kb.key(), "W.first.second", "Key for two segments is correct");

        kb.push_array();
        assert_eq!(kb.segments.len(), 3, "Three segments ");
        assert_eq!(kb.key(), "W.first.second$", "Key for three segments is correct");

        kb.push_word("astemmedword");
        assert_eq!(kb.segments.len(), 4, "Four segments");
        assert_eq!(kb.key(), "W.first.second$!astemmedword", "Key for four segments is correct");

        kb.push_doc_seq(123);
        assert_eq!(kb.segments.len(), 5, "Five segments");
        assert_eq!(kb.key(), "W.first.second$!astemmedword#123",
                   "Key for five segments is correct");
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.segments.len() > 0")]
    fn test_segments_push_doc_seq_panic() {
        let mut kb = KeyBuilder::new();
        kb.push_doc_seq(456);
    }

        #[test]
    #[should_panic(expected = "assertion failed: self.segments.len() > 0")]
    fn test_segments_push_word_panic() {
        let mut kb = KeyBuilder::new();
        kb.push_word("astemmedword");
    }

    #[test]
    fn test_segments_pop() {
        let mut kb = KeyBuilder::new();
        kb.push_object_key("first".to_string());
        kb.push_object_key("second".to_string());
        kb.push_array();
        kb.push_word("astemmedword");
        kb.push_doc_seq(123);
        kb.push_array_path(&vec![0]);

        assert_eq!(kb.segments.len(), 6, "six segments");
        assert_eq!(kb.key(), "W.first.second$!astemmedword#123,0",
                   "Key for six segments is correct");
        
        kb.pop_array_path();
        assert_eq!(kb.segments.len(), 5, "Five segments");
        assert_eq!(kb.key(), "W.first.second$!astemmedword#123",
                   "Key for five segments is correct");

        kb.pop_doc_seq();
        assert_eq!(kb.segments.len(), 4, "Four segments");
        assert_eq!(kb.key(), "W.first.second$!astemmedword", "Key for four segments is correct");

        kb.pop_word();
        assert_eq!(kb.segments.len(), 3, "Three segments ");
        assert_eq!(kb.key(), "W.first.second$", "Key for three segments is correct");

        kb.pop_array();
        assert_eq!(kb.segments.len(), 2, "Two segments");
        assert_eq!(kb.key(), "W.first.second", "Key for two segments is correct");

        kb.pop_object_key();
        assert_eq!(kb.segments.len(), 1, "One segment");
        assert_eq!(kb.key(), "W.first", "Key for one segments is correct");

        kb.pop_object_key();
        assert_eq!(kb.segments.len(), 0, "No segments so far");
        assert_eq!(kb.key(), "W", "Key for segments is correct");
    }

    #[test]
    fn test_last_pushed_segment_type() {
        let mut kb = KeyBuilder::new();
        assert_eq!(kb.last_pushed_segment_type(), None, "No segments");

        kb.push_object_key("first".to_string());
        assert_eq!(kb.last_pushed_segment_type(), Some(SegmentType::ObjectKey),
                   "Last segment is an object key");

        kb.push_object_key("second".to_string());
        assert_eq!(kb.last_pushed_segment_type(), Some(SegmentType::ObjectKey),
                   "Last segment is an object key");

        kb.push_array();
        assert_eq!(kb.last_pushed_segment_type(), Some(SegmentType::Array),
                   "Last segment is an array");

        kb.push_word("astemmedword");
        assert_eq!(kb.last_pushed_segment_type(), Some(SegmentType::Word),
                   "Last segment is a word");

        kb.push_doc_seq(123);
        assert_eq!(kb.last_pushed_segment_type(), Some(SegmentType::DocSeq),
                   "Last segment is a doc sequence");
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
        dr.array_path = vec![1,0];
        
        assert!(dr == KeyBuilder::parse_doc_result_from_key(&key));
    }
}
