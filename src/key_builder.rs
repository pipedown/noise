//#[derive(PartialEq, Eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum SegmentType {
    // BuildState is really simple state tracker to prevent misuse of api
    ObjectKey,
    Array,
    Word,
    DocSeq,
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
        self.fullkey.push('#');
    }

    pub fn push_doc_seq(&mut self, seq: u64) {
        debug_assert!(self.segments.len() > 0);
        debug_assert!(self.segments.last().unwrap().type_ == SegmentType::Word);
        self.segments.push(Segment{ type_: SegmentType::DocSeq, offset: self.fullkey.len() });
        self.fullkey.push_str(seq.to_string().as_str());
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

    pub fn last_pushed_segment_type(&self) -> Option<SegmentType> {
        self.segments.last().and_then(|segment| Some(segment.type_.clone()))
    }
}


#[cfg(test)]
mod tests {
    use super::{KeyBuilder, SegmentType};

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
        assert_eq!(kb.key(), "W.first.second$!astemmedword#", "Key for four segments is correct");

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
        assert_eq!(kb.segments.len(), 5, "Five segments");
        assert_eq!(kb.key(), "W.first.second$!astemmedword#123",
                   "Key for five segments is correct");

        kb.pop_doc_seq();
        assert_eq!(kb.segments.len(), 4, "Four segments");
        assert_eq!(kb.key(), "W.first.second$!astemmedword#", "Key for four segments is correct");

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
}
