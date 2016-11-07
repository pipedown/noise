#![allow(dead_code)]
#![allow(unused_variables)]
extern crate capnp;

use std::{error, fmt, str};

use index::Index;
use key_builder::KeyBuilder;
use stems::{StemmedWord, Stems};

// TODO vmx 2016-11-02: Make it import "rocksdb" properly instead of needing to import the individual tihngs
use rocksdb::{DBIterator, SeekKey};
use rocksdb::rocksdb::Snapshot;
use records_capnp::payload;

#[derive(Debug)]
enum Error {
    Parse(String),
    Capnp(capnp::Error),
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Parse(ref description) => description,
            Error::Capnp(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Parse(_) => None,
            Error::Capnp(ref err) => Some(err as &error::Error),
        }
    }
}

impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Error {
        Error::Capnp(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Parse(ref err) => write!(f, "Parse error: {}", err),
            Error::Capnp(ref err) => write!(f, "Capnproto error: {}", err),
        }
    }
}


struct DocResult {
    seq: u64,
    array_paths: Vec<Vec<u64>>,
}

impl DocResult {
    fn new() -> DocResult {
        DocResult {
            seq: 0,
            array_paths: Vec::new(),
        }
    }

    fn truncate_array_paths(&mut self, array_depth: usize) {
        for array_path in self.array_paths.iter_mut() {
            debug_assert!(array_path.len() >= array_depth);
            array_path.resize(array_depth, 0);
        }
    }

    fn intersect_array_paths(aa: &DocResult, bb: &DocResult) -> Option<DocResult> {
        let mut doc_result = DocResult::new();
        debug_assert_eq!(aa.seq, bb.seq);
        doc_result.seq = aa.seq;
        for array_path_a in &aa.array_paths {
            for array_path_b in &bb.array_paths {
                if array_path_a == array_path_b {
                    doc_result.array_paths.push(array_path_a.clone());
                }
            }
        }
        if doc_result.array_paths.is_empty() {
            None
        } else {
            Some(doc_result)
        }
    }
}

//trait QueryRuntimeFilter {
//struct QueryRuntimeFilter {}

trait QueryRuntimeFilter {
    fn first_result(&mut self, start_id: u64) -> Result<Option<DocResult>, Error>;
    fn next_result(&mut self) -> Result<Option<DocResult>, Error>;
}

pub struct Query {}

pub struct QueryResults {
}

//struct SnapshotIteratorCreator {
//    snapshot: rocksdb::Snapshot,
//}
// 
//impl SnapshotIteratorCreator {
//    fn new(db: &rocksdb::DB) {
//        let snapshot = rocksdb::Snapshot::new(db);
//        SnapshotIteratorCreator{
//            snapshot: snapshot,
//        }
//    }
// 
//    fn new_iterator(&self) {
//        self.snapshot.iter()
//    }
//}



struct ExactMatchFilter<'a> {
    iter: DBIterator<'a>,
    kb: KeyBuilder,
    stemmed_offset: u64,
    suffix: String,
    suffix_offset: u64,
}

impl<'a> ExactMatchFilter<'a> {
    fn new(iter: DBIterator<'a>, stemmed_word: &StemmedWord, mut kb: KeyBuilder) -> ExactMatchFilter<'a> {
        kb.push_word(&stemmed_word.stemmed);
        ExactMatchFilter{
            iter: iter,
            kb: kb,
            stemmed_offset: stemmed_word.stemmed_offset as u64,
            suffix: stemmed_word.suffix.clone(),
            suffix_offset: stemmed_word.suffix_offset as u64,
        }
    }
}

impl<'a> QueryRuntimeFilter for ExactMatchFilter<'a> {
    fn first_result(&mut self, start_id: u64) -> Result<Option<DocResult>, Error> {
        // Build the full key
        self.kb.push_doc_seq(start_id);

        // Seek in index to >= entry
        self.iter.seek(SeekKey::from(self.kb.key().as_bytes()));

        // Revert
        self.kb.pop_doc_seq();

        self.next_result()
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        let mut doc_result = DocResult::new();

        loop {
            if !self.iter.valid() {
                return Ok(None)
            }

            // New scope needed as the iter.next() below invalidates the
            // current key and value
            {
                let key = self.iter.key();
                if !key.starts_with(self.kb.key().as_bytes()) {
                    return Ok(None)
                }
                let seq = &key[self.kb.key().len()..];

                let value = self.iter.value();
                // NOTE vmx 2016-10-13: I'm not really sure why the dereferencing is needed
                // and why we pass on mutable reference of it to `read_message()`
                let mut ref_value = &*value;
                let message_reader = ::capnp::serialize_packed::read_message(
                    &mut ref_value, ::capnp::message::ReaderOptions::new()).unwrap();
                let payload = message_reader.get_root::<payload::Reader>().unwrap();

                for aos_wis in try!(payload.get_arrayoffsets_to_wordinfos()).iter() {
                    for wi in try!(aos_wis.get_wordinfos()).iter() {
                        if self.stemmed_offset == wi.get_stemmed_offset() &&
                            self.suffix_offset == wi.get_suffix_offset() &&
                            self.suffix == try!(wi.get_suffix_text()) {
                                // We have a candidate document to return
                                let arrayoffsets = try!(aos_wis.get_arrayoffsets());
                                doc_result.array_paths.push(arrayoffsets.iter().collect::<>());
                                doc_result.seq = str::from_utf8(&seq).unwrap().parse().unwrap();
                                break;
                            }
                    }
                }
            }
            self.iter.next();

            if doc_result.seq > 0 {
                return Ok(Some(doc_result));
            }
        }
    }
}



struct DummyFilter {}

impl QueryRuntimeFilter for DummyFilter {
    fn first_result(&mut self, start_id: u64) -> Result<Option<DocResult>, Error> {
        Ok(None)
    }
    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        Ok(None)
    }
}


struct AndFilter<'a> {
    filters: Vec<Box<QueryRuntimeFilter + 'a>>,
    current_filter: usize,
    array_depth: usize,
}

impl<'a> AndFilter<'a> {
    fn new(filters: Vec<Box<QueryRuntimeFilter + 'a>>, array_depth: usize) -> AndFilter<'a> {
        AndFilter {
            filters: filters,
            current_filter: 0,
            array_depth: array_depth,
        }
   }

    fn result(&mut self, base: Option<DocResult>) -> Result<Option<DocResult>, Error> {
        let mut matches_count = self.filters.len();
        // TODO vmx 2016-11-04: Make it nicer
        let mut base_result = match base {
            Some(base_result) => base_result,
            None => return Ok(None),
        };
        loop {
            base_result.truncate_array_paths(self.array_depth);

            self.current_filter += 1;
            if self.current_filter > self.filters.len() {
                self.current_filter = 0;
            }

            let next = try!(self.filters[self.current_filter].first_result(base_result.seq));
            let mut next_result = match next {
                Some(next_result) => next_result,
                None => return Ok(None),
            };
            next_result.truncate_array_paths(self.array_depth);

            if base_result.seq == next_result.seq {
                match DocResult::intersect_array_paths(&base_result, &next_result) {
                    Some(new_result) => {
                        base_result = new_result;
                        matches_count -= 1;
                    },
                    None => {
                        let new_result = try!(self.filters[self.current_filter].first_result(base_result.seq));
                        base_result = match new_result {
                            Some(base_result) => base_result,
                            None => return Ok(None),
                        };
                        matches_count = self.filters.len()
                    }
                }
            } else {
                base_result = next_result;
                matches_count = self.filters.len();
            }
        }
    }
}

impl<'a> QueryRuntimeFilter for AndFilter<'a> {
    fn first_result(&mut self, start_id: u64) -> Result<Option<DocResult>, Error> {
        let base_result = try!(self.filters[self.current_filter].first_result(start_id));
        self.result(base_result)
        //Ok(None)
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        //let base_result = try!(self.filters[self.current_filter].next_result());
        //self.result(base_result)
        Ok(None)
    }
}



struct Parser<'a> {
    query: &'a str,
    offset: usize,
    kb: KeyBuilder,
    snapshot: &'a Snapshot<'a>,
}

impl<'a> Parser<'a> {
    fn new(query: &'a str, snapshot: &'a Snapshot<'a>) -> Parser<'a> {
        Parser{
            query: query,
            offset: 0,
            kb: KeyBuilder::new(),
            snapshot: &snapshot,
        }
    }

    fn whitespace(&mut self) {
        loop {
            if let Some(char) = self.query[self.offset..].chars().next() {
                // Stop when the character isn't a whitespace
                if !char.is_whitespace() {
                    break;
                }
                self.offset += char.len_utf8();
            }
        }
    }

    fn consume(&mut self, token: &str) -> bool {
        if self.could_consume(token) {
            self.offset += token.len();
            true
        } else {
            false
        }
    }

    fn could_consume(&mut self, token: &str) -> bool {
        self.query[self.offset..].starts_with(token)
    }

    fn consume_field(&mut self) -> Option<String> {
        let mut result = String::new();
        for char in self.query[self.offset..].chars() {
            if char.is_alphanumeric() {
                result.push(char);
            } else {
                break;
            }
        }
        if result.len() > 0 {
            self.offset += result.len();
            self.whitespace();
            Some(result)
        } else {
            None
        }
    }

    fn consume_string_literal(&mut self) -> Result<Option<String>, Error> {
        // Does not unescape yet
        let mut lit = String::new();
        if self.consume("\"") {
            for char in self.query[self.offset..].chars() {
                if char != '"' {
                    lit.push(char);
                }
                self.offset += char.len_utf8();
            }
            if self.consume("\"") {
                self.whitespace();
                Ok(Some(lit))
            } else {
                Err(Error::Parse("Expected \"".to_string()))
            }
        } else {
            Ok(None)
        }
    }

    fn bool(&mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        let left = try!(self.compare());
        let mut filters = vec![left];
        loop {
            if !self.consume("&") {
                break;
            }

            let right = try!(self.compare());
            filters.push(right);
        }
        if filters.len() == 1 {
            Ok(filters.pop().unwrap())
        } else {
            Ok(Box::new(AndFilter::new(filters, self.kb.array_depth)))
        }
    }


    fn array(&mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if !self.consume("[") {
            return Err(Error::Parse("Expected '['".to_string()));
        }
        self.kb.push_array();
        let filter = try!(self.bool());
        self.kb.pop_array();
        if self.consume("]") {
            return Err(Error::Parse("Expected ']'".to_string()));
        }
        Ok(filter)
    }

    fn factor(&mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if self.consume("(") {
            let filter = try!(self.bool());
            if !self.consume(")") {
                Err(Error::Parse("Expected ')'".to_string()))
            } else {
                Ok(filter)
            }
        } else if self.could_consume("[") {
            self.array()
        } else {
            Err(Error::Parse("Missing Expression".to_string()))
        }
    }

    fn compare<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        match self.consume_field() {
            Some(field) => {
                if self.consume(".") {
                    self.kb.push_object_key(field);
                    let ret = self.compare();
                    self.kb.pop_object_key();
                    ret
                } else if self.consume("=") {
                    match self.consume_string_literal() {
                        Ok(Some(literal)) => {
                            self.kb.push_object_key(field);

                            let stems = Stems::new(&literal);
                            let mut filters: Vec<Box<QueryRuntimeFilter + 'a>> = Vec::new();
                            for stem in stems {
                                let iter = self.snapshot.iter();
                                let filter = Box::new(ExactMatchFilter::new(
                                   iter, &stem, self.kb.clone()));
                                filters.push(filter);
                            }

                            self.kb.pop_object_key();

                            match filters.len() {
                                0 => panic!("Cannot create a ExactMatchFilter"),
                                1 => Ok(filters.pop().unwrap()),
                                _ => Ok(Box::new(AndFilter::<'a>::new(
                                    filters, self.kb.array_depth))),
                            }
                        },
                        // Empty literal
                        Ok(None) => {Err(Error::Parse("Expected string".to_string()))},
                        Err(error) => {
                            Err(error)
                        }
                    }
                } else if self.could_consume("[") {
                    self.kb.push_object_key(field);
                    let ret = self.array();
                    self.kb.pop_object_key();
                    ret
                } else {
                    Err(Error::Parse("Expected comparison or array operator".to_string()))
                }
            },
            None => {
                self.factor()
            }
        }
    }

    fn build_filter(&mut self) -> Result<QueryResults, String> {
        self.whitespace();
        Ok(QueryResults{})
    }
}

impl Query {
    //pub fn get_matches<'a>(query: &str, index: &'a Index) -> Result<QueryResults, String> {
    pub fn get_matches<'a>(query: &str, snapshot: &'a Snapshot) -> Result<QueryResults, String> {
    //        match &index.rocks {
//            &Some(ref rocks) => {
//                let snapshot = Snapshot::new(rocks);
//                let parser = Parser::new(query, &snapshot);
//                Ok(QueryResults{})
//            },
//            &None => {
//                Err("You must open the index first".to_string())
//            },
//        }
        //let rocks = &index.rocks.unwrap();
        // This one would work as well
        //let rocks = index.rocks.as_ref().unwrap();
        //let snapshot = Snapshot::new(rocks);
        let parser = Parser::new(query, &snapshot);
        Ok(QueryResults{})
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;

    use index::{Index, OpenOptions};

    use rocksdb::Snapshot;

    #[test]
    fn test_whitespace() {
        let mut index = Index::new();
        index.open("test_whitespace", Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();
        let snapshot = Snapshot::new(rocks);

        let mut query = " \n \t test";
        let mut parser = Parser::new(query, &snapshot);
        parser.whitespace();
        assert_eq!(parser.offset, 5);

        query = "test";
        parser = Parser::new(query, &snapshot);
        parser.whitespace();
        assert_eq!(parser.offset, 0);
    }
}
