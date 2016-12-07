#![allow(dead_code)]
#![allow(unused_variables)]
extern crate capnp;

use std::str;

use error::Error;
use index::Index;
use key_builder::KeyBuilder;
use stems::{StemmedWord, Stems};

// TODO vmx 2016-11-02: Make it import "rocksdb" properly instead of needing to import the individual tihngs
use rocksdb::{self, DBIterator, IteratorMode, Snapshot};
use records_capnp::payload;



pub struct DocResult {
    pub seq: u64,
    pub arraypath: Vec<u64>,
}

impl DocResult {
    pub fn new() -> DocResult {
        DocResult {
            seq: 0,
            arraypath: Vec::new(),
        }
    }
}


impl PartialEq for DocResult {
    fn eq(&self, other: &DocResult) -> bool {
        self.seq == other.seq && self.arraypath == other.arraypath
    }
}

impl Eq for DocResult {}

pub trait QueryRuntimeFilter {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error>;
    fn next_result(&mut self) -> Result<Option<DocResult>, Error>;
}

pub struct Query {}

pub struct QueryResults<'a> {
    filter: Box<QueryRuntimeFilter + 'a>,
    doc_result_next: DocResult,
    snapshot: Snapshot<'a>,
}

impl<'a> QueryResults<'a> {
    fn new(filter: Box<QueryRuntimeFilter + 'a>, snapshot: Snapshot<'a>) -> QueryResults<'a> {
        QueryResults{
            filter: filter,
            doc_result_next: DocResult::new(),
            snapshot: snapshot,
        }
    }

    fn get_next(&mut self) -> Result<Option<u64>, Error> {
        let result = try!(self.filter.first_result(&self.doc_result_next));
        match result {
            Some(doc_result) => {
                self.doc_result_next.seq = doc_result.seq + 1;
                Ok(Some(doc_result.seq))
            },
            None => Ok(None),
        }
    }

    pub fn get_next_id(&mut self) -> Result<Option<String>, Error> {
        let seq = try!(self.get_next());
        match seq {
            Some(seq) => {
                let key = format!("S{}", seq);
                match try!(self.snapshot.get(&key.as_bytes())) {
                    // If there is an id, it's UTF-8. Strip off keyspace leading byte
                    Some(id) => Ok(Some(id.to_utf8().unwrap()[1..].to_string())),
                    None => Ok(None)
                }
            },
            None => Ok(None),
        }
    }
}


struct ExactMatchFilter {
    iter: DBIterator,
    kb: KeyBuilder,
    keypathword: String,
    stemmed: String,
    stemmed_offset: u64,
    suffix: String,
    suffix_offset: u64,
}

impl ExactMatchFilter {
    fn new(iter: DBIterator, stemmed_word: &StemmedWord, kb: KeyBuilder) -> ExactMatchFilter {
        let keypathword = kb.get_keypathword_only(&stemmed_word.stemmed);
        ExactMatchFilter{
            iter: iter,
            kb: kb,
            keypathword: keypathword,
            stemmed: stemmed_word.stemmed.clone(),
            stemmed_offset: stemmed_word.stemmed_offset as u64,
            suffix: stemmed_word.suffix.clone(),
            suffix_offset: stemmed_word.suffix_offset as u64,
        }
    }
}

impl QueryRuntimeFilter for ExactMatchFilter {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {

        let key = self.kb.stemmed_word_key_from_doc_result(&self.stemmed, &start);
        // Seek in index to >= entry
        self.iter.set_mode(IteratorMode::From(key.as_bytes(),
                           rocksdb::Direction::Forward));

        self.next_result()
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        loop {
            if !self.iter.valid() {
                return Ok(None)
            }

            // New scope needed as the iter.next() below invalidates the
            // current key and value
            {
                let (key, value) = match self.iter.next() {
                    Some((key, value)) => (key, value),
                    None => return Ok(None),
                };
                if !key.starts_with(self.keypathword.as_bytes()) {
                    // we passed the key path we are interested in. nothing left to do */
                    return Ok(None)
                }

                // NOTE vmx 2016-10-13: I'm not really sure why the dereferencing is needed
                // and why we pass on mutable reference of it to `read_message()`
                let mut ref_value = &*value;
                let message_reader = ::capnp::serialize_packed::read_message(
                    &mut ref_value, ::capnp::message::ReaderOptions::new()).unwrap();
                let payload = message_reader.get_root::<payload::Reader>().unwrap();

                for wi in try!(payload.get_wordinfos()).iter() {
                    if self.stemmed_offset == wi.get_stemmed_offset() &&
                        self.suffix_offset == wi.get_suffix_offset() &&
                        self.suffix == try!(wi.get_suffix_text()) {
                            // We have a candidate document to return
                            let key_str = unsafe{str::from_utf8_unchecked(&key)};
                            return Ok(Some(KeyBuilder::parse_doc_result_from_key(&key_str)));
                    }
                }
            }
        }
    }
}



struct DummyFilter {}

impl QueryRuntimeFilter for DummyFilter {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
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
        let mut matches_count = self.filters.len() - 1;
        // TODO vmx 2016-11-04: Make it nicer
        let mut base_result = match base {
            Some(base_result) => base_result,
            None => return Ok(None),
        };
        base_result.arraypath.resize(self.array_depth, 0);

        loop {
            self.current_filter += 1;
            if self.current_filter == self.filters.len() {
                self.current_filter = 0;
            }

            let next = try!(self.filters[self.current_filter].first_result(&base_result));
            let mut next_result = match next {
                Some(next_result) => next_result,
                None => return Ok(None),
            };
            next_result.arraypath.resize(self.array_depth, 0);

            if base_result == next_result {
                matches_count -= 1;
                if matches_count == 0 {
                    return Ok(Some(base_result));
                }
            } else {
                base_result = next_result;
                matches_count = self.filters.len() - 1;
            }
        }
    }
}

impl<'a> QueryRuntimeFilter for AndFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
        let base_result = try!(self.filters[self.current_filter].first_result(start));
        self.result(base_result)
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        let base_result = try!(self.filters[self.current_filter].next_result());
        self.result(base_result)
    }
}



struct Parser<'a> {
    query: String,
    offset: usize,
    kb: KeyBuilder,
    snapshot: Snapshot<'a>,
}

impl<'a> Parser<'a> {
    fn new(query: String, snapshot: Snapshot<'a>) -> Parser<'a> {
        Parser{
            query: query,
            offset: 0,
            kb: KeyBuilder::new(),
            snapshot: snapshot,
        }
    }

    fn whitespace(&mut self) {
        for char in self.query[self.offset..].chars() {
            if !char.is_whitespace() {
                break;
            }
            self.offset += char.len_utf8();
        }
    }

    fn consume(&mut self, token: &str) -> bool {
        if self.could_consume(token) {
            self.offset += token.len();
            self.whitespace();
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
        let mut lit = String::new();
        let mut next_is_special_char = false;
        if self.could_consume("\"") {
            // can't consume("\"") the leading quote because it will also skip leading whitespace
            // inside the string literal
            self.offset += 1;
            for char in self.query[self.offset..].chars() {
                if next_is_special_char {
                    match char {
                        '\\' | '"' => lit.push(char),
                        'n' => lit.push('\n'),
                        'b' => lit.push('\x08'),
                        'r' => lit.push('\r'),
                        'f' => lit.push('\x0C'),
                        't' => lit.push('\t'),
                        'v' => lit.push('\x0B'),
                        _ => return Err(Error::Parse(format!("Unknown character escape: {}",
                                                             char))),
                    };
                    self.offset += 1;
                    next_is_special_char = false;
                } else {
                    if char == '"' {
                        break;
                    } else if char == '\\' {
                        next_is_special_char = true;
                        self.offset += 1;
                    } else {
                        lit.push(char);
                        self.offset += char.len_utf8();
                    }
                }
            }
            if self.consume("\"") {
                Ok(Some(lit))
            } else {
                Err(Error::Parse("Expected \"".to_string()))
            }
        } else {
            Ok(None)
        }
    }

/*
This is a peg grammar that documents the calls of the recursive descent parser
is implemented. Can be checked here: http://pegjs.org/online

bool
    = ws compare ws ('&' ws compare)*
compare
    = field ('.' field)* ws '=' ws string* / factor
factor
    = '(' ws bool ws ')' ws / array
array
    = '[' ws bool ']' ws
field
    = [a-z]i+ ws
string
    = '"' ('\\\\' / '\\' [\"tfvrnb] / [^\\\"])* '"' ws
ws
    = [ /\t/\r\n]* 
*/

    fn bool<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
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
            Ok(Box::new(AndFilter::new(filters, self.kb.arraypath_len())))
        }
    }


    fn array<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if !self.consume("[") {
            return Err(Error::Parse("Expected '['".to_string()));
        }
        self.kb.push_array();
        let filter = try!(self.bool());
        self.kb.pop_array();
        if !self.consume("]") {
            return Err(Error::Parse("Expected ']'".to_string()));
        }
        Ok(filter)
    }

    fn factor<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
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
                    self.kb.push_object_key(&field);
                    let ret = self.compare();
                    self.kb.pop_object_key();
                    ret
                } else if self.consume("=") {
                    match self.consume_string_literal() {
                        Ok(Some(literal)) => {
                            self.kb.push_object_key(&field);

                            let stems = Stems::new(&literal);
                            let mut filters: Vec<Box<QueryRuntimeFilter + 'a>> = Vec::new();
                            for stem in stems {
                                let iter = self.snapshot.iterator(IteratorMode::Start);
                                let filter = Box::new(ExactMatchFilter::new(
                                   iter, &stem, self.kb.clone()));
                                filters.push(filter);
                            }

                            self.kb.pop_object_key();

                            match filters.len() {
                                0 => panic!("Cannot create a ExactMatchFilter"),
                                1 => Ok(filters.pop().unwrap()),
                                _ => Ok(Box::new(AndFilter::new(
                                    filters, self.kb.arraypath_len()))),
                            }
                        },
                        // Empty literal
                        Ok(None) => {Err(Error::Parse("Expected string".to_string()))},
                        Err(error) => {
                            Err(error)
                        }
                    }
                } else if self.could_consume("[") {
                    self.kb.push_object_key(&field);
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

    fn build_filter(mut self) -> Result<(Box<QueryRuntimeFilter + 'a>, Snapshot<'a>), Error> {
        self.whitespace();
        Ok((self.bool().unwrap(), self.snapshot))
    }
}

impl Query {
    pub fn get_matches<'a>(query: String, index: &'a Index) -> Result<QueryResults<'a>, Error> {
        match index.rocks {
            Some(ref rocks) => {
                let snapshot = Snapshot::new(&rocks);
                let parser = Parser::new(query, snapshot);
                let (filter, snapshot2) = try!(parser.build_filter());
                Ok(QueryResults::new(filter, snapshot2))
            },
            None => {
                Err(Error::Parse("You must open the index first".to_string()))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Parser, Query};

    use index::{Index, OpenOptions};

    use rocksdb::Snapshot;

    #[test]
    fn test_whitespace() {
        let mut index = Index::new();
        index.open("target/tests/test_whitespace", Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();
        let mut snapshot = Snapshot::new(rocks);

        let mut query = " \n \t test".to_string();
        let mut parser = Parser::new(query, snapshot);
        parser.whitespace();
        assert_eq!(parser.offset, 5);

        snapshot = Snapshot::new(rocks);
        query = "test".to_string();
        parser = Parser::new(query, snapshot);
        parser.whitespace();
        assert_eq!(parser.offset, 0);
    }

    #[test]
    fn test_consume_string_literal() {
        let mut index = Index::new();
        index.open("target/tests/test_consume_string_literal", Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();
        let snapshot = Snapshot::new(rocks);

        let query = r#"" \n \t test""#.to_string();
        let mut parser = Parser::new(query, snapshot);
        assert_eq!(parser.consume_string_literal().unwrap().unwrap(),  " \n \t test".to_string());
    }

    #[test]
    fn test_query_hello_world() {
        let dbname = "target/tests/querytestdbhelloworld";
        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let _ = index.add(r#"{"_id": "foo", "hello": "world"}"#);
        index.flush().unwrap();

        let mut query_results = Query::get_matches(r#"hello="world""#.to_string(), &index).unwrap();
        //let mut query_results = Query::get_matches(r#"a.b[foo="bar"]"#.to_string(), &index).unwrap();
        println!("query results: {:?}", query_results.get_next_id());
    }

    #[test]
    fn test_query_basic() {
        let dbname = "target/tests/querytestdbbasic";
        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let _ = index.add(r#"{"_id":"1", "A":[{"B":"B2","C":"C2"},{"B": "b1","C":"C2"}]}"#);
        let _ = index.add(r#"{"_id":"2", "A":[{"B":"B2","C":[{"D":"D"}]},{"B": "b1","C":"C2"}]}"#);
        let _ = index.add(r#"{"_id":"3", "A":"Multi word sentence"}"#);
        let _ = index.add(r#"{"_id":"4", "A":"%&%}{}@);€"}"#);
        let _ = index.add(r#"{"_id":"5", "A":"{}€52 deeply \\n\\v "}"#);

        index.flush().unwrap();

        let mut query_results = Query::get_matches(r#"A[B = "B2" & C[ D = "D" ]]"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("2".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"A[B = "B2" & C = "C2"]"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("1".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"A[B = "b1" & C = "C2"]"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("1".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), Some("2".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"A = "Multi word sentence""#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("3".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"A = "%&%}{}@);€""#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("4".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"A = "{}€52 deeply \\n\\v ""#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("5".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"A[C = "C2"]"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("1".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), Some("2".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);
    }

    #[test]
    fn test_query_more_docs() {
        let dbname = "target/tests/querytestdbmoredocs";
        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();

        for ii in 1..100 {
            let data = ((ii % 25) + 97) as u8 as char;
            let _ = index.add(&format!(r#"{{"_id":"{}", "data": "{}"}}"#, ii, data));
        }
        index.flush().unwrap();

        let mut query_results = Query::get_matches(r#"data = "u""#.to_string(), &index).unwrap();
        loop {
            match query_results.get_next_id() {
                Ok(Some(result)) => println!("result: {}", result),
                Ok(None) => break,
                Err(error) => panic!(error),
            }
        }
    }
}
