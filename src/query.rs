
extern crate capnp;

use std::str;
use std::cmp::Ordering;

use error::Error;
use index::Index;
use key_builder::KeyBuilder;
use stems::Stems;
use filters::{QueryRuntimeFilter, ExactMatchFilter, StemmedWordFilter, StemmedWordPosFilter,
              StemmedPhraseFilter, DistanceFilter, AndFilter, OrFilter};


// TODO vmx 2016-11-02: Make it import "rocksdb" properly instead of needing to import the individual tihngs
use rocksdb::{IteratorMode, Snapshot};


#[derive(PartialEq, Eq, PartialOrd, Clone)]
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

impl Ord for DocResult {
    fn cmp(&self, other: &DocResult) -> Ordering {
        match self.seq.cmp(&other.seq) {
            Ordering::Less    =>  Ordering::Less,
            Ordering::Greater =>   Ordering::Greater,
            Ordering::Equal =>  self.arraypath.cmp(&other.arraypath),
        }
    }
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




struct Parser<'a> {
    query: String,
    offset: usize,
    kb: KeyBuilder,
    snapshot: Snapshot<'a>,
}

impl<'a> Parser<'a> {
    fn new(query: String, snapshot: Snapshot<'a>) -> Parser<'a> {
        Parser {
            query: query,
            offset: 0,
            kb: KeyBuilder::new(),
            snapshot: snapshot,
        }
    }

    fn ws(&mut self) {
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
            self.ws();
            true
        } else {
            false
        }
    }


    fn must_consume(&mut self, token: &str) -> Result<(), Error>  {
        if self.could_consume(token) {
            self.offset += token.len();
            self.ws();
            Ok(())
        } else {
            Err(Error::Parse(format!("Expected '{}' at character {}.",
                                                             token, self.offset)))
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
            self.ws();
            Some(result)
        } else {
            None
        }
    }

    fn consume_integer(&mut self) -> Result<Option<i64>, Error> {
        let mut result = String::new();
        for char in self.query[self.offset..].chars() {
            if char >= '0' && char <= '9' {
                result.push(char);
            } else {
                break;
            }
        }
        if !result.is_empty() {
            self.offset += result.len();
            self.ws();
            Ok(Some(try!(result.parse())))
        } else {
            Ok(None)
        }
    }

    fn consume_string_literal(&mut self) -> Result<String, Error> {
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
            try!(self.must_consume("\""));
            Ok(lit)
        } else {
            Err(Error::Parse("Expected \"".to_string()))
        }
    }
/*

find
	= "find" ws object ws

object
	= "{" ws obool ws "}" ws (("&&" / "||")  ws object)?
   / parens

parens
	= "(" ws object ws ")"

obool
   = ws ocompare ws (('&&' / ',' / '||') ws obool)?
   
ocompare
   = oparens
   / key ws ":" ws (oparens / compare)
   
oparens
   = '(' ws obool ws ')' ws
   / array
   / object

compare
   = ("==" / "~=" / "~" digits "=" ) ws string ws

abool
	= ws acompare ws (('&&'/ ',' / '||') ws abool)?
    
acompare
   = aparens
   / compare

aparens
   = '(' ws abool ')' ws
   / array
   / object

array
   = '[' ws abool ']' ws

key
   = field / string

field
	= [a-z_$]i [a-z_$0-9]i*

string
   = '"' ('\\\\' / '\\' [\"tfvrnb] / [^\\\"])* '"' ws

digits
    = [0-9]+

ws
 = [ \t\n\r]*

ws1
 = [ \t\n\r]+
*/


    fn find<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if !self.consume("find") {
            return Err(Error::Parse("Missing 'find' keyword".to_string()));
        }
        self.object()
    }

    fn object<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if self.consume("{") {
            let left = try!(self.obool());
            try!(self.must_consume("}"));
            
            if self.consume("&&") {
                let right = try!(self.object());
                Ok(Box::new(AndFilter::new(vec![left, right], self.kb.arraypath_len())))

            } else if self.consume("||") {
                let right = try!(self.object());
                Ok(Box::new(OrFilter::new(left, right, self.kb.arraypath_len())))
            } else {
                Ok(left)
            }
        } else {
             self.parens()
        }
    }

    fn parens<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        try!(self.must_consume("("));
        let filter = try!(self.object());
        try!(self.must_consume(")"));
        Ok(filter)
    }

    fn obool<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        let mut filter = try!(self.ocompare());
        loop {
            filter = if self.consume("&&") || self.consume(",") {
                let right = try!(self.obool());
                Box::new(AndFilter::new(vec![filter, right], self.kb.arraypath_len()))
            } else if self.consume("||") {
                let right = try!(self.obool());
                Box::new(OrFilter::new(filter, right, self.kb.arraypath_len()))
            } else {
                break;
            }
        }
        Ok(filter)
    }

    fn ocompare<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if let Some(filter) = try!(self.oparens()) {
            Ok(filter)
        } else if let Some(field) = self.consume_field() {
            self.kb.push_object_key(&field);
            try!(self.must_consume(":"));
            if let Some(filter) = try!(self.oparens()) {
                self.kb.pop_object_key();
                Ok(filter)
            } else {
                let filter = try!(self.compare());
                self.kb.pop_object_key();
                Ok(filter)
            }
        } else {
            Err(Error::Parse("Expected object key or '('".to_string()))
        }
    }

    fn oparens<'b>(&'b mut self) -> Result<Option<Box<QueryRuntimeFilter + 'a>>, Error> {
        if self.consume("(") {
            let f = try!(self.obool());
            try!(self.must_consume(")"));
            Ok(Some(f))
        } else if self.could_consume("[") {
            Ok(Some(try!(self.array())))
        } else if self.could_consume("{") {
            Ok(Some(try!(self.object())))
        } else {
            Ok(None)
        }
    }

    fn compare<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if self.consume("==") {
            let literal = try!(self.consume_string_literal());
            let stems = Stems::new(&literal);
            let mut filters: Vec<Box<QueryRuntimeFilter + 'a>> = Vec::new();
            for stem in stems {
                let iter = self.snapshot.iterator(IteratorMode::Start);
                let filter = Box::new(ExactMatchFilter::new(
                    iter, &stem, &self.kb));
                filters.push(filter);
            }
            match filters.len() {
                0 => panic!("Cannot create a ExactMatchFilter"),
                1 => Ok(filters.pop().unwrap()),
                _ => Ok(Box::new(AndFilter::new(filters, self.kb.arraypath_len()))),
            }
        } else if self.consume("~=") {
            // regular search
            let literal = try!(self.consume_string_literal());
            let stems = Stems::new(&literal);
            let stemmed_words: Vec<String> = stems.map(|stem| stem.stemmed).collect();

            match stemmed_words.len() {
                0 => panic!("Cannot create a StemmedWordFilter"),
                1 => {
                    let iter = self.snapshot.iterator(IteratorMode::Start);
                    Ok(Box::new(StemmedWordFilter::new(iter, &stemmed_words[0], &self.kb)))
                },
                _ => {
                    let mut filters: Vec<StemmedWordPosFilter> = Vec::new();
                    for stemmed_word in stemmed_words {
                        let iter = self.snapshot.iterator(IteratorMode::Start);
                        let filter = StemmedWordPosFilter::new(iter, &stemmed_word, &self.kb);
                        filters.push(filter);
                    }
                    Ok(Box::new(StemmedPhraseFilter::new(filters)))
                },
            }
        } else if self.consume("~") {
            let word_distance = match try!(self.consume_integer()) {
                Some(int) => int,
                None => {
                    return Err(Error::Parse("Expected integer for proximity search".to_string()));
                },
            };
            try!(self.must_consume("="));

            let literal = try!(self.consume_string_literal());
            let stems = Stems::new(&literal);
            let mut filters: Vec<StemmedWordPosFilter> = Vec::new();
            for stem in stems {
                let iter = self.snapshot.iterator(IteratorMode::Start);
                let filter = StemmedWordPosFilter::new(
                    iter, &stem.stemmed, &self.kb);
                filters.push(filter);
            }
            match filters.len() {
                0 => panic!("Cannot create a DistanceFilter"),
                _ => Ok(Box::new(DistanceFilter::new(filters, word_distance))),
            }
        } else {
            Err(Error::Parse("Expected comparison operator".to_string()))
        }
    }

    fn abool<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        let mut filter = try!(self.acompare());
        loop {
            filter = if self.consume("&&") || self.consume(",") {
                let right = try!(self.abool());
                Box::new(AndFilter::new(vec![filter, right], self.kb.arraypath_len()))
            } else if self.consume("||") {
                let right = try!(self.abool());
                Box::new(OrFilter::new(filter, right, self.kb.arraypath_len()))
            } else {
                break;
            }
        }
        Ok(filter)
    }

    fn acompare<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if let Some(filter) = try!(self.aparens()) {
            Ok(filter)
        } else {
            self.compare()
        }
    }

    fn aparens<'b>(&'b mut self) -> Result<Option<Box<QueryRuntimeFilter + 'a>>, Error> {
        if self.consume("(") {
            let f = try!(self.abool());
            try!(self.must_consume(")"));
            Ok(Some(f))
        } else if self.could_consume("[") {
            Ok(Some(try!(self.array())))
        } else if self.could_consume("{") {
            Ok(Some(try!(self.object())))
        } else {
            Ok(None)
        }
    }

    fn array<'b>(&'b mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        if !self.consume("[") {
            return Err(Error::Parse("Expected '['".to_string()));
        }
        self.kb.push_array();
        let filter = try!(self.abool());
        self.kb.pop_array();
        try!(self.must_consume("]"));
        Ok(filter)
    }


    fn build_filter(mut self) -> Result<(Box<QueryRuntimeFilter + 'a>, Snapshot<'a>), Error> {
        self.ws();
        Ok((try!(self.find()), self.snapshot))
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
        parser.ws();
        assert_eq!(parser.offset, 5);

        snapshot = Snapshot::new(rocks);
        query = "test".to_string();
        parser = Parser::new(query, snapshot);
        parser.ws();
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
        assert_eq!(parser.consume_string_literal().unwrap(),  " \n \t test".to_string());
    }

    #[test]
    fn test_query_hello_world() {
        let dbname = "target/tests/querytestdbhelloworld";
        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let _ = index.add(r#"{"_id": "foo", "hello": "world"}"#);
        index.flush().unwrap();

        let mut query_results = Query::get_matches(r#"find {hello:=="world"}"#.to_string(), &index).unwrap();
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
        let _ = index.add(r#"{"_id":"6", "A":[{"B":"B3"},{"B": "B3"}]}"#);
        let _ = index.add(r#"{"_id":"7", "A":[{"B":"B3"},{"B": "B4"}]}"#);
        let _ = index.add(r#"{"_id":"8", "A":["A1", "A1"]}"#);
        let _ = index.add(r#"{"_id":"9", "A":["A1", "A2"]}"#);
        let _ = index.add(r#"{"_id":"10", "A":"a bunch of words in this sentence"}"#);
        let _ = index.add(r#"{"_id":"11", "A":""}"#);

        index.flush().unwrap();

        let mut query_results = Query::get_matches(r#"find {A:[{B: =="B2", C: [{D: =="D"} ]}]}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("2".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[{B: == "B2", C: == "C2"}]}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("1".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[{B: == "B2", C: == "C8"}]}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[{B: == "b1", C: == "C2"}]}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("1".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), Some("2".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: == "Multi word sentence"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("3".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: == "%&%}{}@);€"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("4".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: == "{}€52 deeply \\n\\v "}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("5".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[{C: == "C2"}]}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("1".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), Some("2".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[{B: == "B3" || B: == "B4"}]}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("6".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), Some("7".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[ == "A1" || == "A2"]}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("8".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), Some("9".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~= "Multi"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("3".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~= "multi word"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("3".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~= "word sentence"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("3".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~= "sentence word"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~1= "multi sentence"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("3".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~4= "a sentence"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~5= "a sentence"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("10".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~4= "a bunch of words sentence"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), None);

        query_results = Query::get_matches(r#"find {A: ~5= "a bunch of words sentence"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("10".to_string()));
        assert_eq!(query_results.get_next_id().unwrap(), None);


        query_results = Query::get_matches(r#"find {A: == ""}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.get_next_id().unwrap(), Some("11".to_string()));
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

        let mut query_results = Query::get_matches(r#"find {data: == "u"}"#.to_string(), &index).unwrap();
        loop {
            match query_results.get_next_id() {
                Ok(Some(result)) => println!("result: {}", result),
                Ok(None) => break,
                Err(error) => panic!(error),
            }
        }
    }
}
