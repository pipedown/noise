#![allow(dead_code)]
#![allow(unused_variables)]

extern crate capnp;

use std::str;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashSet;

use error::Error;
use index::Index;
use key_builder::KeyBuilder;
use stems::{StemmedWord, Stems};


// TODO vmx 2016-11-02: Make it import "rocksdb" properly instead of needing to import the individual tihngs
use rocksdb::{self, DBIterator, IteratorMode, Snapshot};
use records_capnp::payload;


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
/*
impl Clone for DocResult {
    fn clone(&self) -> DocResult { *self }
}*/

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
    keypathword: String,
    word_pos: u64,
    suffix: String,
    suffix_offset: u64,
}

impl ExactMatchFilter {
    fn new(iter: DBIterator, stemmed_word: &StemmedWord, kb: &KeyBuilder) -> ExactMatchFilter {
        ExactMatchFilter{
            iter: iter,
            keypathword: kb.get_keypathword_only(&stemmed_word.stemmed),
            word_pos: stemmed_word.word_pos as u64,
            suffix: stemmed_word.suffix.clone(),
            suffix_offset: stemmed_word.suffix_offset as u64,
        }
    }
}

impl QueryRuntimeFilter for ExactMatchFilter {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {

        KeyBuilder::add_doc_result_to_keypathword(&mut self.keypathword, &start);
        // Seek in index to >= entry
        self.iter.set_mode(IteratorMode::From(self.keypathword.as_bytes(),
                           rocksdb::Direction::Forward));
        
        KeyBuilder::truncate_to_keypathword(&mut self.keypathword);

        self.next_result()
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        loop {
            if !self.iter.valid() {
                return Ok(None)
            }

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
                if self.word_pos == wi.get_word_pos() &&
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

struct StemmedWordFilter {
    iter: DBIterator,
    keypathword: String,
}

impl StemmedWordFilter {
    fn new(iter: DBIterator, stemmed_word: &str, kb: &KeyBuilder) -> StemmedWordFilter {
        StemmedWordFilter {
            iter: iter,
            keypathword: kb.get_keypathword_only(&stemmed_word),
        }
    }
}

impl QueryRuntimeFilter for StemmedWordFilter {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {

        KeyBuilder::add_doc_result_to_keypathword(&mut self.keypathword, &start);
        // Seek in index to >= entry
        self.iter.set_mode(IteratorMode::From(self.keypathword.as_bytes(),
                           rocksdb::Direction::Forward));
        
        KeyBuilder::truncate_to_keypathword(&mut self.keypathword);

        self.next_result()
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        if !self.iter.valid() {
            return Ok(None)
        }

        let (key, value) = match self.iter.next() {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };
        if !key.starts_with(self.keypathword.as_bytes()) {
            // we passed the key path we are interested in. nothing left to do */
            return Ok(None)
        }

        // We have a candidate document to return
        let key_str = unsafe{str::from_utf8_unchecked(&key)};
        Ok(Some(KeyBuilder::parse_doc_result_from_key(&key_str)))
    }
}

/// This is not a QueryRuntimeFilter but it imitates one. Instead of returning just a DocResult
/// it also return a vector of word positions, each being a instance of the word occurance
struct StemmedWordPosFilter {
    iter: DBIterator,
    keypathword: String,
}

impl StemmedWordPosFilter {
    fn new(iter: DBIterator, stemmed_word: &str, kb: &KeyBuilder) -> StemmedWordPosFilter {
        StemmedWordPosFilter{
            iter: iter,
            keypathword: kb.get_keypathword_only(&stemmed_word),
        }
    }

    fn first_result(&mut self,
                    start: &DocResult) -> Result<Option<(DocResult, Vec<i64>)>, Error> {

        KeyBuilder::add_doc_result_to_keypathword(&mut self.keypathword, &start);
        // Seek in index to >= entry
        self.iter.set_mode(IteratorMode::From(self.keypathword.as_bytes(),
                           rocksdb::Direction::Forward));
        
        KeyBuilder::truncate_to_keypathword(&mut self.keypathword);

        self.next_result()
    }

    fn next_result(&mut self) -> Result<Option<(DocResult, Vec<i64>)>, Error> {
        if !self.iter.valid() {
            return Ok(None)
        }

        let (key, value) = match self.iter.next() {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };
        if !key.starts_with(self.keypathword.as_bytes()) {
            // we passed the key path we are interested in. nothing left to do */
            return Ok(None)
        }
        let mut ref_value = &*value;
        let message_reader = ::capnp::serialize_packed::read_message(
                &mut ref_value, ::capnp::message::ReaderOptions::new()).unwrap();
        let payload = message_reader.get_root::<payload::Reader>().unwrap();

        let positions = try!(payload.get_wordinfos())
                                    .iter()
                                    .map(|wi| wi.get_word_pos()as i64)
                                    .collect();

        let key_str = unsafe{str::from_utf8_unchecked(&key)};
        let docresult = KeyBuilder::parse_doc_result_from_key(&key_str);

        Ok(Some((docresult, positions)))
    }
}

struct StemmedPhraseFilter {
    filters: Vec<StemmedWordPosFilter>,
}

impl StemmedPhraseFilter {
    fn new(filters: Vec<StemmedWordPosFilter>) -> StemmedPhraseFilter {
        StemmedPhraseFilter {
            filters: filters,
        }
    }

    fn result(&mut self,
              base: Option<(DocResult, Vec<i64>)>) -> Result<Option<DocResult>, Error> {
        // this is the number of matches left before all terms match and we can return a result 
        let mut matches_left = self.filters.len() - 1;

        if base.is_none() { return Ok(None); }
        let (mut base_result, mut base_positions) = base.unwrap();

        let mut current_filter = 0;
        loop {
            current_filter += 1;
            if current_filter == self.filters.len() {
                current_filter = 0;
            }

            let next = try!(self.filters[current_filter].first_result(&base_result));
            
            if next.is_none() { return Ok(None); }
            let (next_result, next_positions) = next.unwrap();

            if base_result == next_result {
                let mut new_positions = Vec::new();
                for &pos in next_positions.iter() {
                    if let Ok(_) = base_positions.binary_search(&(pos-1)) {
                        new_positions.push(pos);
                    }
                }
                if new_positions.len() > 0 {
                    // we have valus that survive! reassign back to base_positions
                    base_positions = new_positions;
                    matches_left -= 1;

                    if matches_left == 0 {
                        return Ok(Some(base_result));
                    }
                } else {
                    // we didn't match on phrase, so get next_result from first filter
                    current_filter = 0;
                    let next = try!(self.filters[current_filter].next_result());
                    if next.is_none() { return Ok(None); }
                    let (next_result, next_positions) = next.unwrap();
                    base_result = next_result;
                    base_positions = next_positions;

                    matches_left = self.filters.len() - 1;
                }
            } else {
                // we didn't match on next_result, so get first_result at next_result on
                // 1st filter.
                current_filter = 0;
                let next = try!(self.filters[current_filter].first_result(&next_result));
                if next.is_none() { return Ok(None); }
                let (next_result, next_positions) = next.unwrap();
                base_result = next_result;
                base_positions = next_positions;

                matches_left = self.filters.len() - 1;
            }
        }
    }
}


impl QueryRuntimeFilter for StemmedPhraseFilter {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
        let base_result = try!(self.filters[0].first_result(start));
        self.result(base_result)
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        let base_result = try!(self.filters[0].next_result());
        self.result(base_result)
    }
}

struct DistanceFilter {
    filters: Vec<StemmedWordPosFilter>,
    current_filter: usize,
    distance: i64,
}

impl DistanceFilter {
    fn new(filters: Vec<StemmedWordPosFilter>, distance: i64) -> DistanceFilter {
        DistanceFilter {
            filters: filters,
            current_filter: 0,
            distance: distance,
        }
    }

    fn result(&mut self,
              base: Option<(DocResult, Vec<i64>)>) -> Result<Option<DocResult>, Error> {
        // yes this code complex. I tried to break it up, but it wants to be like this.

        // this is the number of matches left before all terms match and we can return a result 
        let mut matches_left = self.filters.len() - 1;

        if base.is_none() { return Ok(None); }
        let (mut base_result, positions) = base.unwrap();

        // This contains tuples of word postions and the filter they came from,
        // sorted by word position.
        let mut base_positions: Vec<(i64, usize)> = positions.iter()
                                                            .map(|pos|(*pos, self.current_filter))
                                                            .collect();
        
        // distance is number of words between searched words.
        // add one to make calculating difference easier since abs(posa - posb) == distance + 1
        let dis = self.distance + 1;
        loop {
            self.current_filter += 1;
            if self.current_filter == self.filters.len() {
                self.current_filter = 0;
            }

            let next = try!(self.filters[self.current_filter].first_result(&base_result));
            
            if next.is_none() { return Ok(None); }
            let (next_result, next_positions) = next.unwrap();

            if base_result == next_result {
                // so we are in the same field. Now to check the proximity of the values from the
                // next result to previous results.

                // new_positions_map will accept positions within range of pos. But only if all
                // positions that can be are within range. We use the sorted map so we can add
                // the same positions multiple times and it's a noop.
                let mut new_positions_map = BTreeMap::new();
                for &pos in next_positions.iter() {
                    // coud these lines be any longer? No they could not.
                    let start = match base_positions.binary_search_by_key(&(pos-dis),
                                                                          |&(pos2,_)| pos2) {
                        Ok(start) => start,
                        Err(start) => start,
                    };

                    let end = match base_positions.binary_search_by_key(&(pos+dis),
                                                                        |&(pos2,_)| pos2) {
                        Ok(end) => end,
                        Err(end) => end,
                    };

                    // we now collect all the filters within the range
                    let mut filters_encountered = HashSet::new();
                    for &(_, filter_n) in base_positions[start..end].iter() {
                        filters_encountered.insert(filter_n);
                    }
                    
                    if filters_encountered.len() == self.filters.len() - matches_left {
                        // we encountered all the filters we can at this stage, 
                        // so we should add them all to the new_positions_map
                        for &(prev_pos, filter_n) in base_positions[start..end].iter() {
                            new_positions_map.insert(prev_pos, filter_n);
                        }
                        // and add the current pos
                        new_positions_map.insert(pos, self.current_filter);
                    }
                }
                if new_positions_map.len() > 0 {
                    // we have valus that survive! reassign back to positions
                    base_positions = new_positions_map.into_iter().collect();
                    matches_left -= 1;

                    if matches_left == 0 {
                        return Ok(Some(base_result));
                    } else {
                        continue;
                    }
                }
            }
            // we didn't match on next_result, so get next_result on current filter
            let next = try!(self.filters[self.current_filter].next_result());
            
            if next.is_none() { return Ok(None); }
            let (next_result, next_positions) = next.unwrap();
            base_result = next_result;
            base_positions = next_positions.iter()
                                            .map(|pos| (*pos, self.current_filter))
                                            .collect();

            matches_left = self.filters.len() - 1;
        }
    }
}

impl QueryRuntimeFilter for DistanceFilter {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
        let base_result = try!(self.filters[self.current_filter].first_result(start));
        self.result(base_result)
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        let base_result = try!(self.filters[self.current_filter].next_result());
        self.result(base_result)
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

        if base.is_none() { return Ok(None); }
        let mut base_result = base.unwrap();
        
        base_result.arraypath.resize(self.array_depth, 0);

        loop {
            self.current_filter += 1;
            if self.current_filter == self.filters.len() {
                self.current_filter = 0;
            }

            let next = try!(self.filters[self.current_filter].first_result(&base_result));
            
            if next.is_none() { return Ok(None); }
            let mut next_result = next.unwrap();

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

/// Used by OrFilter to maintain a already fetched result so we don't refetch when one side isn't
/// returned to caller. Because we won't know which side gets returned until both sides are
/// fetched.
struct FilterWithResult<'a> {
    filter: Box<QueryRuntimeFilter + 'a>,
    result: Option<DocResult>,
    is_done: bool,
    array_depth: usize,
}

impl<'a> FilterWithResult<'a> {
    fn prime_first_result(&mut self, start: &DocResult) -> Result<(), Error> {
        if self.is_done {
            return Ok(())
        }
        if self.result.is_none() {
            self.result = try!(self.filter.first_result(start));
        } else if self.result.as_ref().unwrap() < start {
            self.result = try!(self.filter.first_result(start));
        }
        if self.result.is_none() {
            self.is_done = true;
        } else {
            self.result.as_mut().unwrap().arraypath.resize(self.array_depth, 0);
        }
        Ok(())
    }

    fn prime_next_result(&mut self) -> Result<(), Error> {
        if self.is_done {
            return Ok(())
        }
        if self.result.is_none() {
            self.result = try!(self.filter.next_result());
        }
        if self.result.is_none() {
            self.is_done = true;
        } else {
            self.result.as_mut().unwrap().arraypath.resize(self.array_depth, 0);
        }
        Ok(())
    }
}

struct OrFilter<'a> {
    left: FilterWithResult<'a>,
    right: FilterWithResult<'a>,
}

impl<'a> OrFilter<'a> {
    fn new(left: Box<QueryRuntimeFilter + 'a>,
           right: Box<QueryRuntimeFilter + 'a>,
           array_depth: usize) -> OrFilter<'a> {
        OrFilter {
            left: FilterWithResult{filter: left,
                                 result: None,
                                 array_depth: array_depth,
                                 is_done: false,
                                 },
            
            right: FilterWithResult{filter: right,
                                 result: None,
                                 array_depth: array_depth,
                                 is_done: false,
                                 }
        }
    }
    fn take_smallest(&mut self) -> Option<DocResult> {
        if let Some(left) = self.left.result.take() {
            // left exists
            if let Some(right) = self.right.result.take() {
                // both exist, return smallest
                match left.cmp(&right) {
                    Ordering::Less    => {
                        // left is smallest, return and put back right
                        self.right.result = Some(right);
                        Some(left)
                    },
                    Ordering::Greater => {
                        // right is smallest, return and put back left
                        self.left.result = Some(left);
                        Some(right)
                    },
                    Ordering::Equal   => {
                        // return one and discard the other so we don't return
                        // identical result in a subsequent call
                        Some(left)
                    },
                }
            } else {
                // right doesn't exist. return left
                Some(left)
            }
        } else {
            // left doesn't exist
            if self.right.result.is_some() {
                // right exists. return it
                self.right.result.take()
            } else {
                // neither exists. return none
                None
            }
        }
    }
}

impl<'a> QueryRuntimeFilter for OrFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
        try!(self.left.prime_first_result(start));
        try!(self.right.prime_first_result(start));
        Ok(self.take_smallest())
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        try!(self.left.prime_next_result());
        try!(self.right.prime_next_result());
        Ok(self.take_smallest())
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

        let (mut x, mut y) = (1, 2);
        x = x + 1;
        y = y + 1;
        let (x, v) = (x+1, y+1);

        assert_eq!(x, 3);

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
