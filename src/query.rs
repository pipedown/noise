
extern crate capnp;

use std::str;
use std::cmp::Ordering;
use std::io::Write;
use std::collections::HashMap;
use std::iter::Peekable;
use std::mem::transmute;
use std::collections::VecDeque;
use std::iter::Iterator;

use error::Error;
use index::Index;
use key_builder::{KeyBuilder, Segment};
use stems::Stems;
use filters::{QueryRuntimeFilter, ExactMatchFilter, StemmedWordFilter, StemmedWordPosFilter,
              StemmedPhraseFilter, DistanceFilter, AndFilter, OrFilter};


// TODO vmx 2016-11-02: Make it import "rocksdb" properly instead of needing to import the individual tihngs
use rocksdb::{self, DBIterator, IteratorMode, Snapshot};


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
    iter: DBIterator,
    returnable: Box<Returnable>,
    buffer: Vec<u8>,
}

impl<'a> QueryResults<'a> {
    fn new(filter: Box<QueryRuntimeFilter + 'a>,
           snapshot: Snapshot<'a>,
           returnable: Box<Returnable>) -> QueryResults<'a> {
        let iter = snapshot.iterator(IteratorMode::Start);
        QueryResults{
            filter: filter,
            doc_result_next: DocResult::new(),
            snapshot: snapshot,
            iter: iter,
            returnable: returnable,
            buffer: Vec::new(),
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
                let key = format!("V{}#._id", seq);
                match try!(self.snapshot.get(&key.as_bytes())) {
                    // If there is an id, it's UTF-8. Strip off type leading byte
                    Some(id) => Ok(Some(id.to_utf8().unwrap()[1..].to_string())),
                    None => Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    pub fn next_result(&mut self) -> Result<Option<String>, Error> {
        let seq = match try!(self.get_next()) {
            Some(seq) => seq,
            None => return Ok(None),
        };
        let bind = HashMap::new();
        let mut results = VecDeque::new();
        try!(self.returnable.fetch_result(&mut self.iter, seq, &bind, &mut results));
        try!(self.returnable.write_result(&mut results, &mut self.buffer));
        Ok(Some(unsafe{str::from_utf8_unchecked(&self.buffer[..])}.to_string()))
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
            Err(Error::Parse(format!("Expected '{}' at character {}, found {}.",
                                     token, self.offset,
                                     &self.query[self.offset..self.offset+1])))
        }
    }

    fn could_consume(&self, token: &str) -> bool {
        self.query[self.offset..].starts_with(token)
    }

    fn consume_field(&mut self) -> Result<Option<String>, Error> {
        let mut result = String::new();
        {
            let mut chars = self.query[self.offset..].chars();
            if let Some(c) = chars.next() {
                // first char cannot be numeric 
                if c.is_alphabetic() || '_' == c || '$' == c {
                    result.push(c);
                    for c in chars {
                        if c.is_alphanumeric() || '_' == c || '$' == c {
                            result.push(c);
                        } else {
                            break;
                        }
                    }
                }
            }
        } 
        if result.len() > 0 {
            self.offset += result.len();
            self.ws();
            Ok(Some(result))
        } else {
            self.consume_string_literal()
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

    fn consume_keypath(&mut self) -> Result<Option<KeyBuilder>, Error> {
        let key: String = if self.consume(".") {
            if self.consume("[") {
                let key = try!(self.must_consume_string_literal());
                try!(self.must_consume("]"));
                key
            } else {
                 if let Some(key) = try!(self.consume_field()) {
                    key
                } else {
                    self.ws();
                    // this means return the whole document
                    return Ok(Some(KeyBuilder::new()));
                }
            }
        } else {
            return Ok(None);
        };

        let mut kb = KeyBuilder::new();
        kb.push_object_key(&key);
        loop {
            if self.consume("[") {
                if let Some(index) = try!(self.consume_integer()) {
                    kb.push_array_index(index as u64);
                } else {
                    return Err(Error::Parse("Expected array index integer.".to_string()));
                }
                try!(self.must_consume("]"));
            } else if self.consume(".") {
                if let Some(key) = try!(self.consume_field()) {
                    kb.push_object_key(&key);
                } else {
                    return Err(Error::Parse("Expected object key.".to_string()));
                }
            } else {
                break;
            }
        }
        self.ws();
        Ok(Some(kb))
    }


    fn must_consume_string_literal(&mut self) -> Result<String, Error> {
        if let Some(string) = try!(self.consume_string_literal()) {
            Ok(string)
        } else {
            Err(Error::Parse("Expected string literal.".to_string()))
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
            try!(self.must_consume("\""));
            self.ws();
            Ok(Some(lit))
        } else {
            Ok(None)
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
        } else if let Some(field) = try!(self.consume_field()) {
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
            let literal = try!(self.must_consume_string_literal());
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
            let literal = try!(self.must_consume_string_literal());
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

            let literal = try!(self.must_consume_string_literal());
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

    fn return_clause(&mut self) -> Result<Box<Returnable>, Error> {
        if self.consume("return") {
            if let Some(ret_value) = try!(self.ret_value()) {
                Ok(ret_value)
            } else {
                Err(Error::Parse("Expected key, object or array to return.".to_string()))
            }
        } else {
            let mut kb = KeyBuilder::new();
            kb.push_object_key("_id");
            Ok(Box::new(RetValue{kb:kb}))
        }
    }

    fn ret_object(&mut self) -> Result<Box<Returnable>, Error> {
        try!(self.must_consume("{"));
        let mut fields: Vec<(String, Box<Returnable>)> = Vec::new();
        loop {
            if let Some(field) = try!(self.consume_field()) {
                try!(self.must_consume(":"));
                if let Some(ret_value) = try!(self.ret_value()) {
                    fields.push((field, ret_value));
                    if !self.consume(",") {
                        break;
                    }
                } else {
                    return Err(Error::Parse("Expected key to return.".to_string()));
                }
            } else {
                break;
            }
        }
        
        try!(self.must_consume("}"));
        if fields.is_empty() {
            return Err(Error::Parse("Found empty object in return.".to_string()));
        }
        Ok(Box::new(RetObject{fields: fields}))
    }

    fn ret_array(&mut self) -> Result<Box<Returnable>, Error> {
        try!(self.must_consume("["));
        let mut slots = Vec::new();
        loop {
            if let Some(ret_value) = try!(self.ret_value()) {
                slots.push(ret_value);
                if !self.consume(",") {
                    break;
                }
            } else {
                break;
            }
        }
        try!(self.must_consume("]"));
        if slots.is_empty() {
            return Err(Error::Parse("Found empty array in return.".to_string()));
        }
        Ok(Box::new(RetArray{slots: slots}))

    }

    fn ret_value(&mut self) -> Result<Option<Box<Returnable>>, Error> {
        if let Some(kb) = try!(self.consume_keypath()) {
            Ok(Some(Box::new(RetValue{kb: kb})))
        } else if self.could_consume("{") {
            Ok(Some(try!(self.ret_object())))
        } else if self.could_consume("[") {
            Ok(Some(try!(self.ret_array())))
        } else {
            Ok(None)
        }
    }

    fn build_filter(&mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        self.ws();
        Ok(try!(self.find()))
    }
}


pub trait Returnable {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<Option<JsonValue>>) -> Result<(), Error>;

    fn write_result(&self, results: &mut VecDeque<Option<JsonValue>>,
                    write: &mut Write) -> Result<(), Error>;
}

pub enum JsonValue {
    Number(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
    True,
    False,
    Null,
}

impl JsonValue {
    fn str_to_literal(string: &str) ->String {
        let mut ret = String::with_capacity(string.len()*2+2);
        ret.push('"');
        for c in string.chars() {
            if c == '"' || c == '\\' {
                ret.push('\\');
            }
            ret.push(c);
        }
        ret.push('"');
        ret
    }

    fn render(&self, write: &mut Write) -> Result<(), Error> {
        match self {
            &JsonValue::Number(ref num) => try!(write.write_all(num.to_string().as_bytes())), 
            &JsonValue::String(ref string) => {
                try!(write.write_all(JsonValue::str_to_literal(&string).as_bytes()))
            },
            &JsonValue::Array(ref array) => {
                try!(write.write_all("[".as_bytes()));

                let mut iter = array.iter().peekable();
                loop {
                    match iter.next() {
                        Some(json) => try!(json.render(write)),
                        None => break,
                    }
                    if iter.peek().is_some() {
                        try!(write.write_all(",".as_bytes()));
                    }
                }
                try!(write.write_all("]".as_bytes()));
            },
            &JsonValue::Object(ref object) => {
                try!(write.write_all("{".as_bytes()));

                let mut iter = object.iter().peekable();
                loop {
                    match iter.next() {
                        Some(&(ref key, ref json)) => {
                            try!(write.write_all(JsonValue::str_to_literal(&key).as_bytes()));
                            try!(write.write_all(":".as_bytes()));
                            try!(json.render(write));
                        }
                        None => break,
                    }
                    if iter.peek().is_some() {
                        try!(write.write_all(",".as_bytes()));
                    }
                }
                try!(write.write_all("}".as_bytes()));
            },
            &JsonValue::True => try!(write.write_all("true".as_bytes())),
            &JsonValue::False => try!(write.write_all("false".as_bytes())),
            &JsonValue::Null => try!(write.write_all("null".as_bytes())),
        }
        Ok(())
    }
}

pub struct RetObject {
    fields: Vec<(String, Box<Returnable>)>,
}

impl Returnable for RetObject {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<Option<JsonValue>>) -> Result<(), Error> {
        for &(ref _key, ref field) in self.fields.iter() {
            try!(field.fetch_result(iter, seq, bind_var_keys, result));
        }
        Ok(())
    }

    fn write_result(&self, results: &mut VecDeque<Option<JsonValue>>,
                    write: &mut Write) -> Result<(), Error> {
        try!(write.write_all("{".as_bytes()));
        let mut iter = self.fields.iter().peekable();
        loop {
            match iter.next() {
                Some(&(ref key, ref returnable)) => {
                    try!(write.write_all(JsonValue::str_to_literal(key).as_bytes()));
                    try!(write.write_all(":".as_bytes()));
                    try!(returnable.write_result(results, write));
                },
                None => break,
            }
            if iter.peek().is_some() {
                try!(write.write_all(",".as_bytes()));
            }
        }
        try!(write.write_all("}".as_bytes()));
        Ok(())
    }
}


pub struct RetArray {
    slots: Vec<Box<Returnable>>,
}

impl Returnable for RetArray {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<Option<JsonValue>>) -> Result<(), Error> {
        for ref mut slot in self.slots.iter() {
            try!(slot.fetch_result(iter, seq, bind_var_keys, result));
        }
        Ok(())
    }

    fn write_result(&self, results: &mut VecDeque<Option<JsonValue>>,
                    write: &mut Write) -> Result<(), Error> {
        
        try!(write.write_all("[".as_bytes()));
        let mut iter = self.slots.iter().peekable();
        loop {
            match iter.next() {
                Some(ref returnable) => try!(returnable.write_result(results, write)),
                None => break,
            }
            if iter.peek().is_some() {
                try!(write.write_all(",".as_bytes()));
            }
        }
        try!(write.write_all("]".as_bytes()));
        Ok(())
    }
}

pub struct RetValue {
    kb: KeyBuilder,
}

impl RetValue {
    fn bytes_to_json_value(bytes: &[u8]) -> JsonValue {
        match bytes[0] as char {
            's' => {
                let string = unsafe{str::from_utf8_unchecked(&bytes[1..])}.to_string();
                JsonValue::String(string)
            },
            'n' => {
                assert!(bytes.len() == 9);
                let mut bytes2: [u8; 8] = [0; 8];
                for (n, b) in bytes[1..9].iter().enumerate() {
                    bytes2[n] = *b; 
                }
                let double: f64 = unsafe{transmute(bytes2)};
                JsonValue::Number(double)
            },
            'T' => JsonValue::True,
            'F' => JsonValue::False,
            'N' => JsonValue::Null,
            'o' => JsonValue::Object(vec![]),
            'a' => JsonValue::Array(vec![]),
            what => panic!("unexpected type tag in value: {}", what),
        }
    }

    fn return_array(mut array: Vec<(u64, JsonValue)>) -> Result<JsonValue, Error> {
        array.sort_by_key(|tuple| tuple.0);
        Ok(JsonValue::Array(array.into_iter()
                                 .map(|(_i, json)| json)
                                 .collect()))
    }

    fn fetch(iter: &mut Peekable<&mut DBIterator>, value_key: &str,
          mut key: Box<[u8]>, mut value: Box<[u8]>) -> Result<JsonValue, Error> {

        if key.len() == value_key.len() {
            // we have a key match!
            return Ok(RetValue::bytes_to_json_value(value.as_ref()));
        }
        let segment = {
            let key_str = unsafe{str::from_utf8_unchecked(&key)};
            let remaining = &key_str[value_key.len()..];
            KeyBuilder::parse_first_key_value_segment(&remaining)
        };
        
        match segment {
            Some((Segment::ObjectKey(mut unescaped), escaped)) => {
                let mut object: Vec<(String, JsonValue)> = Vec::new();

                let mut value_key_next = value_key.to_string() + &escaped;
                loop {
                    let json_val = try!(RetValue::fetch(iter, &value_key_next, key, value));
                    object.push((unescaped, json_val));
    
                    let segment = match iter.peek() {
                        Some(&(ref k, ref _v)) => {
                            if !k.starts_with(value_key.as_bytes()) {
                                return Ok(JsonValue::Object(object));
                            }

                            let key_str = unsafe{str::from_utf8_unchecked(&k)};
                            let remaining = &key_str[value_key.len()..];

                            KeyBuilder::parse_first_key_value_segment(&remaining)
                        },
                        None => return Ok(JsonValue::Object(object)),
                    };

                    if let Some((Segment::ObjectKey(unescaped2), escaped2)) = segment {
                        unescaped = unescaped2;
                        // advance the peeked iter
                        match iter.next() {
                            Some((k, v)) => {
                                key = k;
                                value = v;
                            }
                            None => panic!("couldn't advanced already peeked iter"),
                        };
                        value_key_next.truncate(value_key.len());
                        value_key_next.push_str(&escaped2);
                    } else {
                        return Ok(JsonValue::Object(object));
                    }
                }
            }
            Some((Segment::Array(mut i), escaped)) => {
                // we use a tuple with ordinal because we encounter
                // elements in lexical sorting order instead of ordinal order
                let mut array: Vec<(u64, JsonValue)> = Vec::new();

                let mut value_key_next = value_key.to_string() + &escaped;
                loop {
                    let json_val = try!(RetValue::fetch(iter, &value_key_next,
                                                        key, value));
                    array.push((i, json_val));
    
                    let segment = match iter.peek() {
                        Some(&(ref k, ref _v)) => {
                            if !k.starts_with(value_key.as_bytes()) {
                                return RetValue::return_array(array);
                            }

                            let key_str = unsafe{str::from_utf8_unchecked(&k)};
                            let remaining = &key_str[value_key.len()..];

                             KeyBuilder::parse_first_key_value_segment(&remaining)
                        },
                        None => return RetValue::return_array(array),
                    };

                    if let Some((Segment::Array(i2), escaped2)) = segment {
                        i = i2;
                        // advance the already peeked iter
                        match iter.next() {
                            Some((k, v)) => {
                                key = k;
                                value = v;
                            },
                            None => panic!("couldn't advanced already peeked iter"),
                        };
                        value_key_next.truncate(value_key.len());
                        value_key_next.push_str(&escaped2);
                    } else {
                        return RetValue::return_array(array);;
                    }
                }
            },
            None => {
                let key_str = unsafe{str::from_utf8_unchecked(&key)};
                panic!("somehow couldn't parse key segment {} {}", value_key, key_str);
            },
        }
    }
}

impl Returnable for RetValue {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<Option<JsonValue>>) -> Result<(), Error> {
        let value_key = if self.kb.keypath_segments_len() == 1 {
            let key = self.kb.peek_object_key();
            if let Some(value_key) = bind_var_keys.get(&key) {
                value_key.to_string()
            } else {
                self.kb.value_key(seq)
            }
        } else {
            self.kb.value_key(seq)
        };

        // Seek in index to >= entry
        iter.set_mode(IteratorMode::From(value_key.as_bytes(),
                                         rocksdb::Direction::Forward));
        
        let (key, value) = match iter.next() {
            Some((key, value)) => (key, value),
            None => {
                result.push_back(None);
                return Ok(())
            },
        };

        if !key.starts_with(value_key.as_bytes()) {
            result.push_back(None);
            return Ok(());
        }

        let json_value = try!(RetValue::fetch(&mut iter.peekable(), &value_key,
                                              key, value));
        result.push_back(Some(json_value));
        Ok(())
    }

    fn write_result(&self, results: &mut VecDeque<Option<JsonValue>>,
                    write: &mut Write) -> Result<(), Error> {
        if let Some(option) = results.pop_front() {
            if let Some(json) = option {
                try!(json.render(write));
            } else {
                // for now just output a Null when we found nothing
                try!(JsonValue::Null.render(write));
            }
        } else {
            panic!("missing result!");
        }
        Ok(())
    }
}



impl Query {
    pub fn get_matches<'a>(query: String, index: &'a Index) -> Result<QueryResults<'a>, Error> {
        match index.rocks {
            Some(ref rocks) => {
                let snapshot = Snapshot::new(&rocks);
                let mut parser = Parser::new(query, snapshot);
                let filter = try!(parser.build_filter());
                let returnable = try!(parser.return_clause());
                Ok(QueryResults::new(filter, parser.snapshot, returnable))
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
    fn test_must_consume_string_literal() {
        let mut index = Index::new();
        index.open("target/tests/test_must_consume_string_literal", Some(OpenOptions::Create)).unwrap();
        let rocks = &index.rocks.unwrap();
        let snapshot = Snapshot::new(rocks);

        let query = r#"" \n \t test""#.to_string();
        let mut parser = Parser::new(query, snapshot);
        assert_eq!(parser.must_consume_string_literal().unwrap(),  " \n \t test".to_string());
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
        let _ = index.add(r#"{"_id":"12", "A":["1","2","3","4","5","6","7","8","9","10","11","12"]}"#);

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

        query_results = Query::get_matches(r#"find {A:[ == "1"]}
                                              return .A "#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),
                Some(r#"["1","2","3","4","5","6","7","8","9","10","11","12"]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[ == "2"]}
                                              return .A[0] "#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#""1""#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[ == "2"]}
                                              return [.A[0], ._id] "#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["1","12"]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[ == "2"]}
                                              return {foo:.A[0], bar: ._id} "#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"foo":"1","bar":"12"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        
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
