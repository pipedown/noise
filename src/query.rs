
extern crate capnp;

use std::str;
use std::cmp::Ordering;
use std::io::Write;
use std::collections::HashMap;
use std::iter::Peekable;
use std::mem::{transmute, swap};
use std::collections::VecDeque;
use std::iter::Iterator;
use std::usize;

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

impl Query {
    pub fn get_matches<'a>(query: String, index: &'a Index) -> Result<QueryResults<'a>, Error> {
        match index.rocks {
            Some(ref rocks) => {
                let snapshot = Snapshot::new(&rocks);
                let mut parser = Parser::new(query, snapshot);
                let filter = try!(parser.build_filter());
                let mut sorts = try!(parser.sort_clause());
                let mut returnable = try!(parser.return_clause());
                let limit = try!(parser.limit_clause());
                
                let mut ags = Vec::new(); 
                returnable.get_aggregate_funs(&mut ags);

                let mut has_ags = false;
                for option_ag in ags.iter() {
                    if option_ag.is_some() {
                        has_ags = true;
                        break;
                    }
                }
                let has_sorting = !sorts.is_empty();

                returnable = if has_sorting && has_ags {
                    return Err(Error::Parse("Cannot have aggregates and sorting in the same query"
                                            .to_string()));
                } else if has_sorting {
                    returnable.take_sort_for_matching_fields(&mut sorts);
                    if !sorts.is_empty() {
                        let vec = sorts.into_iter()
                                     .map(|(_key, sort_info)|
                                          RetValue {kb: sort_info.kb, 
                                                    ag: None,
                                                    default: sort_info.default,
                                                    sort: Some(sort_info.sort)})
                                    .collect();
                        Box::new(RetHidden{unrendered: vec, visible: returnable})
                    } else {
                        returnable
                    }
                } else {
                    returnable
                };

                let option_ags = if has_ags {
                    // we have at least one AggregationFun. Make sure they are all set.
                    for option_ag in ags.iter() {
                        if option_ag.is_none() {
                            return Err(Error::Parse("Return keypaths must either all have \
                                aggregate functions, or none can them.".to_string()));
                        }
                    }
                    Some(ags.into_iter().map(|option| option.unwrap()).collect())
                } else {
                    None
                };

                let sorting = if has_sorting {
                    let mut sorting = Vec::new();
                    returnable.get_sorting(&mut sorting);
                    Some(sorting)
                } else {
                    None
                };
                
                Ok(QueryResults::new(filter, parser.snapshot, returnable,
                                     option_ags, sorting, limit))
            },
            None => {
                Err(Error::Parse("You must open the index first".to_string()))
            },
        }
    }
}

pub struct QueryResults<'a> {
    filter: Box<QueryRuntimeFilter + 'a>,
    doc_result_next: DocResult,
    snapshot: Snapshot<'a>,
    iter: DBIterator,
    returnable: Box<Returnable>,
    buffer: Vec<u8>,
    needs_sorting_and_ags: bool,
    done_with_sorting_and_ags: bool,
    does_group_or_aggr: bool,
    sorts: Option<Vec<(Sort, usize)>>,
    aggr_inits: Vec<(fn (&mut JsonValue), usize)>,
    aggr_actions: Vec<(fn (&mut JsonValue, JsonValue, &JsonValue), JsonValue, usize)>,
    aggr_finals: Vec<(fn (&mut JsonValue), usize)>,
    in_buffer: Vec<VecDeque<JsonValue>>,
    sorted_buffer: Vec<VecDeque<JsonValue>>,
    limit: usize,
}

impl<'a> QueryResults<'a> {
    fn new(filter: Box<QueryRuntimeFilter + 'a>,
           snapshot: Snapshot<'a>,
           returnable: Box<Returnable>,
           ags: Option<Vec<(AggregateFun, JsonValue)>>,
           sorting: Option<Vec<Option<Sort>>>,
           limit: usize) -> QueryResults<'a> {
        
        // the input args for sorts and ags are vecs where the slot is the same slot as
        // a result that the action needs to be applied to. We instead convert them
        // into several new fields with tuples of action and the slot to act on.
        // this way we don't needlesss loop over the actions where most are noops

        // only one can be Some at a time
        debug_assert!(!sorting.is_some() && !ags.is_some() || sorting.is_some() ^ ags.is_some());

        let needs_sorting_and_ags = ags.is_some() || sorting.is_some();
        
        let mut sorts = Vec::new();
        if let Some(mut sorting) = sorting {
            let mut n = sorting.len();
            while let Some(option) = sorting.pop() {
                n -= 1;
                if let Some(sort_dir) = option {
                    sorts.push((sort_dir, n));
                }
            }
            // order we process sorts is important
            sorts.reverse();
        }
        let mut does_group_or_aggr = false;
        let mut aggr_inits = Vec::new();
        let mut aggr_actions = Vec::new();
        let mut aggr_finals = Vec::new();
        if let Some(mut ags) = ags {
            does_group_or_aggr = true;
            let mut n = ags.len();
            while let Some((ag, user_arg)) = ags.pop() {
                n -= 1;
                if ag == AggregateFun::GroupAsc {
                    sorts.push((Sort::Asc, n));
                } else if  ag == AggregateFun::GroupDesc {
                    sorts.push((Sort::Desc, n));
                } else {
                    let ag_impls = ag.get_fun_impls();
                    if let Some(init) = ag_impls.init {
                        aggr_inits.push((init, n));
                    }
                    if let Some(extract) = ag_impls.extract {
                        aggr_finals.push((extract, n));
                    }
                    aggr_actions.push((ag_impls.action, user_arg, n));
                }
            }
            // the order we process groups in important
            sorts.reverse();
        }
        
        QueryResults{
            filter: filter,
            doc_result_next: DocResult::new(),
            iter: snapshot.iterator(IteratorMode::Start),
            snapshot: snapshot,
            returnable: returnable,
            buffer: Vec::new(),
            needs_sorting_and_ags: needs_sorting_and_ags,
            done_with_sorting_and_ags: false,
            does_group_or_aggr: does_group_or_aggr,
            sorts: Some(sorts),
            aggr_inits: aggr_inits,
            aggr_actions: aggr_actions,
            aggr_finals: aggr_finals,
            in_buffer: Vec::new(),
            sorted_buffer: Vec::new(),
            limit: limit,
        }
    }

    fn get_next(&mut self) -> Result<Option<u64>, Error> {
        if self.done_with_sorting_and_ags {
            return Ok(None);
        }
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
        if self.needs_sorting_and_ags {
            loop {
                match if self.done_with_sorting_and_ags { None } else { try!(self.get_next()) } {
                    Some(seq) => {
                        let bind = HashMap::new();
                        let mut results = VecDeque::new();
                        try!(self.returnable.fetch_result(&mut self.iter, seq,
                                                          &bind, &mut results));
                        self.in_buffer.push(results);
                        if self.in_buffer.len() == self.limit {
                            self.do_sorting_and_ags();
                        }
                    },
                    None => {
                        if !self.done_with_sorting_and_ags {
                            self.do_sorting_and_ags();
                            self.done_with_sorting_and_ags = true;
                            if !self.aggr_finals.is_empty() {
                                // need to finalize the values
                                for end in self.sorted_buffer.iter_mut() {
                                    for &(ref finalize, n) in self.aggr_finals.iter() {
                                        (finalize)(&mut end[n]);
                                    }
                                }
                            }
                        }
                        if let Some(mut result) = self.sorted_buffer.pop() {
                            self.buffer.clear();
                            try!(self.returnable.write_result(&mut result, &mut self.buffer));
                            let str = unsafe{str::from_utf8_unchecked(&self.buffer[..])};
                            return Ok(Some(str.to_string()));
                        } else {
                            return Ok(None);
                        }
                    },
                }
            }
        } else {
            let seq = match try!(self.get_next()) {
                Some(seq) => seq,
                None => return Ok(None),
            };
            let bind = HashMap::new();
            let mut results = VecDeque::new();
            try!(self.returnable.fetch_result(&mut self.iter, seq, &bind, &mut results));
            self.buffer.clear();
            try!(self.returnable.write_result(&mut results, &mut self.buffer));
            Ok(Some(unsafe{str::from_utf8_unchecked(&self.buffer[..])}.to_string()))
        }
    }

    fn cmp_results(sorts: &Vec<(Sort, usize)>,
                   a: &VecDeque<JsonValue>, b: &VecDeque<JsonValue>) -> Ordering {
        for &(ref sort_dir, n) in sorts.iter() {
            let cmp = if *sort_dir != Sort::Desc {
                b[n].cmp(&a[n])
            } else {
                a[n].cmp(&b[n])
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }

    fn do_sorting_and_ags(&mut self) {
        // ugh borrow check madness means this is how this must happen.
        // we need to put it back before returning.
        let sorts = self.sorts.take().unwrap();
        if !sorts.is_empty() {
            self.in_buffer.sort_by(|a, b| QueryResults::cmp_results(&sorts, &a, &b));
        }
        // put back
        self.sorts = Some(sorts);

        if !self.does_group_or_aggr {
            if self.sorted_buffer.is_empty() {
                swap(&mut self.sorted_buffer, &mut self.in_buffer);
            } else {
                //merge the sorted buffers
                let mut new_buffer = Vec::with_capacity(self.sorted_buffer.len() +
                                                        self.in_buffer.len());
                let mut option_a = self.sorted_buffer.pop();
                let mut option_b = self.in_buffer.pop();
                // take out for borrow check
                let sorts = self.sorts.take().unwrap();
                loop {
                    match (option_a, option_b) {
                        (Some(a), Some(b)) => {
                            match QueryResults::cmp_results(&sorts, &a, &b) {
                                Ordering::Less => {
                                    new_buffer.push(b);
                                    option_a = Some(a);
                                    option_b = self.in_buffer.pop();
                                },
                                Ordering::Greater => {
                                    new_buffer.push(a);
                                    option_a = self.sorted_buffer.pop();
                                    option_b = Some(b);

                                },
                                Ordering::Equal => {
                                    new_buffer.push(a);
                                    new_buffer.push(b);
                                    option_a = self.sorted_buffer.pop();
                                    option_b = self.in_buffer.pop();
                                }
                            }
                            if new_buffer.len() >= self.limit {
                                self.sorted_buffer.clear();
                                self.in_buffer.clear();
                                new_buffer.truncate(self.limit);
                                break;
                            }
                        },
                        (Some(a), None) => {
                            new_buffer.push(a);
                            if new_buffer.len() == self.limit {
                                break;
                            }
                            while let Some(a) = self.sorted_buffer.pop() {
                                new_buffer.push(a);
                                if new_buffer.len() == self.limit {
                                    break;
                                }
                            }
                            break;
                        },
                        (None, Some(b)) => {
                            new_buffer.push(b);
                            if new_buffer.len() == self.limit {
                                break;
                            }
                            while let Some(b) = self.in_buffer.pop() {
                                new_buffer.push(b);
                                if new_buffer.len() == self.limit {
                                    break;
                                }
                            }
                            break;
                        },
                        (None, None) => break,
                    }   
                }
                // put back
                self.sorts = Some(sorts);

                new_buffer.reverse();
                swap(&mut self.sorted_buffer, &mut new_buffer);
            }
            return;
        }

        
        //merge the sorted buffers
        let mut new_buffer = Vec::with_capacity(self.sorted_buffer.len() +
                                                self.in_buffer.len());
        let mut option_old = self.sorted_buffer.pop();
        let mut option_new = self.in_buffer.pop();
        // take out for borrow check
        let sorts = self.sorts.take().unwrap();
        loop {
            match (option_old, option_new) {
                (Some(mut old), Some(mut new)) => {
                    match QueryResults::cmp_results(&sorts, &old, &new) {
                        Ordering::Less => {
                            for &(ref init, n) in self.aggr_inits.iter() {
                                (init)(&mut new[n]);
                            }
                            //push back old value into sorted_buffer,
                            //then use new value as old value.
                            self.sorted_buffer.push(old);
                            option_old = Some(new);
                            option_new = self.in_buffer.pop();
                        },
                        Ordering::Greater => {
                            new_buffer.push(old);
                            option_old = self.sorted_buffer.pop();
                            option_new = Some(new);
                        },
                        Ordering::Equal => {
                            for &(ref action, ref user_arg, n) in self.aggr_actions.iter() {
                                // we can't swap out a value of new directly, so this lets us
                                // without shifting or cloning values, both of which can be
                                // expensive
                                let mut new_n = JsonValue::Null;
                                swap(&mut new_n, &mut new[n]);
                                (action)(&mut old[n], new_n, &user_arg);
                            }
                            option_old = Some(old);
                            option_new = self.in_buffer.pop();
                        }
                    }
                    if new_buffer.len() == self.limit {
                        self.sorted_buffer.clear();
                        self.in_buffer.clear();
                        break;
                    }
                },
                (Some(old), None) => {
                    new_buffer.push(old);
                    if new_buffer.len() == self.limit {
                        break;
                    }
                    while let Some(old) = self.sorted_buffer.pop() {
                        new_buffer.push(old);
                        if new_buffer.len() == self.limit {
                            break;
                        }
                    }
                    break;
                },
                (None, Some(mut new)) => {
                    for &(ref init, n) in self.aggr_inits.iter() {
                        (init)(&mut new[n]);
                    }
                    option_old = Some(new);
                    option_new = self.in_buffer.pop();
                },
                (None, None) => break,
            }   
        }
        // put back
        self.sorts = Some(sorts);

        new_buffer.reverse();
        swap(&mut self.sorted_buffer, &mut new_buffer);
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

    fn consume_key(&mut self) -> Result<Option<String>, Error> {
        if let Some(key) = self.consume_field() {
            Ok(Some(key))
        } else if let Some(key) = try!(self.consume_string_literal()) {
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }

    fn consume_field(&mut self) -> Option<String> {
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

    fn consume_default(&mut self) -> Result<Option<JsonValue>, Error> {
        if self.consume("default") {
            try!(self.must_consume("="));
            if let Some(json) = try!(self.json()) {
                Ok(Some(json))
            } else {
                Err(Error::Parse("Expected json value for default".to_string()))
            }
        } else {
            Ok(None)
        }
    }
    
    fn consume_aggregate(&mut self) -> Result<Option<(AggregateFun, 
                                                      KeyBuilder,
                                                      JsonValue)>, Error> {
        let offset = self.offset;
        let mut aggregate_fun = if self.consume("group") {
            AggregateFun::GroupAsc
        } else if self.consume("sum") {
            AggregateFun::Sum
        } else if self.consume("max") {
            AggregateFun::Max
        } else if self.consume("min") {
            AggregateFun::Min
        } else if self.consume("list") {
            AggregateFun::List
        } else if self.consume("concat") {
            AggregateFun::Concat
        } else if self.consume("avg") {
            AggregateFun::Avg
        } else if self.consume("count") {
            AggregateFun::Count
        } else {
            return Ok(None)
        };

        if self.consume("(") {
            if aggregate_fun == AggregateFun::Count {
                try!(self.must_consume(")"));
                Ok(Some((aggregate_fun, KeyBuilder::new(), JsonValue::Null)))
            } else if aggregate_fun == AggregateFun::Concat {
                if let Some(kb) = try!(self.consume_keypath()) {
                    let json = if self.consume("sep") {
                        try!(self.must_consume("="));
                        JsonValue::String(try!(self.must_consume_string_literal()))
                    } else {
                        JsonValue::String(",".to_string())
                    };
                    try!(self.must_consume(")"));
                    Ok(Some((aggregate_fun, kb, json)))
                } else {
                    Err(Error::Parse("Expected keypath or bind variable".to_string()))
                }
            } else if let Some(kb) = try!(self.consume_keypath()) {
                if self.consume("order") {
                    try!(self.must_consume("="));
                    if self.consume("asc") {
                        aggregate_fun = AggregateFun::GroupAsc;
                    } else if self.consume("desc") {
                        aggregate_fun = AggregateFun::GroupDesc;
                    } else {
                        return Err(Error::Parse("Expected asc or desc".to_string()));
                    }
                }
                try!(self.must_consume(")"));

                Ok(Some((aggregate_fun, kb, JsonValue::Null)))
            } else {
                Err(Error::Parse("Expected keypath or bind variable".to_string()))
            }
        } else {
            // this consumed word above might be a Bind var. Unconsume and return nothing.
            self.offset = offset;
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
                 if let Some(key) = self.consume_field() {
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
                if let Some(key) = self.consume_field() {
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

    fn consume_number(&mut self) -> Result<Option<f64>, Error> {
        // Yes this parsing code is hideously verbose. But it conforms exactly to the json spec
        // and uses the rust f64 parser, which can't tell us how many characters it used or needs.

        // At the end it then uses the std rust String::parse<f64>() method to parse and return
        // the f64 value and advance the self.offset. The rust method is a super set of the
        // allowable json syntax, so it will parse any valid json floating point number. It might
        // return an error if the number is out of bounds.
        let mut result = String::new();
        'outer: loop {
            // this loop isn't a loop, it's just there to scope the self borrow
            // and then jump to the end to do another borrow (self.ws())
            let mut chars = self.query[self.offset..].chars();
            let mut c = if let Some(c) = chars.next() {
                c
            } else {
                return Ok(None);
            };

            // parse the sign
            c = if c == '-' {
                result.push('-');
                if let Some(c) = chars.next() { c } else {return Ok(None); }
            } else {
                c
            };

            // parse the first digit
            let mut leading_zero = false;
            c = if c == '0' {
                result.push('0');
                leading_zero = true;
                if let Some(c) = chars.next() { c } else {return Ok(None); }
            } else if c >= '1' && c <= '9' {
                result.push(c);
                if let Some(c) = chars.next() { c } else {return Ok(None); }
            } else if result.is_empty() {
                // no sign or digits found. not a number
                return Ok(None);
            } else {
                return Err(Error::Parse("Expected digits after sign (-).".to_string()));
            };

            // parse remaning significant digits
            if !leading_zero {
                // no more digits allowed if first digit is zero
                loop {
                    c = if c >= '0' && c <= '9' {
                        result.push(c);
                        if let Some(c) = chars.next() {
                            c
                        } else {
                            break 'outer;
                        }
                    } else {
                        break;
                    };
                }
            }

            // parse decimal
            c = if c == '.' {
                result.push(c);
                if let Some(c) = chars.next() {
                    c
                } else {
                    return Err(Error::Parse("Expected digits after decimal point.".to_string()));
                }
            } else {
                break 'outer;
            };

            // parse mantissa
            let mut found_mantissa = false;
            loop {
                c = if c >= '0' && c <= '9' {
                    result.push(c);
                    found_mantissa = true;

                    if let Some(c) = chars.next() {
                        c
                    } else {
                        break 'outer;
                    }
                } else {
                    if found_mantissa {
                        break;
                    }
                    return Err(Error::Parse("Expected digits after decimal point.".to_string()));
                };
            }

            // parse exponent symbol
            c = if c == 'e' || c == 'E' {
                result.push(c);
                if let Some(c) = chars.next() {
                    c
                } else {
                    return Err(Error::Parse("Expected exponent after e.".to_string()));
                }
            } else {
                break 'outer;
            };
            
            // parse exponent sign
            c = if c == '+' || c == '-' {
                result.push(c);
                if let Some(c) = chars.next() {
                    c
                } else {
                    return Err(Error::Parse("Expected exponent after e.".to_string()));
                }
            } else {
                c
            };

            // parse exponent digits
            let mut found_exponent = false;
            loop {
                c = if c >= '0' && c <= '9' {
                    result.push(c);
                    found_exponent = true;
                    if let Some(c) = chars.next() {
                        c
                    } else {
                        break 'outer;
                    }
                } else {
                    if found_exponent {
                        break 'outer;
                    }
                    return Err(Error::Parse("Expected exponent after e.".to_string()));
                }
            }
        }

        self.offset += result.len();
        self.ws();
        Ok(Some(try!(result.parse())))
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
        if !self.could_consume("\"") {
            return Ok(None);
        }
        // can't consume("\"") the leading quote because it will also skip leading whitespace
        // inside the string literal
        self.offset += 1;
        {
        let mut chars = self.query[self.offset..].chars();
        'outer: loop {
            let char = if let Some(char) = chars.next() {
                char
            } else {
                break;
            };
            if char == '\\' {
                self.offset += 1;

                let char = if let Some(char) = chars.next() {
                    char
                } else {
                    break;
                };
                match char {
                    '\\' | '"' | '/' => lit.push(char),
                    'n' => lit.push('\n'),
                    'b' => lit.push('\x08'),
                    'r' => lit.push('\r'),
                    'f' => lit.push('\x0C'),
                    't' => lit.push('\t'),
                    'v' => lit.push('\x0B'),
                    'u' => {
                        let mut n = 0;
                        for _i in 0..4 {
                            let char = if let Some(char) = chars.next() {
                                char
                            } else {
                                break 'outer;
                            };
                            n = match char {
                                c @ '0' ... '9' => n * 16 + ((c as u16) - ('0' as u16)),
                                c @ 'a' ... 'f' => n * 16 + (10 + (c as u16) - ('a' as u16)),
                                c @ 'A' ... 'F' => n * 16 + (10 + (c as u16) - ('A' as u16)),
                                _ => return Err(Error::Parse(format!(
                                        "Invalid hexidecimal escape: {}", char))),
                            };
                            
                        }
                        self.offset += 3; // 3 because 1 is always added after the match below        
                    },
                    _ => return Err(Error::Parse(format!("Unknown character escape: {}",
                                                            char))),
                };
                self.offset += 1;
            } else {
                if char == '"' {
                    break;
                } else {
                    lit.push(char);
                    self.offset += char.len_utf8();
                }
            }
        }
        }
        try!(self.must_consume("\""));
        Ok(Some(lit))
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
        } else if let Some(field) = try!(self.consume_key()) {
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

    fn sort_clause(&mut self) -> Result<HashMap<String, SortInfo>, Error> {
        let mut sort_infos = HashMap::new();
        if self.consume("sort") {
            loop {
                if let Some(kb) = try!(self.consume_keypath()) {
                    // doing the search for source 2x so user can order
                    // anyway they like. Yes it's a hack, but it simple.
                    let mut sort = if self.consume("asc") {
                        Sort::Asc
                    } else if self.consume("desc") {
                        Sort::Desc
                    } else {
                        Sort::Asc
                    };

                    let default = if self.consume("default") {
                        try!(self.must_consume("="));
                        if let Some(json) = try!(self.json()) {
                            json
                        } else {
                            return Err(Error::Parse("Expected Json after default.".to_string()));
                        }
                    } else {
                        JsonValue::Null
                    };

                    sort = if self.consume("asc") {
                        Sort::Asc
                    } else if self.consume("desc") {
                        Sort::Desc
                    } else {
                        sort
                    };

                    sort_infos.insert(kb.value_key(0), SortInfo{kb:kb,
                                                                sort:sort,
                                                                default:default});
                    if !self.consume(",") {
                        break;
                    }
                }
            }
            if sort_infos.is_empty() {
                return Err(Error::Parse("Expected field path in sort expression.".to_string()));
            }
        }
        Ok(sort_infos)
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
            Ok(Box::new(RetValue{kb: kb, ag:None, default: JsonValue::Null, sort: None}))
        }
    }

    fn ret_object(&mut self) -> Result<Box<Returnable>, Error> {
        try!(self.must_consume("{"));
        let mut fields: Vec<(String, Box<Returnable>)> = Vec::new();
        loop {
            if let Some(field) = try!(self.consume_key()) {
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
        Ok(Box::new(RetArray{slots: slots}))

    }

    fn ret_value(&mut self) -> Result<Option<Box<Returnable>>, Error> {
        if let Some((ag, kb, json)) = try!(self.consume_aggregate()) {
            let default = if let Some(default) = try!(self.consume_default()) {
                default
            } else {
                JsonValue::Null
            };
            Ok(Some(Box::new(RetValue{kb: kb, ag: Some((ag, json)),
                                      default: default, sort:None})))
        }
        else if let Some(kb) = try!(self.consume_keypath()) {
            let default = if let Some(default) = try!(self.consume_default()) {
                default
            } else {
                JsonValue::Null
            };
            Ok(Some(Box::new(RetValue{kb: kb, ag: None, default: default, sort: None})))
        } else if self.could_consume("{") {
            Ok(Some(try!(self.ret_object())))
        } else if self.could_consume("[") {
            Ok(Some(try!(self.ret_array())))
        } else if let Some(string) = try!(self.consume_string_literal()) {
            Ok(Some(Box::new(RetLiteral{json: JsonValue::String(string)})))
        } else if let Some(num) = try!(self.consume_number()) {
            Ok(Some(Box::new(RetLiteral{json: JsonValue::Number(num)})))
        } else {
            if self.consume("true") {
                Ok(Some(Box::new(RetLiteral{json: JsonValue::True})))
            } else if self.consume("false") {
                Ok(Some(Box::new(RetLiteral{json: JsonValue::False})))
            } else if self.consume("null") {
                Ok(Some(Box::new(RetLiteral{json: JsonValue::Null})))
            } else {
                Ok(None)
            }
        }
    }

    fn limit_clause(&mut self) -> Result<usize, Error> {
        if self.consume("limit") {
            if let Some(i) = try!(self.consume_integer()) {
                if i <= 0 {
                    return Err(Error::Parse("limit must be an integer greater than 0"
                                            .to_string()));
                }
                Ok(i as usize)
            } else {
                return Err(Error::Parse("limit expects an integer greater than 0"
                                            .to_string()));
            }
        } else {
            Ok(usize::MAX)
        }
    }

    fn json(&mut self) -> Result<Option<JsonValue>, Error> {
        if self.could_consume("{") {
            Ok(Some(try!(self.json_object())))
        } else if self.could_consume("[") {
            Ok(Some(try!(self.json_array())))
        } else if let Some(string) = try!(self.consume_string_literal()) {
            Ok(Some(JsonValue::String(string)))
        } else {
            if self.consume("true") {
                Ok(Some(JsonValue::True))
            } else if self.consume("false") {
                Ok(Some(JsonValue::False))
            } else if self.consume("null") {
                Ok(Some(JsonValue::Null))
            } else if let Some(num) = try!(self.consume_number()) {
                Ok(Some(JsonValue::Number(num)))
            } else {
                Ok(None)
            }
        }
    }

    fn json_object(&mut self) -> Result<JsonValue, Error> {
        try!(self.must_consume("{"));
        let mut object = Vec::new();
        if self.consume("}") {
            return Ok(JsonValue::Object(object));
        }
        loop {
            if let Some(field) = try!(self.consume_key()) {
                try!(self.must_consume(":"));
                if let Some(json) = try!(self.json()) {
                    object.push((field, json));
                    if !self.consume(",") {
                        break;
                    }
                } else {
                    return Err(Error::Parse("Invalid json found".to_string()));
                }
            } else {
                return Err(Error::Parse("Invalid json found".to_string()));
            }
        }
        try!(self.must_consume("}"));
        Ok(JsonValue::Object(object))
    }

    fn json_array(&mut self) -> Result<JsonValue, Error> {
        try!(self.must_consume("["));
        let mut array = Vec::new();
        if self.consume("]") {
            return Ok(JsonValue::Array(array));
        }
        loop {
            if let Some(json) = try!(self.json()) {
                array.push(json);
                if !self.consume(",") {
                    break;
                }
            } else {
                return Err(Error::Parse("Invalid json found".to_string()));
            }
        }
        try!(self.must_consume("]"));
        Ok(JsonValue::Array(array))
    }

    fn build_filter(&mut self) -> Result<Box<QueryRuntimeFilter + 'a>, Error> {
        self.ws();
        Ok(try!(self.find()))
    }
}



#[derive(PartialEq, Eq, Clone)]
pub enum AggregateFun {
    GroupAsc,
    GroupDesc,
    Sum,
    Max,
    Min,
    List,
    Concat,
    Avg,
    Count,
}

struct AggregateFunImpls {
    init: Option<fn (&mut JsonValue)>,
    action: fn (&mut JsonValue, JsonValue, &JsonValue),
    extract: Option<fn (&mut JsonValue)>,
}

impl AggregateFun {
    fn get_fun_impls(&self) -> AggregateFunImpls {
        match self {
            &AggregateFun::GroupAsc => panic!("cannot get aggregate fun for grouping!"),
            &AggregateFun::GroupDesc => panic!("cannot get aggregate fun for grouping!"),
            &AggregateFun::Sum => AggregateFunImpls{
                init: Some(AggregateFun::sum_init),
                action: AggregateFun::sum,
                extract: None,
             },
            &AggregateFun::Max => AggregateFunImpls{
                init: None,
                action: AggregateFun::max,
                extract: None,
             },
            &AggregateFun::Min => AggregateFunImpls{
                init: None,
                action: AggregateFun::min,
                extract: None,
             },
            &AggregateFun::List => AggregateFunImpls{
                init: Some(AggregateFun::list_init),
                action: AggregateFun::list,
                extract: None,
             },
            &AggregateFun::Concat => AggregateFunImpls{
                init: Some(AggregateFun::concat_init),
                action: AggregateFun::concat,
                extract: None,
             },
            &AggregateFun::Avg => AggregateFunImpls{
                init: Some(AggregateFun::avg_init),
                action: AggregateFun::avg,
                extract: Some(AggregateFun::avg_final),
             },
            &AggregateFun::Count => AggregateFunImpls{
                init: Some(AggregateFun::count_init),
                action: AggregateFun::count,
                extract: None,
             },
        }
    }

    fn sum_init(existing: &mut JsonValue) {
        if let &mut JsonValue::Number(_) = existing {
            //do nothing
        } else {
            *existing = JsonValue::Number(0.0)
        }
    }

    fn sum(existing: &mut JsonValue, new: JsonValue, _user_arg: &JsonValue) {
        match (existing, new) {
            (&mut JsonValue::Number(ref mut existing), JsonValue::Number(new)) => {
                *existing += new;
            },
            _ => (),
        }
    }

    fn max(existing: &mut JsonValue, new: JsonValue, _user_arg: &JsonValue) {
        if *existing < new {
            *existing = new
        }
    }

    fn min(existing: &mut JsonValue, new: JsonValue, _user_arg: &JsonValue) {
        if *existing > new {
            *existing = new
        }
    }

    fn list_init(existing: &mut JsonValue) {
        *existing = JsonValue::Array(vec![existing.clone()]);
    }

    fn list(existing: &mut JsonValue, new: JsonValue, _user_arg: &JsonValue) {
        if let &mut JsonValue::Array(ref mut existing) = existing {
            existing.push(new);
        }
    }

    fn concat_init(existing: &mut JsonValue) {
        if let &mut JsonValue::String(ref _string) = existing {
            // do nothing
        } else {
            JsonValue::String(String::new());
        }
    }

    fn concat(existing: &mut JsonValue, new: JsonValue, user_arg: &JsonValue) {
        if let &mut JsonValue::String(ref mut existing) = existing {
            if let JsonValue::String(new) = new {
                if let &JsonValue::String(ref user_arg) = user_arg {
                    existing.push_str(&user_arg);
                    existing.push_str(&new);
                }
            }
        }
    }

    fn avg_init(existing: &mut JsonValue) {
        let new = if let &mut JsonValue::Number(ref num) = existing {
            JsonValue::Array(vec![JsonValue::Number(num.clone()), JsonValue::Number(1.0)])
        } else {
            JsonValue::Array(vec![JsonValue::Number(0.0), JsonValue::Number(0.0)])
        };
        *existing = new;
    }

    fn avg(existing: &mut JsonValue, new: JsonValue, _user_arg: &JsonValue) {
        if let JsonValue::Number(new) = new {
            if let &mut JsonValue::Array(ref mut array) = existing {
                let mut avg = if let &JsonValue::Number(ref avg) = &array[0] {
                    *avg
                } else {
                    // can't happen but compiler need this here
                    1.0
                };

                let mut count = if let &JsonValue::Number(ref count) = &array[1] {
                    *count
                } else {
                    // can't happen but compiler need this here
                    1.0
                };
                
                avg = (avg * count + new) / (count + 1.0);
                count += 1.0;
                array[0] = JsonValue::Number(avg);
                array[1] = JsonValue::Number(count);
            }
        }
    }

    fn avg_final(existing: &mut JsonValue) {
        let json = if let &mut JsonValue::Array(ref mut array) = existing {
            if let &JsonValue::Number(ref avg) = &array[0] {
                if let &JsonValue::Number(ref count) = &array[1] {
                    if *count == 0.0 {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(*avg)
                    }
                } else {
                    // can't happen but compiler need this here
                    JsonValue::Null
                }
            } else {
                // can't happen but compiler need this here
                JsonValue::Null
            }
        } else {
            // can't happen but compiler need this here
            JsonValue::Null
        };
        *existing = json
    }

    fn count_init(existing: &mut JsonValue) {
        *existing = JsonValue::Number(1.0);
    }

    fn count(existing: &mut JsonValue, _: JsonValue, _user_arg: &JsonValue) {
        if let &mut JsonValue::Number(ref mut num) = existing {
            *num += 1.0;
        }
    }
}

#[derive(PartialEq, PartialOrd, Clone, Debug)]
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

    fn cmp_always_equal(_a: &JsonValue, _b: &JsonValue) -> Ordering {
        Ordering::Equal
    }

    fn cmp_f64(a: &JsonValue, b: &JsonValue) -> Ordering {
        if let &JsonValue::Number(a_val) = a {
            if let &JsonValue::Number(b_val) = b {
                if a_val < b_val {
                    Ordering::Less
                } else if a_val > b_val {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            } else {
                panic!("cast error in cmp_f64");
            } 
        } else {
            panic!("cast error in cmp_f64");
        } 
    }

    fn cmp_string(a: &JsonValue, b: &JsonValue) -> Ordering {
        if let &JsonValue::String(ref a_val) = a {
            if let &JsonValue::String(ref b_val) = b {
                // Note we eventually want to switch to a collation library like ICU
                a_val.cmp(&b_val)
            } else {
                panic!("cast error in cmp_string");
            } 
        } else {
            panic!("cast error in cmp_string");
        } 
    }

    fn cmp_array(a: &JsonValue, b: &JsonValue) -> Ordering {
        if let &JsonValue::Array(ref a_val) = a {
            if let &JsonValue::Array(ref b_val) = b {
                for (a_el, b_el) in a_val.iter().zip(b_val.iter()) {
                    let order = a_el.cmp(&b_el);
                    if order != Ordering::Equal {
                        return order;
                    }
                }
                // if we got here all elements were equal. But one array might be longer
                // so sort it last
                a_val.len().cmp(&b_val.len())
            } else {
                panic!("cast error in cmp_array");
            } 
        } else {
            panic!("cast error in cmp_array");
        } 
    }
    
    fn cmp_object(a: &JsonValue, b: &JsonValue) -> Ordering {
        if let &JsonValue::Object(ref a_val) = a {
            if let &JsonValue::Object(ref b_val) = b {
                for (a_el, b_el) in a_val.iter().zip(b_val.iter()) {
                    // compare key
                    let mut order = a_el.0.cmp(&b_el.0);
                    if order != Ordering::Equal {
                        return order;
                    }
                    // compare value
                    order = a_el.1.cmp(&b_el.1);
                    if order != Ordering::Equal {
                        return order;
                    }
                }
                // if we got here all elements were equal. But one object might be longer
                // so sort it last
                a_val.len().cmp(&b_val.len())
            } else {
                panic!("cast error in cmp_object");
            } 
        } else {
            panic!("cast error in cmp_object");
        } 
    }

    fn type_sort_order(&self) -> (usize, fn(&JsonValue, &JsonValue) -> Ordering) {
        match self {
            &JsonValue::Null => (0, JsonValue::cmp_always_equal),
            &JsonValue::False => (1, JsonValue::cmp_always_equal),
            &JsonValue::True => (2, JsonValue::cmp_always_equal),
            &JsonValue::Number(_) => (3, JsonValue::cmp_f64),
            &JsonValue::String(_) => (4, JsonValue::cmp_string),
            &JsonValue::Array(_) => (5, JsonValue::cmp_array),
            &JsonValue::Object(_) => (6, JsonValue::cmp_object),
        }
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

impl Eq for JsonValue {}

impl Ord for JsonValue {
    fn cmp(&self, other: &JsonValue) -> Ordering {
        let (self_order_num, self_cmp_fun) = self.type_sort_order();
        let (other_order_num, _other_cmp_fun) = other.type_sort_order();
        match self_order_num.cmp(&other_order_num) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self_cmp_fun(self, other),
        }
    }
}

trait Returnable {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<JsonValue>) -> Result<(), Error>;

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>);

    fn get_sorting(&self, sorts: &mut Vec<Option<Sort>>);

    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>);

    fn write_result(&self, results: &mut VecDeque<JsonValue>,
                    write: &mut Write) -> Result<(), Error>;
}

struct RetObject {
    fields: Vec<(String, Box<Returnable>)>,
}

impl Returnable for RetObject {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<JsonValue>) -> Result<(), Error> {
        for &(ref _key, ref field) in self.fields.iter() {
            try!(field.fetch_result(iter, seq, bind_var_keys, result));
        }
        Ok(())
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        for &(ref _key, ref field) in self.fields.iter() {
            field.get_aggregate_funs(funs);
        }
    }

    fn get_sorting(&self, sorts: &mut Vec<Option<Sort>>) {
       for &(ref _key, ref field) in self.fields.iter() {
            field.get_sorting(sorts);
       }
    }
    
    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
       for &mut (ref _key, ref mut field) in self.fields.iter_mut() {
            field.take_sort_for_matching_fields(map);
       }
    }

    fn write_result(&self, results: &mut VecDeque<JsonValue>,
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


struct RetArray {
    slots: Vec<Box<Returnable>>,
}

impl Returnable for RetArray {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<JsonValue>) -> Result<(), Error> {
        for ref slot in self.slots.iter() {
            try!(slot.fetch_result(iter, seq, bind_var_keys, result));
        }
        Ok(())
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
       for ref slot in self.slots.iter() {
            slot.get_aggregate_funs(funs);
        }
    }

    fn get_sorting(&self, sorts: &mut Vec<Option<Sort>>) {
       for ref slot in self.slots.iter() {
            slot.get_sorting(sorts);
       }
    }
    
    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
       for slot in self.slots.iter_mut() {
            slot.take_sort_for_matching_fields(map);
       }
    }

    fn write_result(&self, results: &mut VecDeque<JsonValue>,
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

#[derive(PartialEq, Eq, Clone)]
enum Sort {
    Asc,
    Desc,
}

struct SortInfo {
    kb: KeyBuilder,
    sort: Sort,
    default: JsonValue,
}

struct RetHidden {
    unrendered: Vec<RetValue>,
    visible: Box<Returnable>,
}

impl Returnable for RetHidden {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<JsonValue>) -> Result<(), Error> {
        for ref mut unrendered in self.unrendered.iter() {
            try!(unrendered.fetch_result(iter, seq, bind_var_keys, result));
        }

        self.visible.fetch_result(iter, seq, bind_var_keys, result)
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        self.visible.get_aggregate_funs(funs);
    }

    fn get_sorting(&self, sorts: &mut Vec<Option<Sort>>) {
       for ref mut unrendered in self.unrendered.iter() {
            unrendered.get_sorting(sorts);
        }
       
        self.visible.get_sorting(sorts);
    }
    
    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
        self.visible.take_sort_for_matching_fields(map);
    }

    fn write_result(&self, results: &mut VecDeque<JsonValue>,
                    write: &mut Write) -> Result<(), Error> {
        for _n in 0..self.unrendered.len() {
            // we already sorted at this point, now discard the values
            results.pop_front();
        }
        self.visible.write_result(results, write)
    }
}

struct RetLiteral {
    json: JsonValue,
}

impl Returnable for RetLiteral {
    fn fetch_result(&self, _iter: &mut DBIterator, _seq: u64,
                    _bind_var_keys: &HashMap<String, String>,
                    _result: &mut VecDeque<JsonValue>) -> Result<(), Error> {
        Ok(())
    }

    fn get_aggregate_funs(&self, _funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        //noop
    }

    fn get_sorting(&self, _sorts: &mut Vec<Option<Sort>>) {
        //noop
    }
    
    fn take_sort_for_matching_fields(&mut self, _map: &mut HashMap<String, SortInfo>) {
        //noop
    }

    fn write_result(&self, _results: &mut VecDeque<JsonValue>,
                    write: &mut Write) -> Result<(), Error> {
        
        self.json.render(write)
    }
}

pub struct RetValue {
    kb: KeyBuilder,
    ag: Option<(AggregateFun, JsonValue)>,
    default: JsonValue,
    sort: Option<Sort>,
}

impl RetValue {
    fn bytes_to_json_value(bytes: &[u8]) -> JsonValue {
        match bytes[0] as char {
            's' => {
                let string = unsafe{str::from_utf8_unchecked(&bytes[1..])}.to_string();
                JsonValue::String(string)
            },
            'f' => {
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
                    result: &mut VecDeque<JsonValue>) -> Result<(), Error> {
        if Some((AggregateFun::Count, JsonValue::Null)) == self.ag {
            //don't fetch anything for count(). just stick in a null
            result.push_back(JsonValue::Null);
            return Ok(());
        }
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
                result.push_back(self.default.clone());
                return Ok(())
            },
        };

        if !key.starts_with(value_key.as_bytes()) {
            result.push_back(self.default.clone());
            return Ok(());
        }

        let json_value = try!(RetValue::fetch(&mut iter.peekable(), &value_key,
                                              key, value));
        result.push_back(json_value);
        Ok(())
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        funs.push(self.ag.clone());
    }

    fn get_sorting(&self, sorts: &mut Vec<Option<Sort>>) {
        sorts.push(self.sort.clone());
    }
    
    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String,SortInfo>) {
        if let Some(sort_info) = map.remove(&self.kb.value_key(0)) {
            self.sort = Some(sort_info.sort);
        }
    }

    fn write_result(&self, results: &mut VecDeque<JsonValue>,
                    write: &mut Write) -> Result<(), Error> {
        if let Some(json) = results.pop_front() {
            try!(json.render(write));
        } else {
            panic!("missing result!");
        }
        Ok(())
    }
}



#[cfg(test)]
mod tests {
    extern crate rustc_serialize;

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
        let _ = index.add(r#"{"_id":"13", "A":["foo",1,true,false,null,{},[]]}"#);

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

        query_results = Query::get_matches(r#"find {A:[ == "foo"]}
                                              return .A "#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["foo",1,true,false,null,{},[]]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        query_results = Query::get_matches(r#"find {A:[ == "foo"]}
                                              return .B "#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"null"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        query_results = Query::get_matches(r#"find {A:[ == "foo"]}
                                              return .B default={foo:"foo"}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"foo":"foo"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        query_results = Query::get_matches(r#"find {A:[ == "foo"]}
                                              return .B default={}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        query_results = Query::get_matches(r#"find {A:[ == "foo"]}
                                              return {foo: .B default={bar:"bar"}}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"foo":{"bar":"bar"}}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

        query_results = Query::get_matches(r#"find {A:[ == "foo"]}
            return {"a":"a","b":1.123,"true":true,"false":false,"null":null,array:[],object:{}}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),
            Some(r#"{"a":"a","b":1.123,"true":true,"false":false,"null":null,"array":[],"object":{}}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

        
    }

    #[test]
    fn test_query_group() {
        let dbname = "target/tests/querytestgroup";
        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();


        let _ = index.add(r#"{"_id":"1", "foo":"group", "baz": "a", "bar": 1}"#);
        let _ = index.add(r#"{"_id":"2", "foo":"group", "baz": "b", "bar": 2}"#);
        let _ = index.add(r#"{"_id":"3", "foo":"group", "baz": "c", "bar": 3}"#);
        let _ = index.add(r#"{"_id":"4", "foo":"group", "baz": "a", "bar": 1}"#);
        let _ = index.add(r#"{"_id":"5", "foo":"group", "baz": "b", "bar": 2}"#);
        let _ = index.add(r#"{"_id":"6", "foo":"group", "baz": "c", "bar": 3}"#);
        let _ = index.add(r#"{"_id":"7", "foo":"group", "baz": "a", "bar": 1}"#);
        let _ = index.add(r#"{"_id":"8", "foo":"group", "baz": "b", "bar": 2}"#);
        let _ = index.add(r#"{"_id":"9", "foo":"group", "baz": "c", "bar": 3}"#);
        let _ = index.add(r#"{"_id":"10", "foo":"group", "baz": "a", "bar": 1}"#);
        let _ = index.add(r#"{"_id":"11", "foo":"group", "baz": "b", "bar": 2}"#);
        let _ = index.add(r#"{"_id":"12", "foo":"group", "baz": "c", "bar": 3}"#);
        
        index.flush().unwrap();

        {
        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                                    return {baz: group(.baz), bar: sum(.bar)}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"a","bar":4}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"b","bar":8}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"c","bar":12}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                                    return {bar: sum(.bar)}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"bar":24}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                                    return {bar: avg(.bar)}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"bar":2}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                            return {baz: group(.baz), concat: concat(.baz sep="|")}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"a","concat":"a|a|a|a"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"b","concat":"b|b|b|b"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"c","concat":"c|c|c|c"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                            return {baz: group(.baz), list: list(.baz)}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"a","list":["a","a","a","a"]}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"b","list":["b","b","b","b"]}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"c","list":["c","c","c","c"]}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                            return {baz: group(.baz), count: count()}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"a","count":4}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"b","count":4}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"baz":"c","count":4}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                            return {max: max(.bar)}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"max":3}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                            return {min: min(.bar)}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"min":1}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);


        let mut query_results = Query::get_matches(r#"find {foo: =="group"}
                            return {max: max(.baz)}"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"max":"c"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        }


        let _ = index.add(r#"{"_id":"1", "foo":"group2", "baz": "a", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"2", "foo":"group2", "baz": "a", "bar": "b"}"#);
        let _ = index.add(r#"{"_id":"3", "foo":"group2", "baz": "b", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"4", "foo":"group2", "baz": "b", "bar": "b"}"#);
        let _ = index.add(r#"{"_id":"5", "foo":"group2", "baz": "a", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"6", "foo":"group2", "baz": "a", "bar": "c"}"#);
        let _ = index.add(r#"{"_id":"7", "foo":"group2", "baz": "b", "bar": "d"}"#);
        let _ = index.add(r#"{"_id":"8", "foo":"group2", "baz": "b", "bar": "e"}"#);
        let _ = index.add(r#"{"_id":"9", "foo":"group2", "baz": "a", "bar": "f"}"#);
        
        index.flush().unwrap();

        {
        let mut query_results = Query::get_matches(r#"find {foo: =="group2"}
                            return [group(.baz order=asc), group(.bar order=desc), count()]"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","f",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","c",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","b",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","a",2]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["b","e",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["b","d",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["b","b",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["b","a",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        }

        {
        let mut query_results = Query::get_matches(r#"find {foo: =="group2"}
                            return [group(.baz order=asc), group(.bar order=desc), count()]
                            limit 2"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","f",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","c",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        }

        let _ = index.add(r#"{"_id":"1", "foo":"group3", "baz": "a", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"2", "foo":"group3",             "bar": "b"}"#);
        let _ = index.add(r#"{"_id":"3", "foo":"group3", "baz": "b", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"4", "foo":"group3", "baz": "b", "bar": "b"}"#);
        let _ = index.add(r#"{"_id":"5", "foo":"group3", "baz": "a", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"6", "foo":"group3", "baz": "a",           }"#);
        let _ = index.add(r#"{"_id":"7", "foo":"group3", "baz": "b", "bar": "d"}"#);
        let _ = index.add(r#"{"_id":"8", "foo":"group3", "baz": "b", "bar": "e"}"#);
        let _ = index.add(r#"{"_id":"9", "foo":"group3", "baz": "a", "bar": "f"}"#);
        
        index.flush().unwrap();


        let mut query_results = Query::get_matches(r#"find {foo: =="group2"}
                            return [group(.baz order=asc) default="a", group(.bar order=desc) default="c", count()]"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","f",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","c",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","b",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","a",2]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["b","e",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["b","d",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["b","b",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["b","a",1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);

    }


    #[test]
    fn test_query_json_collation() {
        let dbname = "target/tests/querytestjsoncollation";

        let _ = Index::delete(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();


        assert_eq!(Ok(()), index.add(r#"{"_id":"1", "foo":"coll", "bar": {}}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"2", "foo":"coll", "bar": {"foo":"bar"}}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"3", "foo":"coll", "bar": {"foo":"baz"}}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"4", "foo":"coll", "bar": {"foo":"baz","bar":"baz"}}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"5", "foo":"coll", "bar": {"foo":"baz","bar":"bar"}}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"6", "foo":"coll", "bar": 1}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"7", "foo":"coll", "bar": 1.00001}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"8", "foo":"coll", "bar": 2.00001}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"9", "foo":"coll", "bar": true}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"10", "foo":"coll", "bar": false}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"11", "foo":"coll", "bar": null}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"12", "foo":"coll", "bar": []}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"13", "foo":"coll", "bar": [true]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"14", "foo":"coll", "bar": [null]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"15", "foo":"coll", "bar": "string"}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"16", "foo":"coll", "bar": "string2"}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"17", "foo":"coll", "bar": "string3"}"#));
        
        index.flush().unwrap();

        
        {
        let mut query_results = Query::get_matches(r#"find {foo: =="coll"}
                                                      sort .bar asc
                                                      return .bar "#.to_string(), &index).unwrap();

        assert_eq!(query_results.next_result().unwrap(),Some(r#"null"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"false"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"true"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"1"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"1.00001"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"2.00001"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#""string""#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#""string2""#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#""string3""#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[null]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[true]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"bar":"bar","foo":"baz"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"bar":"baz","foo":"baz"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"foo":"bar"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"{"foo":"baz"}"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        }

        {
        let mut query_results = Query::get_matches(r#"find {foo: =="coll"}
                                                      sort .bar asc
                                                      return .bar
                                                      limit 5"#.to_string(), &index).unwrap();

        assert_eq!(query_results.next_result().unwrap(),Some(r#"null"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"false"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"true"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"1"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"1.00001"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        }

        {
        let mut query_results = Query::get_matches(r#"find {foo: =="coll"}
                                                      sort .bar asc
                                                      return .bar
                                                      limit 1"#.to_string(), &index).unwrap();

        assert_eq!(query_results.next_result().unwrap(),Some(r#"null"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        }


        assert_eq!(Ok(()), index.add(r#"{"_id":"20", "foo":"coll2", "bar":[1,1,1]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"21", "foo":"coll2", "bar":[1,1,2]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"22", "foo":"coll2", "bar":[1,2,2]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"23", "foo":"coll2", "bar":[2,2,2]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"24", "foo":"coll2", "bar":[2,1,1]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"25", "foo":"coll2", "bar":[2,1,2]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"26", "foo":"coll2", "bar":[2,3,2]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"27", "foo":"coll2", "bar":[3,4,3]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"28", "foo":"coll2", "bar":[5,4,3]}"#));
        assert_eq!(Ok(()), index.add(r#"{"_id":"29", "foo":"coll2", "bar":[5,5,5]}"#));
        
        index.flush().unwrap();

        {
        let mut query_results = Query::get_matches(r#"find {foo: =="coll2"}
                                                      sort .bar[0] asc, .bar[1] desc, .bar[2] desc
                                                      return [.bar[0], .bar[1], .bar[2]] "#.to_string(), &index).unwrap();

        
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[1,2,2]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[1,1,2]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[1,1,1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[2,3,2]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[2,2,2]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[2,1,2]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[2,1,1]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[3,4,3]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[5,5,5]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"[5,4,3]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        }



        let _ = index.add(r#"{"_id":"1", "foo":"group2", "baz": "a", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"2", "foo":"group2", "baz": "a", "bar": "b"}"#);
        let _ = index.add(r#"{"_id":"3", "foo":"group2", "baz": "b", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"4", "foo":"group2", "baz": "b", "bar": "b"}"#);
        let _ = index.add(r#"{"_id":"5", "foo":"group2", "baz": "a", "bar": "a"}"#);
        let _ = index.add(r#"{"_id":"6", "foo":"group2", "baz": "a", "bar": "c"}"#);
        let _ = index.add(r#"{"_id":"7", "foo":"group2", "baz": "b", "bar": "d"}"#);
        let _ = index.add(r#"{"_id":"8", "foo":"group2", "baz": "b", "bar": "e"}"#);
        let _ = index.add(r#"{"_id":"9", "foo":"group2", "baz": "a", "bar": "f"}"#);
        
        index.flush().unwrap();

        {
        let mut query_results = Query::get_matches(r#"find {foo: =="group2"}
                            sort .baz asc, .bar desc
                            return [.baz, .bar]
                            limit 2"#.to_string(), &index).unwrap();
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","f"]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(),Some(r#"["a","c"]"#.to_string()));
        assert_eq!(query_results.next_result().unwrap(), None);
        }
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
