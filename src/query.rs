
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
use parser::Parser;
use json_value::JsonValue;
use filters::QueryRuntimeFilter;


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

#[derive(PartialEq, Eq, Clone)]
pub enum Sort {
    Asc,
    Desc,
}

pub struct SortInfo {
    pub kb: KeyBuilder,
    pub sort: Sort,
    pub default: JsonValue,
}


pub trait Returnable {
    fn fetch_result(&self, iter: &mut DBIterator, seq: u64,
                    bind_var_keys: &HashMap<String, String>,
                    result: &mut VecDeque<JsonValue>) -> Result<(), Error>;

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>);

    fn get_sorting(&self, sorts: &mut Vec<Option<Sort>>);

    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>);

    fn write_result(&self, results: &mut VecDeque<JsonValue>,
                    write: &mut Write) -> Result<(), Error>;
}

pub struct RetObject {
    pub fields: Vec<(String, Box<Returnable>)>,
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


pub struct RetArray {
    pub slots: Vec<Box<Returnable>>,
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



pub struct RetHidden {
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

pub struct RetLiteral {
    pub json: JsonValue,
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
    pub kb: KeyBuilder,
    pub ag: Option<(AggregateFun, JsonValue)>,
    pub default: JsonValue,
    pub sort: Option<Sort>,
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

    use super::Query;

    use index::{Index, OpenOptions};

    

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
