
use std::str;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::swap;
use std::collections::VecDeque;
use std::iter::Iterator;
use std::usize;

use error::Error;
use index::Index;
use parser::Parser;
use json_value::JsonValue;
use filters::QueryRuntimeFilter;
use aggregates::AggregateFun;
use returnable::{Returnable, RetValue, RetScore, RetHidden, ReturnPath};
use snapshot::{Snapshot, JsonFetcher};



#[derive(Clone)]
pub struct DocResult {
    pub seq: u64,
    pub arraypath: Vec<u64>,
    pub bind_name_result: HashMap<String, Vec<String>>,
    pub scores: Vec<(f32, usize)>, // (sum of score, num matches of term)
}

impl DocResult {
    pub fn new() -> DocResult {
        DocResult {
            seq: 0,
            arraypath: Vec::new(),
            bind_name_result: HashMap::new(),
            scores: Vec::new(),
        }
    }

    pub fn add_bind_name_result(&mut self, bind_name: &str, result_key: String) {
        if let Some(ref mut result_keys) = self.bind_name_result.get_mut(bind_name) {
            result_keys.push(result_key);
            return;
        }
        self.bind_name_result
            .insert(bind_name.to_string(), vec![result_key]);
    }

    pub fn combine(&mut self, other: &mut DocResult) {
        let mut replace = HashMap::new();
        swap(&mut replace, &mut other.bind_name_result);
        for (bind_name, mut result_keys_other) in replace.into_iter() {
            if let Some(ref mut result_keys) = self.bind_name_result.get_mut(&bind_name) {
                result_keys.append(&mut result_keys_other);
                continue;
            }
            self.bind_name_result
                .insert(bind_name, result_keys_other);
        }
        self.scores.append(&mut other.scores);
    }

    pub fn add_score(&mut self, term_ordinal: usize, score: f32) {
        if term_ordinal >= self.scores.len() {
            self.scores.resize(term_ordinal + 1, (0.0, 0));
        }
        self.scores[term_ordinal].0 += score;
        self.scores[term_ordinal].1 += 1;
    }

    pub fn clone_only_seq_and_arraypath(&self) -> DocResult {
        let mut dr = DocResult::new();
        dr.seq = self.seq;
        dr.arraypath = self.arraypath.clone();
        dr
    }

    pub fn boost_scores(&mut self, boost: f32) {
        for &mut (ref mut score, ref mut _num_match) in self.scores.iter_mut() {
            *score *= boost;
        }
    }

    pub fn less(&self, other: &DocResult, mut array_depth: usize) -> bool {
        if self.seq < other.seq {
            return true;
        }
        let mut s = self.arraypath.iter();
        let mut o = other.arraypath.iter();
        loop {
            if array_depth == 0 {
                return false;
            }
            array_depth -= 1;
            if let Some(i_s) = s.next() {
                if let Some(i_o) = o.next() {
                    if i_s < i_o {
                        return true;
                    }
                } else {
                    // self cannot be less than other
                    return false;
                }
            } else {
                loop {
                    if array_depth == 0 {
                        return false;
                    }
                    array_depth -= 1;
                    if let Some(i_o) = o.next() {
                        if *i_o > 0 {
                            return true;
                        }
                    } else {
                        return true;
                    }
                }
            }
        }
    }

    // arraypaths must be the same length
    pub fn cmp(&self, other: &DocResult) -> Ordering {
        debug_assert_eq!(self.arraypath.len(), other.arraypath.len());
        match self.seq.cmp(&other.seq) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self.arraypath.cmp(&other.arraypath),
        }
    }

    pub fn increment_last(&mut self, array_depth: usize) {
        if array_depth == 0 {
            self.seq += 1;
        } else {
            self.arraypath.resize(array_depth, 0);
            if let Some(mut i) = self.arraypath.last_mut() {
                *i += 1;
            }
        }
    }
}

impl PartialEq for DocResult {
    fn eq(&self, other: &DocResult) -> bool {
        if self.seq != other.seq {
            false
        } else {
            self.arraypath == other.arraypath
        }
    }
}

impl Eq for DocResult {}

pub struct QueryScoringInfo {
    pub num_terms: usize,
    pub sum_of_idt_sqs: f32,
}

pub struct Query {}

impl Query {
    pub fn get_matches<'a>(query: &str, index: &'a Index) -> Result<QueryResults<'a>, Error> {
        if index.rocks.is_none() {
            return Err(Error::Parse("You must open the index first".to_string()));
        }

        let snapshot = index.new_snapshot();
        let mut parser = Parser::new(query, snapshot);
        let mut filter = try!(parser.build_filter());
        let mut orders = try!(parser.order_clause());
        let mut returnable = try!(parser.return_clause());
        let limit = try!(parser.limit_clause());
        try!(parser.non_ws_left());
        try!(filter.check_double_not(false));

        if filter.is_all_not() {
            return Err(Error::Parse("query cannot be made up of only logical not. Must have at \
                                     least one match clause not negated."
                                            .to_string()));
        }

        let mut ags = Vec::new();
        returnable.get_aggregate_funs(&mut ags);

        let mut has_ags = false;
        for option_ag in ags.iter() {
            if option_ag.is_some() {
                has_ags = true;
                break;
            }
        }
        let has_ordering = !orders.is_empty();

        returnable = if has_ordering && has_ags {
            return Err(Error::Parse("Cannot have aggregates and ordering in the same query"
                                        .to_string()));
        } else if has_ordering {
            returnable.take_order_for_matching_fields(&mut orders);
            if !orders.is_empty() {
                let mut vec: Vec<Box<Returnable>> = Vec::new();
                for (_key, order_info) in orders.into_iter() {
                    let order = order_info.clone();
                    match order_info.field {
                        OrderField::FetchValue(rp) => {
                            vec.push(Box::new(RetValue {
                                                  rp: rp,
                                                  ag: None,
                                                  default: order_info.default,
                                                  order_info: Some(order),
                                              }));
                        }
                        OrderField::Score => {
                            vec.push(Box::new(RetScore { order_info: Some(order) }));
                        }
                    }
                }
                Box::new(RetHidden {
                             unrendered: vec,
                             visible: returnable,
                         })
            } else {
                returnable
            }
        } else {
            returnable
        };

        if has_ags {
            // we have at least one AggregationFun. Make sure they are all set.
            for option_ag in ags.iter() {
                if option_ag.is_none() {
                    return Err(Error::Parse("Return keypaths must either all have \
                        aggregate functions, or none can them."
                                                    .to_string()));
                }
            }
        }

        let needs_ordering_and_ags = has_ags || has_ordering;

        // the input args for orders and ags are vecs where the slot is the same slot as
        // a result that the action needs to be applied to. We instead convert them
        // into several new fields with tuples of action and the slot to act on.
        // this way we don't needlesss loop over the actions where most are noops


        let mut orders = if has_ordering {
            let mut orders = Vec::new();
            let mut ordering = Vec::new();
            returnable.get_ordering(&mut ordering);
            let mut n = ordering.len();
            while let Some(option) = ordering.pop() {
                n -= 1;
                if let Some(order_info) = option {
                    orders.push((order_info, n));
                }
            }
            // order we process orders is important
            orders.sort_by_key(|&(ref order_info, ref _n)| order_info.order_to_apply);
            orders
                .into_iter()
                .map(|(order_info, n)| (order_info.order, n))
                .collect()
        } else {
            Vec::new()
        };


        let mut does_group_or_aggr = false;
        let mut aggr_inits = Vec::new();
        let mut aggr_actions = Vec::new();
        let mut aggr_finals = Vec::new();
        if has_ags {
            does_group_or_aggr = true;
            let mut n = ags.len();
            while let Some(Some((ag, user_arg))) = ags.pop() {
                n -= 1;
                if ag == AggregateFun::GroupAsc {
                    orders.push((Order::Asc, n));
                } else if ag == AggregateFun::GroupDesc {
                    orders.push((Order::Desc, n));
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
            orders.reverse();
        }

        let mut qsi = QueryScoringInfo {
            num_terms: 0,
            sum_of_idt_sqs: 0.0,
        };

        if parser.needs_scoring {
            filter.prepare_relevancy_scoring(&mut qsi);
        }

        let query_norm = if qsi.num_terms > 0 {
            1.0 / (qsi.sum_of_idt_sqs as f32)
        } else {
            0.0
        };

        Ok(QueryResults {
               filter: filter,
               doc_result_next: DocResult::new(),
               fetcher: parser.snapshot.new_json_fetcher(),
               snapshot: parser.snapshot,
               returnable: returnable,
               needs_ordering_and_ags: needs_ordering_and_ags,
               done_with_ordering_and_ags: false,
               does_group_or_aggr: does_group_or_aggr,
               orders: Some(orders),
               aggr_inits: aggr_inits,
               aggr_actions: aggr_actions,
               aggr_finals: aggr_finals,
               in_buffer: Vec::new(),
               ordered_buffer: Vec::new(),
               limit: limit,
               scoring_num_terms: qsi.num_terms,
               scoring_query_norm: query_norm,
           })
    }
}


pub struct QueryResults<'a> {
    filter: Box<QueryRuntimeFilter + 'a>,
    doc_result_next: DocResult,
    snapshot: Snapshot<'a>,
    fetcher: JsonFetcher,
    returnable: Box<Returnable>,
    needs_ordering_and_ags: bool,
    done_with_ordering_and_ags: bool,
    does_group_or_aggr: bool,
    orders: Option<Vec<(Order, usize)>>,
    aggr_inits: Vec<(fn(JsonValue) -> JsonValue, usize)>,
    aggr_actions: Vec<(fn(&mut JsonValue, JsonValue, &JsonValue), JsonValue, usize)>,
    aggr_finals: Vec<(fn(&mut JsonValue), usize)>,
    in_buffer: Vec<VecDeque<JsonValue>>,
    ordered_buffer: Vec<VecDeque<JsonValue>>,
    limit: usize,
    scoring_num_terms: usize,
    scoring_query_norm: f32,
}

impl<'a> QueryResults<'a> {
    fn compute_relevancy_score(&self, dr: &DocResult) -> f32 {
        if self.scoring_num_terms == 0 {
            return 0.0;
        }
        let mut num_terms_matched = 0;
        let mut score: f32 = 0.0;
        for &(ref total_term_score, ref num_times_term_matched) in dr.scores.iter() {
            if *num_times_term_matched > 0 {
                score += *total_term_score / (*num_times_term_matched as f32);
                num_terms_matched += 1;
            }
        }
        self.scoring_query_norm * score * (num_terms_matched as f32) /
        (self.scoring_num_terms as f32)
    }

    fn get_next_result(&mut self) -> Option<DocResult> {
        if self.done_with_ordering_and_ags {
            return None;
        }
        let result = self.filter.first_result(&self.doc_result_next);
        match result {
            Some(doc_result) => {
                self.doc_result_next.seq = doc_result.seq + 1;
                Some(doc_result)
            }
            None => None,
        }
    }

    fn get_next(&mut self) -> Option<u64> {
        if let Some(doc_result) = self.get_next_result() {
            Some(doc_result.seq)
        } else {
            None
        }
    }

    pub fn get_next_id(&mut self) -> Option<String> {
        let seq = self.get_next();
        match seq {
            Some(seq) => {
                let key = format!("V{}#._id", seq);
                match self.snapshot.get(&key.as_bytes()) {
                    // If there is an id, it's UTF-8. Strip off type leading byte
                    Some(id) => Some(id.to_utf8().unwrap()[1..].to_string()),
                    None => None,
                }
            }
            None => None,
        }
    }

    pub fn next_result(&mut self) -> Option<JsonValue> {
        if self.needs_ordering_and_ags {
            loop {
                let next = if self.done_with_ordering_and_ags {
                    None
                } else {
                    self.get_next_result()
                };
                match next {
                    Some(dr) => {
                        let score = self.compute_relevancy_score(&dr);
                        let mut results = VecDeque::new();
                        self.returnable
                            .fetch_result(&mut self.fetcher,
                                          dr.seq,
                                          score,
                                          &dr.bind_name_result,
                                          &mut results);
                        self.in_buffer.push(results);
                        if self.in_buffer.len() == self.limit {
                            self.do_ordering_and_ags();
                        }
                    }
                    None => {
                        if !self.done_with_ordering_and_ags {
                            self.do_ordering_and_ags();
                            self.done_with_ordering_and_ags = true;
                            if !self.aggr_finals.is_empty() {
                                // need to finalize the values
                                for end in self.ordered_buffer.iter_mut() {
                                    for &(ref finalize, n) in self.aggr_finals.iter() {
                                        (finalize)(&mut end[n]);
                                    }
                                }
                            }
                        }
                        if let Some(mut results) = self.ordered_buffer.pop() {
                            return Some(self.returnable.json_result(&mut results));
                        } else {
                            return None;
                        }
                    }
                }
            }
        } else {
            if self.limit == 0 {
                return None;
            }
            self.limit -= 1;
            let dr = match self.get_next_result() {
                Some(dr) => dr,
                None => return None,
            };
            let score = self.compute_relevancy_score(&dr);
            let mut results = VecDeque::new();
            self.returnable
                .fetch_result(&mut self.fetcher,
                              dr.seq,
                              score,
                              &dr.bind_name_result,
                              &mut results);
            Some(self.returnable.json_result(&mut results))
        }
    }

    fn cmp_results(orders: &Vec<(Order, usize)>,
                   a: &VecDeque<JsonValue>,
                   b: &VecDeque<JsonValue>)
                   -> Ordering {
        for &(ref order_dir, n) in orders.iter() {
            let cmp = if *order_dir != Order::Desc {
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

    fn do_ordering_and_ags(&mut self) {
        // ugh borrow check madness means this is how this must happen.
        // we need to put it back before returning.
        let orders = self.orders.take().unwrap();
        if !orders.is_empty() {
            self.in_buffer
                .sort_by(|a, b| QueryResults::cmp_results(&orders, &a, &b));
        }
        // put back
        self.orders = Some(orders);

        if !self.does_group_or_aggr {
            if self.ordered_buffer.is_empty() {
                swap(&mut self.ordered_buffer, &mut self.in_buffer);
            } else {
                //merge the ordered buffers
                let mut new_buffer = Vec::with_capacity(self.ordered_buffer.len() +
                                                        self.in_buffer.len());
                let mut option_a = self.ordered_buffer.pop();
                let mut option_b = self.in_buffer.pop();
                // take out for borrow check
                let orders = self.orders.take().unwrap();
                loop {
                    match (option_a, option_b) {
                        (Some(a), Some(b)) => {
                            match QueryResults::cmp_results(&orders, &a, &b) {
                                Ordering::Less => {
                                    new_buffer.push(b);
                                    option_a = Some(a);
                                    option_b = self.in_buffer.pop();
                                }
                                Ordering::Greater => {
                                    new_buffer.push(a);
                                    option_a = self.ordered_buffer.pop();
                                    option_b = Some(b);

                                }
                                Ordering::Equal => {
                                    new_buffer.push(a);
                                    new_buffer.push(b);
                                    option_a = self.ordered_buffer.pop();
                                    option_b = self.in_buffer.pop();
                                }
                            }
                            if new_buffer.len() >= self.limit {
                                self.ordered_buffer.clear();
                                self.in_buffer.clear();
                                new_buffer.truncate(self.limit);
                                break;
                            }
                        }
                        (Some(a), None) => {
                            new_buffer.push(a);
                            if new_buffer.len() == self.limit {
                                break;
                            }
                            while let Some(a) = self.ordered_buffer.pop() {
                                new_buffer.push(a);
                                if new_buffer.len() == self.limit {
                                    break;
                                }
                            }
                            break;
                        }
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
                        }
                        (None, None) => break,
                    }
                }
                // put back
                self.orders = Some(orders);

                new_buffer.reverse();
                swap(&mut self.ordered_buffer, &mut new_buffer);
            }
            return;
        }


        //merge the ordered buffers
        let mut new_buffer = Vec::with_capacity(self.ordered_buffer.len() + self.in_buffer.len());
        let mut option_old = self.ordered_buffer.pop();
        let mut option_new = self.in_buffer.pop();
        // take out for borrow check
        let orders = self.orders.take().unwrap();
        loop {
            match (option_old, option_new) {
                (Some(mut old), Some(mut new)) => {
                    match QueryResults::cmp_results(&orders, &old, &new) {
                        Ordering::Less => {
                            for &(ref init, n) in self.aggr_inits.iter() {
                                // we can't swap out a value of new directly, so this lets us
                                // without shifting or cloning values, both of which can be
                                // expensive
                                let mut new_n = JsonValue::Null;
                                swap(&mut new_n, &mut new[n]);
                                new[n] = (init)(new_n);
                            }
                            //push back old value into ordered_buffer,
                            //then use new value as old value.
                            self.ordered_buffer.push(old);
                            option_old = Some(new);
                            option_new = self.in_buffer.pop();
                        }
                        Ordering::Greater => {
                            new_buffer.push(old);
                            option_old = self.ordered_buffer.pop();
                            option_new = Some(new);
                        }
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
                        self.ordered_buffer.clear();
                        self.in_buffer.clear();
                        break;
                    }
                }
                (Some(old), None) => {
                    new_buffer.push(old);
                    if new_buffer.len() == self.limit {
                        break;
                    }
                    while let Some(old) = self.ordered_buffer.pop() {
                        new_buffer.push(old);
                        if new_buffer.len() == self.limit {
                            break;
                        }
                    }
                    break;
                }
                (None, Some(mut new)) => {
                    for &(ref init, n) in self.aggr_inits.iter() {
                        // we can't swap out a value of new directly, so this lets us
                        // without shifting or cloning values, both of which can be
                        // expensive
                        let mut new_n = JsonValue::Null;
                        swap(&mut new_n, &mut new[n]);
                        new[n] = (init)(new_n);
                    }
                    option_old = Some(new);
                    option_new = self.in_buffer.pop();
                }
                (None, None) => break,
            }
        }
        // put back
        self.orders = Some(orders);

        new_buffer.reverse();
        swap(&mut self.ordered_buffer, &mut new_buffer);
    }
}

impl<'a> Iterator for QueryResults<'a> {
    type Item = JsonValue;

    fn next(&mut self) -> Option<JsonValue> {
        self.next_result()
    }
}

#[derive(PartialEq, Eq, Clone)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Clone)]
pub enum OrderField {
    FetchValue(ReturnPath),
    Score,
}

#[derive(Clone)]
pub struct OrderInfo {
    pub field: OrderField,
    pub order_to_apply: usize,
    pub order: Order,
    pub default: JsonValue,
}



#[cfg(test)]
mod tests {
    extern crate rustc_serialize;

    use super::Query;

    use index::{Index, OpenOptions, Batch};

    #[test]
    fn test_query_hello_world() {
        let dbname = "target/tests/querytestdbhelloworld";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = Batch::new();
        let _ = index.add(r#"{"_id": "foo", "hello": "world"}"#, &mut batch);
        index.flush(batch).unwrap();

        let mut query_results = Query::get_matches(r#"find {hello:=="world"}"#, &index).unwrap();
        println!("query results: {:?}", query_results.get_next_id());
    }

    #[test]
    fn test_query_more_docs() {
        let dbname = "target/tests/querytestdbmoredocs";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        let mut batch = Batch::new();
        for ii in 1..100 {
            let data = ((ii % 25) + 97) as u8 as char;
            let _ = index.add(&format!(r#"{{"_id":"{}", "data": "{}"}}"#, ii, data),
                              &mut batch);
        }
        index.flush(batch).unwrap();

        let mut query_results = Query::get_matches(r#"find {data: == "u"}"#, &index).unwrap();
        loop {
            match query_results.get_next_id() {
                Some(result) => println!("result: {}", result),
                None => break,
            }
        }
    }
}
