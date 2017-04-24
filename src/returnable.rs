
use std::str;
use std::collections::HashMap;
use std::collections::VecDeque;

use key_builder::KeyBuilder;
use json_value::JsonValue;
use query::SortInfo;
use snapshot::JsonFetcher;
use aggregates::AggregateFun;

#[derive(Clone)]
pub enum PathSegment {
    ObjectKey(String),
    Array(u64),
    ArrayAll,
}

#[derive(Clone)]
pub struct ReturnPath {
    path: Vec<PathSegment>,
}

impl ReturnPath {
    pub fn new() -> ReturnPath {
        ReturnPath { path: Vec::new() }
    }

    pub fn push_object_key(&mut self, key: String) {
        self.path.push(PathSegment::ObjectKey(key));
    }

    pub fn push_array(&mut self, index: u64) {
        self.path.push(PathSegment::Array(index));
    }

    pub fn push_array_all(&mut self) {
        self.path.push(PathSegment::ArrayAll);
    }

    pub fn to_key(&self) -> String {
        let mut key = String::new();
        for seg in self.path.iter() {
            match seg {
                &PathSegment::ObjectKey(ref str) => {
                    key.push('.');
                    for cc in str.chars() {
                        // Escape chars that conflict with delimiters
                        if "\\$.".contains(cc) {
                            key.push('\\');
                        }
                        key.push(cc);
                    }
                }
                &PathSegment::Array(ref i) => {
                    key.push('$');
                    key.push_str(&i.to_string());
                }
                &PathSegment::ArrayAll => {
                    key.push_str("$*");
                }
            }
        }
        key
    }

    pub fn nth(&self, i: usize) -> Option<&PathSegment> {
        if self.path.len() <= i {
            None
        } else {
            Some(&self.path[i])
        }
    }
}



/// Returnables are created from parsing the return statement in queries.
/// They nest inside of each other, with the outermost typically being a RetObject or RetArray.
pub trait Returnable {
    /// When a match is found, information about the match is passed to outer most Returnable
    /// and then each nested Returnable will fetch information about the document (fields or
    /// scores or bind variables etc) and convert them to JsonValues and add them to the result
    /// VecDeque.
    fn fetch_result(&self,
                    fetcher: &mut JsonFetcher,
                    seq: u64,
                    score: f32,
                    bind_var_keys: &HashMap<String, Vec<String>>,
                    result: &mut VecDeque<JsonValue>);

    /// If aggregates are used each Returnable needs to return information about the
    /// aggregate function it's using and the default value.
    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>);

    /// If a query has a sort clause then we want to match the fields being sorted with
    /// fields being returned. We pass the sorting info by the path of the sorted fields
    /// or scores and Returnables that have the same path will take the sort
    /// information. Any fields not matching a returnable are then added to special hidden
    /// Returnable (RetHidden) which fetches those fields for sorting but not rendered or
    /// returned.
    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>);

    /// Each Returnable will return the sorting direction in the same slot as the returnable
    /// so that later after fetching they will be sorted by QueryResults after fetching but
    /// converting to the final json result.
    fn get_sorting(&mut self, sorts: &mut Vec<Option<SortInfo>>);

    /// This is the final step of a Returnable. The previous fetched JsonValues are now
    /// rendered with other ornamental json elements.
    fn json_result(&self, results: &mut VecDeque<JsonValue>) -> JsonValue;
}

/// A static Json Object the can contain another number of fields and nested returnables.
pub struct RetObject {
    pub fields: Vec<(String, Box<Returnable>)>,
}

impl Returnable for RetObject {
    fn fetch_result(&self,
                    fetcher: &mut JsonFetcher,
                    seq: u64,
                    score: f32,
                    bind_var_keys: &HashMap<String, Vec<String>>,
                    result: &mut VecDeque<JsonValue>) {
        for &(ref _key, ref field) in self.fields.iter() {
            field.fetch_result(fetcher, seq, score, bind_var_keys, result);
        }
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        for &(ref _key, ref field) in self.fields.iter() {
            field.get_aggregate_funs(funs);
        }
    }

    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
        for &mut (ref _key, ref mut field) in self.fields.iter_mut() {
            field.take_sort_for_matching_fields(map);
        }
    }

    fn get_sorting(&mut self, sorts: &mut Vec<Option<SortInfo>>) {
        for &mut (ref mut _key, ref mut field) in self.fields.iter_mut() {
            field.get_sorting(sorts);
        }
    }

    fn json_result(&self, results: &mut VecDeque<JsonValue>) -> JsonValue {
        let mut vec = Vec::with_capacity(self.fields.len());
        for &(ref key, ref returnable) in self.fields.iter() {
            vec.push((key.clone(), returnable.json_result(results)));
        }
        JsonValue::Object(vec)
    }
}

/// A static Json array the can contain another number of nested Returnables.
pub struct RetArray {
    pub slots: Vec<Box<Returnable>>,
}

impl Returnable for RetArray {
    fn fetch_result(&self,
                    fetcher: &mut JsonFetcher,
                    seq: u64,
                    score: f32,
                    bind_var_keys: &HashMap<String, Vec<String>>,
                    result: &mut VecDeque<JsonValue>) {
        for ref slot in self.slots.iter() {
            slot.fetch_result(fetcher, seq, score, bind_var_keys, result);
        }
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        for ref slot in self.slots.iter() {
            slot.get_aggregate_funs(funs);
        }
    }

    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
        for slot in self.slots.iter_mut() {
            slot.take_sort_for_matching_fields(map);
        }
    }

    fn get_sorting(&mut self, sorts: &mut Vec<Option<SortInfo>>) {
        for ref mut slot in self.slots.iter_mut() {
            slot.get_sorting(sorts);
        }
    }

    fn json_result(&self, results: &mut VecDeque<JsonValue>) -> JsonValue {
        let mut vec = Vec::with_capacity(self.slots.len());
        for slot in self.slots.iter() {
            vec.push(slot.json_result(results));
        }
        JsonValue::Array(vec)
    }
}

/// A special returnable that only fetches values for later sorting but never renders
/// them back to the caller.
pub struct RetHidden {
    pub unrendered: Vec<Box<Returnable>>,
    pub visible: Box<Returnable>,
}

impl Returnable for RetHidden {
    fn fetch_result(&self,
                    fetcher: &mut JsonFetcher,
                    seq: u64,
                    score: f32,
                    bind_var_keys: &HashMap<String, Vec<String>>,
                    result: &mut VecDeque<JsonValue>) {
        for ref unrendered in self.unrendered.iter() {
            unrendered.fetch_result(fetcher, seq, score, bind_var_keys, result);
        }

        self.visible
            .fetch_result(fetcher, seq, score, bind_var_keys, result);
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        self.visible.get_aggregate_funs(funs);
    }

    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
        self.visible.take_sort_for_matching_fields(map);
    }

    fn get_sorting(&mut self, sorts: &mut Vec<Option<SortInfo>>) {
        for ref mut unrendered in self.unrendered.iter_mut() {
            unrendered.get_sorting(sorts);
        }

        self.visible.get_sorting(sorts);
    }

    fn json_result(&self, results: &mut VecDeque<JsonValue>) -> JsonValue {
        for _n in 0..self.unrendered.len() {
            // we already sorted at this point, now discard the values
            results.pop_front();
        }
        self.visible.json_result(results)
    }
}

/// A literal JsonValue. Number, String, Null, True or False. Just in case the query
/// wants to return something that doesn't come from a document.
pub struct RetLiteral {
    pub json: JsonValue,
}

impl Returnable for RetLiteral {
    fn fetch_result(&self,
                    _fetcher: &mut JsonFetcher,
                    _seq: u64,
                    _score: f32,
                    _bind_var_keys: &HashMap<String, Vec<String>>,
                    _result: &mut VecDeque<JsonValue>) {
    }

    fn get_aggregate_funs(&self, _funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        //noop
    }

    fn take_sort_for_matching_fields(&mut self, _map: &mut HashMap<String, SortInfo>) {
        //noop
    }

    fn get_sorting(&mut self, _sorts: &mut Vec<Option<SortInfo>>) {
        //noop
    }

    fn json_result(&self, _results: &mut VecDeque<JsonValue>) -> JsonValue {
        self.json.clone()
    }
}

/// A value from a document. It knows the path it wants to fetch and loads the value from the
/// stored original document.
pub struct RetValue {
    pub rp: ReturnPath,
    pub ag: Option<(AggregateFun, JsonValue)>,
    pub default: JsonValue,
    pub sort_info: Option<SortInfo>,
}



impl Returnable for RetValue {
    fn fetch_result(&self,
                    fetcher: &mut JsonFetcher,
                    seq: u64,
                    _score: f32,
                    _bind_var_keys: &HashMap<String, Vec<String>>,
                    result: &mut VecDeque<JsonValue>) {
        if Some((AggregateFun::Count, JsonValue::Null)) == self.ag {
            //don't fetch anything for count(). just stick in a null
            result.push_back(JsonValue::Null);
        }
        let mut kb = KeyBuilder::new();
        if let Some(json) = fetcher.fetch(seq, &mut kb, &self.rp) {
            result.push_back(json);
        } else {
            result.push_back(self.default.clone());
        }
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        funs.push(self.ag.clone());
    }

    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
        self.sort_info = map.remove(&self.rp.to_key());
    }

    fn get_sorting(&mut self, sorts: &mut Vec<Option<SortInfo>>) {
        sorts.push(self.sort_info.take());
    }

    fn json_result(&self, results: &mut VecDeque<JsonValue>) -> JsonValue {
        if let Some(json) = results.pop_front() {
            json
        } else {
            panic!("missing result!");
        }
    }
}

/// A bind variable. If a bind variable was matched it will be fetched then it's path is
/// added to the bind_var_keys passed into fetch_result(). This will load the values from the
/// original document and return it.
pub struct RetBind {
    pub bind_name: String,
    pub extra_rp: ReturnPath,
    pub ag: Option<(AggregateFun, JsonValue)>,
    pub default: JsonValue,
    pub sort_info: Option<SortInfo>,
}

impl Returnable for RetBind {
    fn fetch_result(&self,
                    fetcher: &mut JsonFetcher,
                    seq: u64,
                    _score: f32,
                    bind_var_keys: &HashMap<String, Vec<String>>,
                    result: &mut VecDeque<JsonValue>) {

        if let Some(value_keys) = bind_var_keys.get(&self.bind_name) {
            let mut array = Vec::with_capacity(value_keys.len());
            for base_key in value_keys {
                let mut kb = KeyBuilder::new();
                kb.parse_value_key_path_only(KeyBuilder::value_key_path_only_from_str(&base_key));

                if let Some(json) = fetcher.fetch(seq, &mut kb, &self.extra_rp) {
                    array.push(json);
                } else {
                    array.push(self.default.clone());
                }
            }
            result.push_back(JsonValue::Array(array));
        } else {
            result.push_back(JsonValue::Array(vec![self.default.clone()]));
        }
    }

    fn get_aggregate_funs(&self, funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        funs.push(self.ag.clone());
    }

    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
        self.sort_info = map.remove(&(self.bind_name.to_string() + &self.extra_rp.to_key()));
    }

    fn get_sorting(&mut self, sorts: &mut Vec<Option<SortInfo>>) {
        sorts.push(self.sort_info.take());
    }

    fn json_result(&self, results: &mut VecDeque<JsonValue>) -> JsonValue {
        if let Some(json) = results.pop_front() {
            json
        } else {
            panic!("missing bind result!");
        }
    }
}

/// Returns a relevency score for a match.
pub struct RetScore {
    pub sort_info: Option<SortInfo>,
}

impl Returnable for RetScore {
    fn fetch_result(&self,
                    _fetcher: &mut JsonFetcher,
                    _seq: u64,
                    score: f32,
                    _bind_var_keys: &HashMap<String, Vec<String>>,
                    result: &mut VecDeque<JsonValue>) {
        result.push_back(JsonValue::Number(score as f64));
    }

    fn get_aggregate_funs(&self, _funs: &mut Vec<Option<(AggregateFun, JsonValue)>>) {
        // noop
    }

    fn take_sort_for_matching_fields(&mut self, map: &mut HashMap<String, SortInfo>) {
        self.sort_info = map.remove("score()");
    }

    fn get_sorting(&mut self, sorts: &mut Vec<Option<SortInfo>>) {
        sorts.push(self.sort_info.take());
    }

    fn json_result(&self, results: &mut VecDeque<JsonValue>) -> JsonValue {
        if let Some(json) = results.pop_front() {
            json
        } else {
            panic!("missing score result!");
        }
    }
}
