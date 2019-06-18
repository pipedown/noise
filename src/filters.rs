extern crate varint;

use std::{self, mem, str};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::io::Cursor;
use std::rc::Rc;

use self::varint::VarintRead;

use error::Error;
use key_builder::KeyBuilder;
use query::{DocResult, QueryScoringInfo};
use json_value::JsonValue;
use snapshot::{Snapshot, DocResultIterator, Scorer, JsonFetcher, AllDocsIterator};
use rocksdb::{self, DBIterator, IteratorMode};

pub trait QueryRuntimeFilter {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult>;
    fn next_result(&mut self) -> Option<DocResult>;
    fn prepare_relevancy_scoring(&mut self, qsi: &mut QueryScoringInfo);

    /// returns error is a double negation is detected
    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error>;

    /// return true if filter or all subfilters are NotFilters
    fn is_all_not(&self) -> bool;
}


#[derive(PartialEq)]
pub enum RangeOperator {
    Inclusive(f64),
    Exclusive(f64),
    // For booleans and null only exact match makes sense, hence no inclusive/exclusive
    // boundaries are needed
    True,
    False,
    Null,
}



pub struct AllDocsFilter {
    iter: AllDocsIterator,
}

impl AllDocsFilter {
    pub fn new(snapshot: &Snapshot) -> AllDocsFilter {
        AllDocsFilter { iter: snapshot.new_all_docs_iterator() }
    }
}

impl QueryRuntimeFilter for AllDocsFilter {
    fn first_result(&mut self, _start: &DocResult) -> Option<DocResult> {
        self.next_result()
    }

    fn next_result(&mut self) -> Option<DocResult> {
        if let Some(mut dr) = self.iter.next() {
            dr.add_score(1, 1.0);
            Some(dr)
        } else {
            None
        }
    }

    fn prepare_relevancy_scoring(&mut self, qsi: &mut QueryScoringInfo) {
        qsi.num_terms += 1;
        qsi.sum_of_idt_sqs += 1.0;
    }

    fn check_double_not(&self, _parent_is_neg: bool) -> Result<(), Error> {
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        false
    }
}

pub struct StemmedWordFilter {
    iter: DocResultIterator,
    scorer: Scorer,
}

impl StemmedWordFilter {
    pub fn new(snapshot: &Snapshot,
               stemmed_word: &str,
               kb: &KeyBuilder,
               boost: f32)
               -> StemmedWordFilter {
        StemmedWordFilter {
            iter: snapshot.new_term_doc_result_iterator(stemmed_word, kb),
            scorer: snapshot.new_scorer(stemmed_word, kb, boost),
        }
    }
}

impl QueryRuntimeFilter for StemmedWordFilter {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        self.iter.advance_gte(start);
        self.next_result()
    }

    fn next_result(&mut self) -> Option<DocResult> {
        if let Some((mut dr, pos)) = self.iter.next() {
            if self.scorer.should_score() {
                let count = pos.positions().len();
                self.scorer.add_match_score(count as u32, &mut dr);
            }
            Some(dr)
        } else {
            None
        }
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        self.scorer.init(&mut qsi);
    }

    fn check_double_not(&self, _parent_is_neg: bool) -> Result<(), Error> {
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        false
    }
}

/// This is not a QueryRuntimeFilter but it imitates one. Instead of returning just a DocResult
/// it also return a vector of word positions, each being a instance of the word occurance
pub struct StemmedWordPosFilter {
    iter: DocResultIterator,
    scorer: Scorer,
}

impl StemmedWordPosFilter {
    pub fn new(snapshot: &Snapshot,
               stemmed_word: &str,
               kb: &KeyBuilder,
               boost: f32)
               -> StemmedWordPosFilter {
        StemmedWordPosFilter {
            iter: snapshot.new_term_doc_result_iterator(stemmed_word, kb),
            scorer: snapshot.new_scorer(&stemmed_word, &kb, boost),
        }
    }

    fn first_result(&mut self, start: &DocResult) -> Option<(DocResult, Vec<u32>)> {
        self.iter.advance_gte(start);
        self.next_result()
    }

    fn next_result(&mut self) -> Option<(DocResult, Vec<u32>)> {
        if let Some((mut dr, pos)) = self.iter.next() {
            let positions = pos.positions();
            if self.scorer.should_score() {
                let count = positions.len();
                self.scorer.add_match_score(count as u32, &mut dr);
            }
            Some((dr, positions))
        } else {
            None
        }
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        self.scorer.init(&mut qsi);
    }
}

pub struct StemmedPhraseFilter {
    filters: Vec<StemmedWordPosFilter>,
}

impl StemmedPhraseFilter {
    pub fn new(filters: Vec<StemmedWordPosFilter>) -> StemmedPhraseFilter {
        assert!(filters.len() > 0);
        StemmedPhraseFilter { filters: filters }
    }

    fn result(&mut self, base: Option<(DocResult, Vec<u32>)>) -> Option<DocResult> {
        // this is the number of matches left before all terms match and we can return a result
        let mut matches_left = self.filters.len() - 1;

        if base.is_none() {
            return None;
        }
        let (mut base_result, mut base_positions) = base.unwrap();

        if matches_left == 0 {
            return Some(base_result);
        }

        let mut current_filter = 0;
        loop {
            current_filter += 1;
            if current_filter == self.filters.len() {
                current_filter = 0;
            }

            let next = self.filters[current_filter].first_result(&base_result);

            if next.is_none() {
                return None;
            }
            let (next_result, next_positions) = next.unwrap();

            if base_result == next_result {
                let mut new_positions = Vec::new();
                for &pos in next_positions.iter() {
                    if let Ok(_) = base_positions.binary_search(&(pos.saturating_sub(1))) {
                        new_positions.push(pos);
                    }
                }
                if new_positions.len() > 0 {
                    // we have valus that survive! reassign back to base_positions
                    base_positions = new_positions;
                    matches_left -= 1;

                    if matches_left == 0 {
                        return Some(base_result);
                    }
                } else {
                    // we didn't match on phrase, so get next_result from first filter
                    current_filter = 0;
                    let next = self.filters[current_filter].next_result();
                    if next.is_none() {
                        return None;
                    }
                    let (next_result, next_positions) = next.unwrap();
                    base_result = next_result;
                    base_positions = next_positions;

                    matches_left = self.filters.len() - 1;
                }
            } else {
                // we didn't match on next_result, so get first_result at next_result on
                // 1st filter.
                current_filter = 0;
                let next = self.filters[current_filter].first_result(&next_result);
                if next.is_none() {
                    return None;
                }
                let (next_result, next_positions) = next.unwrap();
                base_result = next_result;
                base_positions = next_positions;

                matches_left = self.filters.len() - 1;
            }
        }
    }
}


impl QueryRuntimeFilter for StemmedPhraseFilter {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        let base_result = self.filters[0].first_result(start);
        self.result(base_result)
    }

    fn next_result(&mut self) -> Option<DocResult> {
        let base_result = self.filters[0].next_result();
        self.result(base_result)
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        for f in self.filters.iter_mut() {
            f.prepare_relevancy_scoring(&mut qsi);
        }
    }

    fn check_double_not(&self, _parent_is_neg: bool) -> Result<(), Error> {
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        false
    }
}


pub struct ExactMatchFilter {
    iter: DBIterator,
    filter: StemmedPhraseFilter,
    kb: KeyBuilder,
    phrase: String,
    case_sensitive: bool,
    term_ordinal: Option<usize>,
}

impl ExactMatchFilter {
    pub fn new(snapshot: &Snapshot,
               filter: StemmedPhraseFilter,
               kb: KeyBuilder,
               phrase: String,
               case_sensitive: bool)
               -> ExactMatchFilter {
        ExactMatchFilter {
            iter: snapshot.new_iterator(),
            filter: filter,
            kb: kb,
            phrase: if case_sensitive {
                phrase
            } else {
                phrase.to_lowercase()
            },
            case_sensitive: case_sensitive,
            term_ordinal: None,
        }
    }

    fn check_exact(&mut self, mut dr: DocResult) -> Option<DocResult> {
        loop {
            let value_key = self.kb.kp_value_key_from_doc_result(&dr);

            self.iter
                .set_mode(IteratorMode::From(value_key.as_bytes(), rocksdb::Direction::Forward));

            if let Some((key, value)) = self.iter.next() {
                debug_assert!(key.starts_with(value_key.as_bytes())); // must always be true!
                if let JsonValue::String(string) = JsonFetcher::bytes_to_json_value(&*value) {
                    let matches = if self.case_sensitive {
                        self.phrase == string
                    } else {
                        self.phrase == string.to_lowercase()
                    };
                    if matches {
                        if self.term_ordinal.is_some() {
                            dr.add_score(self.term_ordinal.unwrap(), 1.0);
                        }
                        return Some(dr);
                    } else {
                        if let Some(next) = self.filter.next_result() {
                            dr = next;
                            // continue looping
                        } else {
                            return None;
                        }
                    }
                } else {
                    panic!("Not a string, wtf!");
                }
            } else {
                panic!("Couldn't find value, hulk smash!");
            }
        }
    }
}

impl QueryRuntimeFilter for ExactMatchFilter {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        if let Some(dr) = self.filter.first_result(start) {
            self.check_exact(dr)
        } else {
            None
        }
    }

    fn next_result(&mut self) -> Option<DocResult> {
        if let Some(dr) = self.filter.next_result() {
            self.check_exact(dr)
        } else {
            None
        }
    }

    fn prepare_relevancy_scoring(&mut self, qsi: &mut QueryScoringInfo) {
        // we score these as binary. Either they have a value of 1 or nothing.
        self.term_ordinal = Some(qsi.num_terms);
        qsi.num_terms += 1;
        qsi.sum_of_idt_sqs += 1.0;
    }

    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error> {
        self.filter.check_double_not(parent_is_neg)
    }

    fn is_all_not(&self) -> bool {
        self.filter.is_all_not()
    }
}

pub struct RangeFilter {
    iter: DBIterator,
    kb: KeyBuilder,
    min: Option<RangeOperator>,
    max: Option<RangeOperator>,
    keypath: String,
    term_ordinal: Option<usize>,
}

impl RangeFilter {
    pub fn new(snapshot: &Snapshot,
               kb: KeyBuilder,
               min: Option<RangeOperator>,
               max: Option<RangeOperator>)
               -> RangeFilter {
        RangeFilter {
            iter: snapshot.new_iterator(),
            kb: kb,
            min: min,
            max: max,
            // The keypath we use to seek to the correct key within RocksDB
            keypath: String::new(),
            term_ordinal: None,
        }
    }
}

impl QueryRuntimeFilter for RangeFilter {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        let mut value_key = {
            // `min` and `max` have the save type, so picking one is OK
            let range_operator = self.min.as_ref().or(self.max.as_ref()).unwrap();
            match range_operator {
                &RangeOperator::Inclusive(_) |
                &RangeOperator::Exclusive(_) => self.kb.number_key(start.seq),
                &RangeOperator::True => self.kb.bool_null_key('T', start.seq),
                &RangeOperator::False => self.kb.bool_null_key('F', start.seq),
                &RangeOperator::Null => self.kb.bool_null_key('N', start.seq),
            }
        };
        // NOTE vmx 2017-04-13: Iterating over keys is really similar to the
        // `DocResultIterator` in `snapshot.rs`. It should probablly be unified.
        self.iter
            .set_mode(IteratorMode::From(value_key.as_bytes(), rocksdb::Direction::Forward));
        KeyBuilder::truncate_to_kp_word(&mut value_key);
        self.keypath = value_key;
        self.next_result()
    }

    fn next_result(&mut self) -> Option<DocResult> {
        while let Some((key, value)) = self.iter.next() {
            if !key.starts_with(self.keypath.as_bytes()) {
                // we passed the key path we are interested in. nothing left to do
                return None;
            }

            let key_str = unsafe { str::from_utf8_unchecked(&key) };

            // The key already matched, hence it's a valid doc result. Return it.
            if self.min == Some(RangeOperator::True) || self.min == Some(RangeOperator::False) ||
               self.min == Some(RangeOperator::Null) {
                let mut dr = KeyBuilder::parse_doc_result_from_kp_word_key(&key_str);
                if self.term_ordinal.is_some() {
                    dr.add_score(self.term_ordinal.unwrap(), 1.0);
                }
                return Some(dr);
            }
            // Else it's a range query on numbers

            let number = unsafe {
                let array = *(value[..].as_ptr() as *const [_; 8]);
                mem::transmute::<[u8; 8], f64>(array)
            };

            let min_condition = match self.min {
                Some(RangeOperator::Inclusive(min)) => number >= min,
                Some(RangeOperator::Exclusive(min)) => number > min,
                // No condition was given => it always matches
                None => true,
                _ => panic!("Can't happen, it returns early on the other types"),
            };
            let max_condition = match self.max {
                Some(RangeOperator::Inclusive(max)) => number <= max,
                Some(RangeOperator::Exclusive(max)) => number < max,
                // No condition was given => it always matches
                None => true,
                _ => panic!("Can't happen, it returns early on the other types"),
            };

            if min_condition && max_condition {
                let mut dr = KeyBuilder::parse_doc_result_from_kp_word_key(&key_str);
                if self.term_ordinal.is_some() {
                    dr.add_score(self.term_ordinal.unwrap(), 1.0);
                }
                return Some(dr);
            }
            // Else: No match => KKeep looping and move on to the next key
        }
        None
    }

    // TODO vmx 2017-04-13: Scoring is not implemented yet
    fn prepare_relevancy_scoring(&mut self, qsi: &mut QueryScoringInfo) {
        // we score these as binary. Either they have a value of 1 or nothing.
        self.term_ordinal = Some(qsi.num_terms);
        qsi.num_terms += 1;
        qsi.sum_of_idt_sqs += 1.0;
    }

    fn check_double_not(&self, _parent_is_neg: bool) -> Result<(), Error> {
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        false
    }
}

pub struct BboxFilter<'a> {
    snapshot: Rc<Snapshot<'a>>,
    iter: Option<DBIterator>,
    kb: KeyBuilder,
    bbox: Vec<u8>,
    term_ordinal: Option<usize>,
}

impl<'a> BboxFilter<'a> {
    pub fn new(snapshot: Rc<Snapshot<'a>>,
               kb: KeyBuilder,
               bbox: [f64; 4])
               -> BboxFilter<'a> {
        let mut bbox_vec = Vec::with_capacity(32);
        bbox_vec.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bbox[0]) });
        bbox_vec.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bbox[2]) });
        bbox_vec.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bbox[1]) });
        bbox_vec.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bbox[3]) });

        BboxFilter {
            snapshot: snapshot,
            iter: None,
            kb: kb,
            bbox: bbox_vec,
            term_ordinal: None,
        }
    }

    /// Function to deserialize the Arraypaths
    fn from_u8_slice(slice: &[u8]) -> Vec<u64> {
        let u64_slice = unsafe {
            std::slice::from_raw_parts(slice.as_ptr() as *const u64, slice.len() / 8)
        };
        u64_slice.to_vec()
    }
}

impl<'a> QueryRuntimeFilter for BboxFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        let query = self.kb.rtree_query_key(start.seq, std::u64::MAX, &self.bbox);
        self.iter = Some(self.snapshot.new_rtree_iterator(&query));
        self.next_result()
    }

    fn next_result(&mut self) -> Option<DocResult> {
        let iter = self.iter.as_mut().unwrap();
        if let Some((key, value)) = iter.next() {
            let mut vec = Vec::with_capacity(key.len());
            vec.extend_from_slice(&key);
            let mut read = Cursor::new(vec);
            let key_len = read.read_unsigned_varint_32().unwrap();
            let offset = read.position() as usize;

            let iid = unsafe {
                let array = *(key[offset + key_len as usize..].as_ptr() as *const [_; 8]);
                mem::transmute::<[u8; 8], u64>(array)
            };

            let mut dr = DocResult::new();
            dr.seq = iid;
            dr.arraypath = BboxFilter::from_u8_slice(&value);
            if self.term_ordinal.is_some() {
                dr.add_score(self.term_ordinal.unwrap(), 1.0);
            }
            Some(dr)
        } else {
            None
        }
    }

    fn prepare_relevancy_scoring(&mut self, qsi: &mut QueryScoringInfo) {
        // We score these as binary. Either they have a value of 1 or nothing.
        self.term_ordinal = Some(qsi.num_terms);
        qsi.num_terms += 1;
        qsi.sum_of_idt_sqs += 1.0;
    }

    fn check_double_not(&self, _parent_is_neg: bool) -> Result<(), Error> {
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        false
    }
}


pub struct DistanceFilter {
    filters: Vec<StemmedWordPosFilter>,
    current_filter: usize,
    distance: u32,
}

impl DistanceFilter {
    pub fn new(filters: Vec<StemmedWordPosFilter>, distance: u32) -> DistanceFilter {
        DistanceFilter {
            filters: filters,
            current_filter: 0,
            distance: distance,
        }
    }

    fn result(&mut self, base: Option<(DocResult, Vec<u32>)>) -> Option<DocResult> {
        // yes this code complex. I tried to break it up, but it wants to be like this.

        // this is the number of matches left before all terms match and we can return a result
        let mut matches_left = self.filters.len() - 1;

        if base.is_none() {
            return None;
        }
        let (mut base_result, positions) = base.unwrap();

        // This contains tuples of word postions and the filter they came from,
        // sorted by word position.
        let mut base_positions: Vec<(u32, usize)> = positions
            .iter()
            .map(|pos| (*pos, self.current_filter))
            .collect();

        // distance is number of words between searched words.
        // add one to make calculating difference easier since abs(posa - posb) == distance + 1
        let dis = self.distance + 1;
        loop {
            self.current_filter += 1;
            if self.current_filter == self.filters.len() {
                self.current_filter = 0;
            }

            let next = self.filters[self.current_filter].first_result(&base_result);

            if next.is_none() {
                return None;
            }
            let (next_result, next_positions) = next.unwrap();

            if base_result != next_result {
                // not same field, next_result becomes base_result.
                base_result = next_result;
                base_positions = next_positions
                    .iter()
                    .map(|pos| (*pos, self.current_filter))
                    .collect();

                matches_left = self.filters.len() - 1;
                continue;
            }
            // so we are in the same field. Now to check the proximity of the values from the
            // next result to previous results.

            // new_positions_map will accept positions within range of pos. But only if all
            // positions that can be are within range. We use the sorted map so we can add
            // the same positions multiple times and it's a noop.
            let mut new_positions_map = BTreeMap::new();
            for &pos in next_positions.iter() {
                // coud these lines be any longer? No they could not.
                let sub = pos.saturating_sub(dis); // underflows othewises
                let start = match base_positions.binary_search_by_key(&(sub), |&(pos2, _)| pos2) {
                    Ok(start) => start,
                    Err(start) => start,
                };

                let end = match base_positions.binary_search_by_key(&(pos + dis),
                                                                    |&(pos2, _)| pos2) {
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
                    return Some(base_result);
                } else {
                    continue;
                }
            }
            // we didn't match on next_result, so get next_result on current filter
            let next = self.filters[self.current_filter].next_result();

            if next.is_none() {
                return None;
            }
            let (next_result, next_positions) = next.unwrap();
            base_result = next_result;
            base_positions = next_positions
                .iter()
                .map(|pos| (*pos, self.current_filter))
                .collect();

            matches_left = self.filters.len() - 1;
        }
    }
}

impl QueryRuntimeFilter for DistanceFilter {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        let base_result = self.filters[self.current_filter].first_result(start);
        self.result(base_result)
    }

    fn next_result(&mut self) -> Option<DocResult> {
        let base_result = self.filters[self.current_filter].next_result();
        self.result(base_result)
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        for f in self.filters.iter_mut() {
            f.prepare_relevancy_scoring(&mut qsi);
        }
    }

    fn check_double_not(&self, _parent_is_neg: bool) -> Result<(), Error> {
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        false
    }
}


pub struct AndFilter<'a> {
    filters: Vec<Box<dyn QueryRuntimeFilter + 'a>>,
    current_filter: usize,
    array_depth: usize,
}

impl<'a> AndFilter<'a> {
    pub fn new(filters: Vec<Box<dyn QueryRuntimeFilter + 'a>>, array_depth: usize) -> AndFilter<'a> {
        AndFilter {
            filters: filters,
            current_filter: 0,
            array_depth: array_depth,
        }
    }

    fn result(&mut self, base: Option<DocResult>) -> Option<DocResult> {
        let mut matches_count = self.filters.len() - 1;

        if base.is_none() {
            return None;
        }
        let mut base_result = base.unwrap();

        base_result.arraypath.resize(self.array_depth, 0);

        loop {
            self.current_filter += 1;
            if self.current_filter == self.filters.len() {
                self.current_filter = 0;
            }

            let next = self.filters[self.current_filter].first_result(&base_result);

            if next.is_none() {
                return None;
            }
            let mut next_result = next.unwrap();

            next_result.arraypath.resize(self.array_depth, 0);

            if base_result == next_result {
                matches_count -= 1;
                base_result.combine(&mut next_result);
                if matches_count == 0 {
                    return Some(base_result);
                }
            } else {
                base_result = next_result;
                matches_count = self.filters.len() - 1;
            }
        }
    }
}

impl<'a> QueryRuntimeFilter for AndFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        let base_result = self.filters[self.current_filter].first_result(start);
        self.result(base_result)
    }

    fn next_result(&mut self) -> Option<DocResult> {
        let base_result = self.filters[self.current_filter].next_result();
        self.result(base_result)
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        for f in self.filters.iter_mut() {
            f.prepare_relevancy_scoring(&mut qsi);
        }
    }

    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error> {
        for f in self.filters.iter() {
            try!(f.check_double_not(parent_is_neg));
        }
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        for f in self.filters.iter() {
            if !f.is_all_not() {
                return false;
            }
        }
        true
    }
}

/// Used by OrFilter to maintain a already fetched result so we don't refetch when one side isn't
/// returned to caller. Because we won't know which side gets returned until both sides are
/// fetched.
pub struct FilterWithResult<'a> {
    filter: Box<dyn QueryRuntimeFilter + 'a>,
    result: Option<DocResult>,
    is_done: bool,
    array_depth: usize,
}

impl<'a> FilterWithResult<'a> {
    fn prime_first_result(&mut self, start: &DocResult) {
        if self.is_done {
            return;
        }
        if self.result.is_none() {
            self.result = self.filter.first_result(start);
        } else if self.result
                      .as_ref()
                      .unwrap()
                      .less(start, self.array_depth) {
            self.result = self.filter.first_result(start);
        }
        if self.result.is_none() {
            self.is_done = true;
        } else {
            self.result
                .as_mut()
                .unwrap()
                .arraypath
                .resize(self.array_depth, 0);
        }
    }

    fn prime_next_result(&mut self) {
        if self.is_done {
            return;
        }
        if self.result.is_none() {
            self.result = self.filter.next_result();
        }
        if self.result.is_none() {
            self.is_done = true;
        } else {
            self.result
                .as_mut()
                .unwrap()
                .arraypath
                .resize(self.array_depth, 0);
        }
    }
}

pub struct OrFilter<'a> {
    left: FilterWithResult<'a>,
    right: FilterWithResult<'a>,
}

impl<'a> OrFilter<'a> {
    pub fn new(left: Box<dyn QueryRuntimeFilter + 'a>,
               right: Box<dyn QueryRuntimeFilter + 'a>,
               array_depth: usize)
               -> OrFilter<'a> {
        OrFilter {
            left: FilterWithResult {
                filter: left,
                result: None,
                array_depth: array_depth,
                is_done: false,
            },

            right: FilterWithResult {
                filter: right,
                result: None,
                array_depth: array_depth,
                is_done: false,
            },
        }
    }
    fn take_smallest(&mut self) -> Option<DocResult> {
        if let Some(mut left) = self.left.result.take() {
            // left exists
            if let Some(mut right) = self.right.result.take() {
                // both exist, return smallest
                match left.cmp(&right) {
                    Ordering::Less => {
                        // left is smallest, return and put back right
                        self.right.result = Some(right);
                        Some(left)
                    }
                    Ordering::Greater => {
                        // right is smallest, return and put back left
                        self.left.result = Some(left);
                        Some(right)
                    }
                    Ordering::Equal => {
                        left.combine(&mut right);
                        self.right.result = Some(right);
                        Some(left)
                    }
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
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        self.left.prime_first_result(start);
        self.right.prime_first_result(start);
        self.take_smallest()
    }

    fn next_result(&mut self) -> Option<DocResult> {
        self.left.prime_next_result();
        self.right.prime_next_result();
        self.take_smallest()
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        self.left.filter.prepare_relevancy_scoring(&mut qsi);
        self.right.filter.prepare_relevancy_scoring(&mut qsi);
    }

    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error> {
        try!(self.left.filter.check_double_not(parent_is_neg));
        try!(self.right.filter.check_double_not(parent_is_neg));
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        if self.left.filter.is_all_not() && self.right.filter.is_all_not() {
            true
        } else {
            false
        }
    }
}


pub struct NotFilter<'a> {
    iter: DBIterator,
    filter: Box<dyn QueryRuntimeFilter + 'a>,
    last_doc_returned: Option<DocResult>,
    kb: KeyBuilder,
}

impl<'a> NotFilter<'a> {
    pub fn new(snapshot: &Snapshot,
               filter: Box<dyn QueryRuntimeFilter + 'a>,
               kb: KeyBuilder)
               -> NotFilter<'a> {
        NotFilter {
            iter: snapshot.new_iterator(),
            filter: filter,
            last_doc_returned: Some(DocResult::new()),
            kb: kb,
        }
    }

    fn is_a_not_match(&mut self, dr: &DocResult) -> bool {
        let ret = match dr.last_segment_array_index() {
            Some(&0) => {
                // if we got a (not) match on the first array element, it's always a match
                // but only if the document actually exists.
                true
            }
            Some(_) => {
                // if we got a (not) match on any other element, check to make sure the key exists.
                // if not, it means other elements did a regular match and skipped them, then we
                // ran off the end of the array.
                let value_key = self.kb.kp_value_key_from_doc_result(&dr);
                self.iter
                    .set_mode(IteratorMode::From(value_key.as_bytes(),
                                                 rocksdb::Direction::Forward));
                if let Some((key, _value)) = self.iter.next() {
                    let key_str = unsafe { str::from_utf8_unchecked(&key) };
                    KeyBuilder::is_kp_value_key_prefix(&value_key, &key_str)
                } else {
                    false
                }
            }
            None => {
                //not an array. always a (not) match.
                true
            }
        };
        if ret {
            // make sure we actually have a document. It's possible we matched a non-existent seq.
            let mut kb = KeyBuilder::new();
            kb.push_object_key("_id");
            let value_key = kb.kp_value_key_from_doc_result(dr);
            self.iter
                .set_mode(IteratorMode::From(value_key.as_bytes(), rocksdb::Direction::Forward));
            if let Some((key, _value)) = self.iter.next() {
                let key_str = unsafe { str::from_utf8_unchecked(&key) };
                value_key == key_str
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl<'a> QueryRuntimeFilter for NotFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        let mut start = start.clone_only_seq_and_arraypath();
        start.arraypath.resize(self.kb.arraypath_len(), 0);
        while let Some(dr) = self.filter.first_result(&start) {
            if start.less(&dr, self.kb.arraypath_len()) {
                if self.is_a_not_match(&start) {
                    self.last_doc_returned = Some(start.clone_only_seq_and_arraypath());
                    return Some(start.clone_only_seq_and_arraypath());
                } else {
                    start.increment_first(self.kb.arraypath_len());
                }
            } else {
                start.increment_last(self.kb.arraypath_len());
            }
        }
        self.last_doc_returned = None;
        if self.is_a_not_match(&start) {
            Some(start)
        } else {
            None
        }
    }

    fn next_result(&mut self) -> Option<DocResult> {
        if let Some(mut next) = self.last_doc_returned.take() {
            next.increment_last(self.kb.arraypath_len());
            self.first_result(&next)
        } else {
            None
        }
    }

    fn prepare_relevancy_scoring(&mut self, _qsi: &mut QueryScoringInfo) {
        // no op
    }

    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error> {
        if parent_is_neg {
            return Err(Error::Parse("Logical not (\"!\") is nested inside of another logical not. \
                                     This is not allowed."
                                            .to_string()));
        }
        try!(self.filter.check_double_not(true));
        Ok(())
    }

    fn is_all_not(&self) -> bool {
        true
    }
}

pub struct BindFilter<'a> {
    bind_var_name: String,
    filter: Box<dyn QueryRuntimeFilter + 'a>,
    array_depth: usize,
    kb: KeyBuilder,
    option_next: Option<DocResult>,
}

impl<'a> BindFilter<'a> {
    pub fn new(bind_var_name: String,
               filter: Box<dyn QueryRuntimeFilter + 'a>,
               kb: KeyBuilder)
               -> BindFilter {
        BindFilter {
            bind_var_name: bind_var_name,
            filter: filter,
            array_depth: kb.arraypath_len(),
            kb: kb,
            option_next: None,
        }
    }

    fn collect_results(&mut self, mut first: DocResult) -> Option<DocResult> {
        let value_key = self.kb.kp_value_key_from_doc_result(&first);
        first.add_bind_name_result(&self.bind_var_name, value_key);

        while let Some(next) = self.filter.next_result() {
            if next.seq == first.seq {
                let value_key = self.kb.kp_value_key_from_doc_result(&next);
                first.add_bind_name_result(&self.bind_var_name, value_key);
            } else {
                self.option_next = Some(next);
                return Some(first);
            }
        }
        Some(first)
    }
}

impl<'a> QueryRuntimeFilter for BindFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        let first = if let Some(next) = self.option_next.take() {
            if start.less(&next, self.array_depth) {
                Some(next)
            } else {
                self.filter.first_result(&start)
            }
        } else {
            self.filter.first_result(&start)
        };

        if let Some(first) = first {
            self.collect_results(first)
        } else {
            None
        }
    }

    fn next_result(&mut self) -> Option<DocResult> {
        let first = if let Some(next) = self.option_next.take() {
            Some(next)
        } else {
            self.filter.next_result()
        };

        if let Some(first) = first {
            self.collect_results(first)
        } else {
            None
        }
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        self.filter.prepare_relevancy_scoring(&mut qsi);
    }

    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error> {
        self.filter.check_double_not(parent_is_neg)
    }

    fn is_all_not(&self) -> bool {
        self.filter.is_all_not()
    }
}

pub struct BoostFilter<'a> {
    filter: Box<dyn QueryRuntimeFilter + 'a>,
    boost: f32,
}

impl<'a> BoostFilter<'a> {
    pub fn new(filter: Box<dyn QueryRuntimeFilter + 'a>, boost: f32) -> BoostFilter {
        BoostFilter {
            filter: filter,
            boost: boost,
        }
    }
}

impl<'a> QueryRuntimeFilter for BoostFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Option<DocResult> {
        if let Some(mut dr) = self.filter.first_result(&start) {
            dr.boost_scores(self.boost);
            Some(dr)
        } else {
            None
        }
    }

    fn next_result(&mut self) -> Option<DocResult> {
        if let Some(mut dr) = self.filter.next_result() {
            dr.boost_scores(self.boost);
            Some(dr)
        } else {
            None
        }
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        self.filter.prepare_relevancy_scoring(&mut qsi);
    }

    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error> {
        self.filter.check_double_not(parent_is_neg)
    }

    fn is_all_not(&self) -> bool {
        self.filter.is_all_not()
    }
}
