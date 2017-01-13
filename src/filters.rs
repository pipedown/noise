extern crate varint;

use std::str;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashSet;
use index::Index;
use std::f32;
use std::io::Cursor;

use error::Error;
use key_builder::KeyBuilder;
use query::{DocResult, QueryScoringInfo, RetValue};
use json_value::JsonValue;

// TODO vmx 2016-11-02: Make it import "rocksdb" properly instead of needing to import the individual tihngs
use rocksdb::{self, DBIterator, Snapshot, IteratorMode};
use self::varint::VarintRead;

struct Scorer {
    iter: DBIterator,
    idf: f32,
    boost: f32,
    kb: KeyBuilder,
    word: String,
    term_ordinal: usize,
}

impl Scorer {
    fn new(iter: DBIterator, word: &str, kb: &KeyBuilder, boost: f32) -> Scorer {
        Scorer {
            iter: iter,
            idf: f32::NAN,
            boost: boost,
            kb: kb.clone(),
            word: word.to_string(),
            term_ordinal: 0,
        }
    }

    fn init(&mut self, qsi: &mut QueryScoringInfo) {
        let key = self.kb.keypathword_count_key(&self.word);
        let doc_freq = if let Some(bytes) = self.get_value(&key) {
            Index::convert_bytes_to_u32(bytes.as_ref()) as f32
        } else {
            0.0
        };

        let key = self.kb.keypath_count_key();
        let num_docs = if let Some(bytes) = self.get_value(&key) {
            Index::convert_bytes_to_u32(bytes.as_ref()) as f32
        } else {
            0.0
        };

        self.idf = 1.0 + (num_docs/(doc_freq + 1.0)).ln();
        self.term_ordinal = qsi.num_terms;
        qsi.num_terms += 1;
        qsi.sum_of_idt_sqs += self.idf * self.idf;
    }

    fn get_value(&mut self, key: &str) -> Option<Box<[u8]>> {
        self.iter.set_mode(IteratorMode::From(key.as_bytes(), rocksdb::Direction::Forward));
        if let Some((ret_key, ret_value)) = self.iter.next() {
            if ret_key.len() == key.len() && ret_key.starts_with(key.as_bytes())  {
                Some(ret_value)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn add_match_score(&mut self, num_matches: u32, dr: &mut DocResult) {
        if self.should_score() {
            let key = self.kb.field_length_key_from_doc_result(dr);
            let total_field_words = if let Some(bytes) = self.get_value(&key) {
                Index::convert_bytes_to_u32(bytes.as_ref()) as f32
            } else {
                panic!("Couldn't find field length for a match!! WHAT!");
            };

            let tf: f32 = (num_matches as f32).sqrt();
            let norm = 1.0/(total_field_words as f32).sqrt();
            let score = self.idf * self.idf * tf * norm * self.boost;
            dr.add_score(self.term_ordinal, score);
        }
    }

    fn should_score(&self) -> bool {
        !self.idf.is_nan()
    }
}

pub trait QueryRuntimeFilter {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error>;
    fn next_result(&mut self) -> Result<Option<DocResult>, Error>;
    fn prepare_relevancy_scoring(&mut self, qsi: &mut QueryScoringInfo);
    
    /// returns error is a double negation is detected
    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error>;
    
    /// return true if filter or all subfilters are NotFilters
    fn is_all_not(&self) -> bool;
}


pub struct StemmedWordFilter {
    iter: DBIterator,
    keypathword: String,
    scorer: Scorer,
}

impl StemmedWordFilter {
    pub fn new(snapshot: &Snapshot, stemmed_word: &str,
               kb: &KeyBuilder, boost: f32) -> StemmedWordFilter {
        StemmedWordFilter {
            iter: snapshot.iterator(IteratorMode::Start),
            keypathword: kb.get_keypathword_only(&stemmed_word),
            scorer: Scorer::new(snapshot.iterator(IteratorMode::Start), stemmed_word, kb, boost),
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
        let mut dr = KeyBuilder::parse_doc_result_from_key(&key_str);

        if self.scorer.should_score() {
            let mut vec = Vec::with_capacity(value.len());
            vec.extend(value.into_iter());
            let mut bytes = Cursor::new(vec);
            let mut count = 0;
            while let Ok(_pos) = bytes.read_unsigned_varint_32() {
                count += 1;
            }
            self.scorer.add_match_score(count, &mut dr);
        }

        Ok(Some(dr))
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
    iter: DBIterator,
    keypathword: String,
    scorer: Scorer,
}

impl StemmedWordPosFilter {
    pub fn new(snapshot: &Snapshot, stemmed_word: &str,
               kb: &KeyBuilder, boost: f32) -> StemmedWordPosFilter {
        StemmedWordPosFilter{
            iter: snapshot.iterator(IteratorMode::Start),
            keypathword: kb.get_keypathword_only(&stemmed_word),
            scorer: Scorer::new(snapshot.iterator(IteratorMode::Start),
                                &stemmed_word, &kb, boost),
        }
    }

    fn first_result(&mut self,
                    start: &DocResult) -> Result<Option<(DocResult, Vec<u32>)>, Error> {

        KeyBuilder::add_doc_result_to_keypathword(&mut self.keypathword, &start);
        // Seek in index to >= entry
        self.iter.set_mode(IteratorMode::From(self.keypathword.as_bytes(),
                           rocksdb::Direction::Forward));
        
        KeyBuilder::truncate_to_keypathword(&mut self.keypathword);

        self.next_result()
    }

    fn next_result(&mut self) -> Result<Option<(DocResult, Vec<u32>)>, Error> {
        let (key, value) = match self.iter.next() {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };
        if !key.starts_with(self.keypathword.as_bytes()) {
            // we passed the key path we are interested in. nothing left to do */
            return Ok(None)
        }

        let key_str = unsafe{str::from_utf8_unchecked(&key)};
        let mut dr = KeyBuilder::parse_doc_result_from_key(&key_str);

        let mut vec = Vec::with_capacity(value.len());
        vec.extend(value.into_iter());
        let mut bytes = Cursor::new(vec);
        let mut positions = Vec::new();
        while let Ok(pos) = bytes.read_unsigned_varint_32() {
            positions.push(pos);
        }

        self.scorer.add_match_score(positions.len() as u32, &mut dr);

        Ok(Some((dr, positions)))
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
        StemmedPhraseFilter {
            filters: filters,
        }
    }

    fn result(&mut self,
              base: Option<(DocResult, Vec<u32>)>) -> Result<Option<DocResult>, Error> {
        // this is the number of matches left before all terms match and we can return a result 
        let mut matches_left = self.filters.len() - 1;

        if base.is_none() { return Ok(None); }
        let (mut base_result, mut base_positions) = base.unwrap();

        if matches_left == 0 {
            return Ok(Some(base_result));
        }

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
                    if let Ok(_) = base_positions.binary_search(&(pos.saturating_sub(1))) {
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
}

impl ExactMatchFilter {
    pub fn new(snapshot: &Snapshot, filter: StemmedPhraseFilter,
            kb: KeyBuilder, phrase: String, case_sensitive: bool) -> ExactMatchFilter {
        ExactMatchFilter {
            iter: snapshot.iterator(IteratorMode::Start),
            filter: filter,
            kb: kb,
            phrase: if case_sensitive {phrase} else {phrase.to_lowercase()},
            case_sensitive: case_sensitive,
        }
    }

    fn check_exact(&mut self, mut dr: DocResult) -> Result<Option<DocResult>, Error> {
        loop {
            let value_key = self.kb.value_key_from_doc_result(&dr);

            self.iter.set_mode(IteratorMode::From(value_key.as_bytes(),
                               rocksdb::Direction::Forward));

            if let Some((key, value)) = self.iter.next() {
                debug_assert!(key.starts_with(value_key.as_bytes())); // must always be true!
                if let JsonValue::String(string) = RetValue::bytes_to_json_value(&*value) {
                    let matches = if self.case_sensitive {
                        self.phrase == string
                    } else {
                        self.phrase == string.to_lowercase()
                    };
                    if matches {
                        return Ok(Some(dr));
                    } else {
                        if let Some(next) = try!(self.filter.next_result()) {
                            dr = next;
                            // continue looping
                        } else {
                            return Ok(None);
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
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
        if let Some(dr) = try!(self.filter.first_result(start)) {
            self.check_exact(dr)
        } else {
            Ok(None)
        }
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        if let Some(dr) = try!(self.filter.next_result()) {
            self.check_exact(dr)
        } else {
            Ok(None)
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

    fn result(&mut self,
              base: Option<(DocResult, Vec<u32>)>) -> Result<Option<DocResult>, Error> {
        // yes this code complex. I tried to break it up, but it wants to be like this.

        // this is the number of matches left before all terms match and we can return a result 
        let mut matches_left = self.filters.len() - 1;

        if base.is_none() { return Ok(None); }
        let (mut base_result, positions) = base.unwrap();

        // This contains tuples of word postions and the filter they came from,
        // sorted by word position.
        let mut base_positions: Vec<(u32, usize)> = positions.iter()
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

            if base_result != next_result {
                // not same field, next_result becomes base_result.
                base_result = next_result;
                base_positions = next_positions.iter()
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
                let start = match base_positions.binary_search_by_key(&(sub),
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
    filters: Vec<Box<QueryRuntimeFilter + 'a>>,
    current_filter: usize,
    array_depth: usize,
}

impl<'a> AndFilter<'a> {
    pub fn new(filters: Vec<Box<QueryRuntimeFilter + 'a>>, array_depth: usize) -> AndFilter<'a> {
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
                base_result.combine(&mut next_result);
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
        } else if self.result.as_ref().unwrap().less(start, self.array_depth) {
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

pub struct OrFilter<'a> {
    left: FilterWithResult<'a>,
    right: FilterWithResult<'a>,
}

impl<'a> OrFilter<'a> {
    pub fn new(left: Box<QueryRuntimeFilter + 'a>,
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
                    },
                    Ordering::Greater => {
                        // right is smallest, return and put back left
                        self.left.result = Some(left);
                        Some(right)
                    },
                    Ordering::Equal => {
                        left.combine(&mut right);
                        self.right.result = Some(right);
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
    filter: Box<QueryRuntimeFilter + 'a>,
    last_doc_returned: Option<DocResult>,
    array_depth: usize,
}

impl<'a> NotFilter<'a> {
    pub fn new(filter: Box<QueryRuntimeFilter + 'a>, array_depth: usize) -> NotFilter {
        NotFilter {
            filter: filter,
            last_doc_returned: Some(DocResult::new()),
            array_depth: array_depth,
        }
    }
}

impl<'a> QueryRuntimeFilter for NotFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
        let mut start = start.clone_only_seq_and_arraypath();
        while let Some(dr) = try!(self.filter.first_result(&start)) {
            if start.less(&dr, self.array_depth) {
                self.last_doc_returned = Some(start.clone_only_seq_and_arraypath());
                return Ok(Some(start.clone_only_seq_and_arraypath()));
            }
            start.increment_last(self.array_depth);
        }
        self.last_doc_returned = None;
        Ok(Some(start))
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        let next = if let Some(ref last_doc_returned) = self.last_doc_returned {
            let mut next = last_doc_returned.clone_only_seq_and_arraypath();
            next.increment_last(self.array_depth);
            next
        } else {
            return Ok(None);
        };
        self.first_result(&next)
    }

    fn prepare_relevancy_scoring(&mut self, _qsi: &mut QueryScoringInfo) {
        // no op
    }

    fn check_double_not(&self, parent_is_neg: bool) -> Result<(), Error> {
        if parent_is_neg {
            return Err(Error::Parse("Logical not (\"!\") is nested inside of another logical not. \
                                     This is not allowed.".to_string()));
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
    filter: Box<QueryRuntimeFilter + 'a>,
    array_depth: usize,
    kb: KeyBuilder,
    option_next: Option<DocResult>,
}

impl<'a> BindFilter<'a> {

    pub fn new(bind_var_name: String,
               filter: Box<QueryRuntimeFilter + 'a>,
               kb: KeyBuilder) -> BindFilter {
        BindFilter {
            bind_var_name: bind_var_name,
            filter: filter,
            array_depth: kb.arraypath_len(),
            kb: kb,
            option_next: None,
        }
    }
    
    fn collect_results(&mut self, mut first: DocResult) -> Result<Option<DocResult>, Error> {
        let value_key = self.kb.value_key_from_doc_result(&first);
        first.add_bind_name_result(&self.bind_var_name, value_key);
        
        while let Some(next) = try!(self.filter.next_result()) {
            if next.seq == first.seq {
                let value_key = self.kb.value_key_from_doc_result(&next);
                first.add_bind_name_result(&self.bind_var_name, value_key);
            } else {
                self.option_next = Some(next);
                return Ok(Some(first));
            }
        }
        Ok(Some(first))
    }
}

impl<'a> QueryRuntimeFilter for BindFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
        let first = if let Some(next) = self.option_next.take() {
            if start.less(&next, self.array_depth) {
                Some(next)
            } else {
                try!(self.filter.first_result(&start))
            }
        } else {
            try!(self.filter.first_result(&start))
        };

        if let Some(first) = first {
            self.collect_results(first)
        } else {
            Ok(None)
        }
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        let first = if let Some(next) = self.option_next.take() {
            Some(next)
        } else {
            try!(self.filter.next_result())
        };

        if let Some(first) = first {
            self.collect_results(first)
        } else {
            Ok(None)
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
    filter: Box<QueryRuntimeFilter + 'a>,
    boost: f32,
}

impl<'a> BoostFilter<'a> {
    pub fn new(filter: Box<QueryRuntimeFilter + 'a>, boost: f32) -> BoostFilter {
        BoostFilter {
            filter: filter,
            boost: boost,
        }
    }
}

impl<'a> QueryRuntimeFilter for BoostFilter<'a> {
    fn first_result(&mut self, start: &DocResult) -> Result<Option<DocResult>, Error> {
        if let Some(mut dr) = try!(self.filter.first_result(&start)) {
            dr.boost_scores(self.boost);
            Ok(Some(dr))
        } else {
            Ok(None)
        }
    }

    fn next_result(&mut self) -> Result<Option<DocResult>, Error> {
        if let Some(mut dr) = try!(self.filter.next_result()) {
            dr.boost_scores(self.boost);
            Ok(Some(dr))
        } else {
            Ok(None)
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

