extern crate capnp;

use std::str;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashSet;
use index::Index;
use std::f32;

use error::Error;
use key_builder::KeyBuilder;
use stems::StemmedWord;
use query::{DocResult, QueryScoringInfo};

// TODO vmx 2016-11-02: Make it import "rocksdb" properly instead of needing to import the individual tihngs
use rocksdb::{self, DBIterator, IteratorMode};
use records_capnp::payload;

struct Scorer {
    idf: f32,
    boost: f32,
    keypathword_count_key: String,
    keypath_count_key: String,
    term_ordinal: usize,
}

impl Scorer {
    fn new(word: &str, kb: &KeyBuilder, boost: f32) -> Scorer {
        Scorer {
            idf: f32::NAN,
            boost: boost,
            keypathword_count_key: kb.keypathword_count_key(&word),
            keypath_count_key: kb.keypath_count_key(),
            term_ordinal: 0,
        }
    }

    fn init(&mut self, mut iter: &mut DBIterator, qsi: &mut QueryScoringInfo) {
        let doc_freq = if let Some(bytes) = self.get_value(&mut iter,
                                                           &self.keypathword_count_key) {
            Index::convert_bytes_to_u64(bytes.as_ref()) as f32
        } else {
            0.0
        };
        
        let num_docs = if let Some(bytes) = self.get_value(&mut iter, &self.keypath_count_key) {
            Index::convert_bytes_to_u64(bytes.as_ref()) as f32
        } else {
            0.0
        };

        self.idf = 1.0 + (num_docs/(doc_freq + 1.0)).ln();
        self.term_ordinal = qsi.num_terms;
        qsi.num_terms += 1;
        qsi.sum_of_idt_sqs += self.idf * self.idf;
    }

    fn get_value(&self, iter: &mut DBIterator, key: &String) -> Option<Box<[u8]>> {
        iter.set_mode(IteratorMode::From(key.as_bytes(), rocksdb::Direction::Forward));
        if let Some((ret_key, ret_value)) = iter.next() {
            if ret_key.len() == key.len() && ret_key.starts_with(key.as_bytes())  {
                Some(ret_value)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn add_match_score(&self, num_matches: u32,
                       total_field_words: u32, dr: &mut DocResult) {
        if self.should_score() {
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
}

pub struct ExactMatchFilter {
    iter: DBIterator,
    keypathword: String,
    word_pos: u32,
    suffix_offset: u32,
    suffix: String,
    scorer: Scorer,
}



impl ExactMatchFilter {
    pub fn new(iter: DBIterator, stemmed_word: &StemmedWord,
               kb: &KeyBuilder, boost: f32) -> ExactMatchFilter {
        ExactMatchFilter{
            iter: iter,
            keypathword: kb.get_keypathword_only(&stemmed_word.stemmed),
            word_pos: stemmed_word.word_pos,
            suffix: stemmed_word.suffix.clone(),
            suffix_offset: stemmed_word.suffix_offset,
            scorer: Scorer::new(&stemmed_word.stemmed, &kb, boost),
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
            let wordinfos = try!(payload.get_wordinfos());
            for wi in wordinfos.iter() {
                if self.word_pos == wi.get_word_pos() &&
                    self.suffix_offset == wi.get_suffix_offset() &&
                    self.suffix == try!(wi.get_suffix_text()) {
                        // We have a candidate document to return
                        let key_str = unsafe{str::from_utf8_unchecked(&key)};
                        let mut dr = KeyBuilder::parse_doc_result_from_key(&key_str);
                        self.scorer.add_match_score(wordinfos.len(),
                                                    payload.get_total_words(), &mut dr);
                        return Ok(Some(dr));
                }
            }
        }
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        self.scorer.init(&mut self.iter, &mut qsi);
    }
}

pub struct StemmedWordFilter {
    iter: DBIterator,
    keypathword: String,
    scorer: Scorer,
}

impl StemmedWordFilter {
    pub fn new(iter: DBIterator, stemmed_word: &str,
               kb: &KeyBuilder, boost: f32) -> StemmedWordFilter {
        StemmedWordFilter {
            iter: iter,
            keypathword: kb.get_keypathword_only(&stemmed_word),
            scorer: Scorer::new(stemmed_word, kb, boost),
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
            let message_reader = ::capnp::serialize_packed::read_message(
                    &mut &*value, ::capnp::message::ReaderOptions::new()).unwrap();
            let payload = message_reader.get_root::<payload::Reader>().unwrap();


            self.scorer.add_match_score(try!(payload.get_wordinfos()).len(),
                                        payload.get_total_words(), &mut dr);
        }

        Ok(Some(dr))
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        self.scorer.init(&mut self.iter, &mut qsi);
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
    pub fn new(iter: DBIterator, stemmed_word: &str, kb: &KeyBuilder, boost: f32) -> StemmedWordPosFilter {
        StemmedWordPosFilter{
            iter: iter,
            keypathword: kb.get_keypathword_only(&stemmed_word),
            scorer: Scorer::new(&stemmed_word, &kb, boost),
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

        let message_reader = ::capnp::serialize_packed::read_message(
                &mut &*value, ::capnp::message::ReaderOptions::new()).unwrap();
        let payload = message_reader.get_root::<payload::Reader>().unwrap();

        let positions: Vec<u32> = try!(payload.get_wordinfos()).iter()
                                                               .map(|wi| wi.get_word_pos())
                                                               .collect();
        
        let key_str = unsafe{str::from_utf8_unchecked(&key)};
        let mut dr = KeyBuilder::parse_doc_result_from_key(&key_str);

        self.scorer.add_match_score(positions.len() as u32, payload.get_total_words(), &mut dr);

        Ok(Some((dr, positions)))
    }

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        self.scorer.init(&mut self.iter, &mut qsi);
    }
}

pub struct StemmedPhraseFilter {
    filters: Vec<StemmedWordPosFilter>,
}

impl StemmedPhraseFilter {
    pub fn new(filters: Vec<StemmedWordPosFilter>) -> StemmedPhraseFilter {
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

    fn prepare_relevancy_scoring(&mut self, mut qsi: &mut QueryScoringInfo) {
        for f in self.filters.iter_mut() {
            f.prepare_relevancy_scoring(&mut qsi);
        }
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
                                 }
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
}

pub struct BindFilter<'a> {
    bind_var_name: String,
    filter: Box<QueryRuntimeFilter + 'a>,
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
            if next >= *start {
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
}

