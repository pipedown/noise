extern crate rocksdb;
extern crate varint;
extern crate uuid;

use std::collections::{HashSet, BTreeMap};
use std::str;
use std::io::Cursor;
use std::mem;
use std::io::Write;
use self::uuid::{Uuid, UuidVersion};
use std::cmp::Ordering;

use self::varint::{VarintRead, VarintWrite};

use rocksdb::{MergeOperands, IteratorMode, Snapshot as RocksSnapshot, CompactionDecision};
pub use rocksdb::batch;

use error::Error;
use json_shred::{Shredder};
use key_builder::KeyBuilder;
use snapshot::Snapshot;

const NOISE_HEADER_VERSION: u64 = 1;

pub struct Index {
    high_doc_seq: u64,
    pub rocks: Option<rocksdb::DB>,
    id_str_in_batch: HashSet<String>,
}

pub enum OpenOptions {
    Create
}

impl Index {
    pub fn new() -> Index {
        Index {
            high_doc_seq: 0,
            rocks: None,
            id_str_in_batch: HashSet::new(),
        }
    }
    // NOTE vmx 2016-10-13: Perhpas the name should be specified on `new()` as it is bound
    // to a single instance. The next question would then be if `open()` really makes sense
    // or if it should be combined with `new()`.
    //fn open(&mut self, name: &str, open_options: Option<OpenOptions>) -> Result<DB, String> {
    pub fn open(&mut self, name: &str, open_options: Option<OpenOptions>) -> Result<(), Error> {
        let mut rocks_options = rocksdb::Options::default();
        rocks_options.set_comparator("noise_cmp", Index::compare_keys);
        rocks_options.set_merge_operator("noise_merge", Index::sum_merge);
        rocks_options.set_compaction_filter("noise_compact", Index::compaction_filter);
        
        let rocks = match rocksdb::DB::open(&rocks_options, name) {
            Ok(rocks) => rocks,
            Err(error) => {
                match open_options {
                    Some(OpenOptions::Create) => (),
                    _ => return Err(Error::Rocks(error)),
                }

                rocks_options.create_if_missing(true);

                let rocks = try!(rocksdb::DB::open(&rocks_options, name));
                
                let mut bytes = Vec::with_capacity(8*2);
                bytes.write(&Index::convert_u64_to_bytes(NOISE_HEADER_VERSION)).unwrap();
                bytes.write(&Index::convert_u64_to_bytes(0)).unwrap();
                try!(rocks.put_opt(b"HDB", &bytes, &rocksdb::WriteOptions::new()));
                
                rocks
            }
        };

        // validate header is there
        let value = try!(rocks.get(b"HDB")).unwrap();
        self.rocks = Some(rocks);
        assert_eq!(value.len(), 8*2);
        // first 8 is version
        assert_eq!(Index::convert_bytes_to_u64(&value[..8]), NOISE_HEADER_VERSION);
        // next 8 is high seq
        self.high_doc_seq = Index::convert_bytes_to_u64(&value[8..]);

        Ok(())
    }

    pub fn is_open(&self) -> bool {
        self.rocks.is_some()
    }

    pub fn new_snapshot(&self) -> Snapshot {
        Snapshot::new(RocksSnapshot::new(self.rocks.as_ref().unwrap()))
    }

    //This deletes the Rockdbs instance from disk
    pub fn drop(name: &str) -> Result<(), Error> {
        let ret = try!(rocksdb::DB::destroy(&rocksdb::Options::default(), name));
        Ok(ret)
    }

    pub fn add(&mut self, json: &str, mut batch: &mut rocksdb::WriteBatch) -> Result<String, Error> {
        if !self.is_open() {
            return Err(Error::Write("Index isn't open.".to_string()));
        }
        let mut shredder = Shredder::new();
        let (seq, docid) = if let Some(docid) = try!(shredder.shred(json)) {
            // user supplied doc id, see if we have an existing one.
            if self.id_str_in_batch.contains(&docid) {
                // oops use trying to add some doc 2x to this batch.
                return Err(Error::Write("Attempt to insert multiple docs with same _id"
                    .to_string()));
            }
            if let Some((seq, existing_key_values)) = try!(self.gather_doc_fields(&docid)) {
                shredder.merge_existing_doc(existing_key_values);
                (seq, docid)
            } else {
                // no existing document found, so we use the one supplied.
                self.high_doc_seq += 1;
                (self.high_doc_seq, docid)
            }
        } else {
            // no doc id supplied in document, so we create one.
            let docid = Uuid::new(UuidVersion::Random).unwrap().simple().to_string();
            try!(shredder.add_id(&docid));
            self.high_doc_seq += 1;
            (self.high_doc_seq, docid)
        };
        // now everything needs to be added to the batch,
        try!(shredder.add_all_to_batch(seq, &mut batch));
        self.id_str_in_batch.insert(docid.clone());

        Ok(docid)
    }

    /// Returns Ok(true) if the document was found and deleted, Ok(false) if it could not be found
    pub fn delete(&mut self, docid: &str, mut batch: &mut rocksdb::WriteBatch) -> Result<bool, Error> {
        if !self.is_open() {
            return Err(Error::Write("Index isn't open.".to_string()));
        }
        if self.id_str_in_batch.contains(docid) {
            // oops use trying to delete a doc that's in the batch. Can't happen,
            return Err(Error::Write("Attempt to delete doc with same _id added earlier"
                    .to_string()));
        }
        if let Some((seq, key_values)) = try!(self.gather_doc_fields(docid)) {
            let mut shredder = Shredder::new();
            try!(shredder.delete_existing_doc(docid, seq, key_values,
                &mut batch));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn gather_doc_fields(&self, docid: &str) ->
            Result<Option<(u64, BTreeMap<String, Vec<u8>>)>, Error> {
        if let Some(seq) = try!(self.fetch_seq(&docid)) {
            // collect up all the fields for the existing doc
            let kb = KeyBuilder::new();
            let value_key = kb.value_key(seq);
            let mut key_values = BTreeMap::new();
            
            let mut iter = self.rocks.as_ref().unwrap().iterator(IteratorMode::Start);
            // Seek in index to >= entry
            iter.set_mode(IteratorMode::From(value_key.as_bytes(),
                                            rocksdb::Direction::Forward));
            loop {
                let (key, value) = match iter.next() {
                    Some((key, value)) => (key, value),
                    None => break,
                };

                if !key.starts_with(value_key.as_bytes()) {
                    break;
                }
                let key = unsafe{ str::from_utf8_unchecked(&key)}.to_string();
                let value = value.iter().map(|i|*i).collect();
                key_values.insert(key, value);
            }
            return Ok(Some((seq, key_values)));
        } else {
            return Ok(None);
        }
    }

    // Store the current batch
    pub fn flush(&mut self, mut batch: rocksdb::WriteBatch) -> Result<(), Error> {
        // Flush can only be called if the index is open
        if !self.is_open() {
            return Err(Error::Write("Index isn't open.".to_string()));
        }
        let rocks = self.rocks.as_ref().unwrap();

        let mut bytes = Vec::with_capacity(8*2);
        bytes.write(&Index::convert_u64_to_bytes(NOISE_HEADER_VERSION)).unwrap();
        bytes.write(&Index::convert_u64_to_bytes(self.high_doc_seq)).unwrap();
        try!(batch.put(b"HDB", &bytes));

        let status = try!(rocks.write(batch));
        // Make sure there's a always a valid WriteBarch after writing it into RocksDB,
        // else calls to `self.batch.as_mut().unwrap()` would panic.
        self.id_str_in_batch.clear();
        Ok(status)
    }

    pub fn all_keys(&self) -> Result<Vec<String>, Error> {
        if !self.is_open() {
            return Err(Error::Write("Index isn't open.".to_string()));
        }
        let rocks = self.rocks.as_ref().unwrap();
        let mut results = Vec::new();
        for (key, _value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
            results.push(key_string);
        }
        Ok(results)
    }

    /// Should not be used generally since it not varint. Used for header fields
    /// since only one header is in the database it's not a problem with excess size.
    fn convert_bytes_to_u64(bytes: &[u8]) -> u64 {
        debug_assert!(bytes.len() == 8);
        let mut buffer = [0; 8];
        for (n, b) in bytes.iter().enumerate() {
            buffer[n] = *b;
        }
        unsafe{ mem::transmute(buffer) }
    }
    
    /// Should not be used generally since it not varint. Used for header fields
    /// since only one header is in the database it's not a problem with excess size.
    fn convert_u64_to_bytes(val: u64) -> [u8; 8] {
        unsafe{ mem::transmute(val) }
    }

    pub fn convert_bytes_to_i32(bytes: &[u8]) -> i32 {
        let mut vec = Vec::with_capacity(bytes.len());
        vec.extend(bytes.into_iter());
        let mut read = Cursor::new(vec);
        read.read_signed_varint_32().unwrap()
    }
    
    pub fn convert_i32_to_bytes(val: i32) -> Vec<u8> {
        let mut bytes = Cursor::new(Vec::new());
        assert!(bytes.write_signed_varint_32(val).is_ok());
        bytes.into_inner()
    }

    pub fn fetch_seq(&self, id: &str) ->  Result<Option<u64>, Error> {
        // Fetching an seq is only possible if the index is open
        // NOTE vmx 2016-10-17: Perhaps that shouldn't panic?
        assert!(&self.rocks.is_some());
        let rocks = self.rocks.as_ref().unwrap();

        let key = format!("I{}", id);
        match try!(rocks.get(&key.as_bytes())) {
            // If there is an id, it's UTF-8
            Some(bytes) => Ok(Some(bytes.to_utf8().unwrap().parse().unwrap())),
            None => Ok(None)
        }
    }

    fn compaction_filter(_level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        if !(key[0] as char == 'F' || key[0] as char == 'K') {
            return CompactionDecision::Keep;
        }
        if 0 == Index::convert_bytes_to_i32(&value) {
            CompactionDecision::Remove
        } else {
            CompactionDecision::Keep
        }
    }

    fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
        if a[0] == 'W' as u8 && b[0] == 'W' as u8 {
            let astr = unsafe {str::from_utf8_unchecked(&a)};
            let bstr = unsafe {str::from_utf8_unchecked(&b)};
            KeyBuilder::compare_keys(astr, bstr)
        } else {
            a.cmp(b)
        }
    }

    fn sum_merge(new_key: &[u8],
                    existing_val: Option<&[u8]>,
                    operands: &mut MergeOperands)
                    -> Vec<u8> {
        if !(new_key[0] as char == 'F' || new_key[0] as char == 'K') {
            panic!("unknown key type to merge!");
        }

        let mut count = if let Some(bytes) = existing_val {
            Index::convert_bytes_to_i32(&bytes)
        } else {
            0
        };
        
        for bytes in operands {
            count += Index::convert_bytes_to_i32(&bytes);
        }
        Index::convert_i32_to_bytes(count)
    }
}


#[cfg(test)]
mod tests {
    extern crate rocksdb;
    use super::{Index, OpenOptions};
    use query::Query;
    use std::str;
    use snapshot::JsonFetcher;
    use json_value::JsonValue;

    #[test]
    fn test_open() {
        let dbname = "target/tests/firstnoisedb";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        //let db = super::Index::open("firstnoisedb", Option::None).unwrap();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();
        index.flush().unwrap();
    }

    #[test]
    fn test_uuid() {
        let dbname = "target/tests/testuuid";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();

        let id = index.add(r#"{"foo":"bar"}"#).unwrap();

        index.flush().unwrap();
                                             
        let mut results = Query::get_matches(r#"find {foo:=="bar"}"#, &index).unwrap();
        let query_id = results.get_next_id().unwrap();
        assert!(query_id.len() == 32);
        assert_eq!(query_id, id);
    }

    #[test]
    fn test_compaction() {
        let dbname = "target/tests/testcompaction";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();

        let id = index.add(r#"{"foo":"bar"}"#).unwrap();
        index.flush().unwrap();

        index.delete(&id).unwrap();
        index.flush().unwrap();

        let rocks = index.rocks.as_mut().unwrap();
        
        // apparently you need to do compaction twice when there are merges
        // first one lets the merges happen, the second lets them be collected.
        // this is acceptable since eventually the keys go away.
        // if this test fails non-deterministically we might have a problem.
        rocks.compact_range(None, None);
        rocks.compact_range(None, None);

        let mut iter = rocks.iterator(rocksdb::IteratorMode::Start);
        let (key, _value) = iter.next().unwrap();
        assert!(key.starts_with(&b"HDB"[..]));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_updates() {
        let dbname = "target/tests/testupdates";
        let _ = Index::drop(dbname);

        let mut index = Index::new();
        index.open(dbname, Some(OpenOptions::Create)).unwrap();

        //let _ = index.add(r#"{"_id":"1", "foo":"array", "baz": ["a","b",["c","d",["e"]]]}"#).unwrap();
        
        //index.flush().unwrap();

        let _ = index.add(r#"{"_id":"1", "foo":"array", "baz": [1,2,[3,4,[5]]]}"#).unwrap();
        
        index.flush().unwrap();
        {
        let rocks = index.rocks.as_mut().unwrap();

        let mut results = Vec::new();
        for (key, value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            if key[0] as char == 'V' {
                let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
                results.push((key_string, JsonFetcher::bytes_to_json_value(&*value)));
            }
        }

        let expected = vec![
            ("V1#._id".to_string(), JsonValue::String("1".to_string())),
            ("V1#.baz$0".to_string(), JsonValue::Number(1.0)),
            ("V1#.baz$1".to_string(), JsonValue::Number(2.0)),
            ("V1#.baz$2$0".to_string(), JsonValue::Number(3.0)),
            ("V1#.baz$2$1".to_string(), JsonValue::Number(4.0)),
            ("V1#.baz$2$2$0".to_string(), JsonValue::Number(5.0)),
            ("V1#.foo".to_string(), JsonValue::String("array".to_string()))];
        assert_eq!(results, expected);
        }

        let _ = index.add(r#"{"_id":"1", "foo":"array", "baz": []}"#).unwrap();
        index.flush().unwrap();

        let rocks = index.rocks.as_mut().unwrap();

        let mut results = Vec::new();
        for (key, value) in rocks.iterator(rocksdb::IteratorMode::Start) {
            if key[0] as char == 'V' {
                let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
                results.push((key_string, JsonFetcher::bytes_to_json_value(&*value)));
            }
        }
       let expected = vec![
            ("V1#._id".to_string(), JsonValue::String("1".to_string())),
            ("V1#.baz".to_string(), JsonValue::Array(vec![])),
            ("V1#.foo".to_string(), JsonValue::String("array".to_string()))
            ];
        assert_eq!(results, expected);
    }
}
