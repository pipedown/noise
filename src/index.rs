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

use std::sync::{Mutex, MutexGuard, LockResult};

use self::varint::{VarintRead, VarintWrite};

use rocksdb::{MergeOperands, IteratorMode, Snapshot as RocksSnapshot, CompactionDecision};
pub use rocksdb::WriteBatch;

use error::Error;
use json_shred::Shredder;
use key_builder::{self, KeyBuilder};
use snapshot::Snapshot;
use query::QueryResults;

const NOISE_HEADER_VERSION: u64 = 1;

pub struct Index {
    name: String,
    high_doc_seq: u64,
    pub rocks: rocksdb::DB,
}

pub struct Batch {
    wb: rocksdb::WriteBatch,
    id_str_in_batch: HashSet<String>,
}

impl Batch {
    pub fn new() -> Batch {
        Batch {
            wb: rocksdb::WriteBatch::default(),
            id_str_in_batch: HashSet::new(),
        }
    }
}

pub enum OpenOptions {
    Create,
}

impl Index {
    pub fn open(name: &str, open_options: Option<OpenOptions>) -> Result<Index, Error> {
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

                let mut bytes = Vec::with_capacity(8 * 2);
                bytes
                    .write(&Index::convert_u64_to_bytes(NOISE_HEADER_VERSION))
                    .unwrap();
                bytes.write(&Index::convert_u64_to_bytes(0)).unwrap();
                try!(rocks.put_opt(b"HDB", &bytes, &rocksdb::WriteOptions::new()));

                rocks
            }
        };

        // validate header is there
        let value = try!(rocks.get(b"HDB")).unwrap();
        assert_eq!(value.len(), 8 * 2);
        // first 8 is version
        assert_eq!(Index::convert_bytes_to_u64(&value[..8]),
                   NOISE_HEADER_VERSION);
        // next 8 is high seq

        Ok(Index {
               name: name.to_string(),
               high_doc_seq: Index::convert_bytes_to_u64(&value[8..]),
               rocks: rocks,
           })
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn new_snapshot(&self) -> Snapshot {
        Snapshot::new(RocksSnapshot::new(&self.rocks))
    }

    //This deletes the Rockdbs instance from disk
    pub fn drop(name: &str) -> Result<(), Error> {
        let ret = try!(rocksdb::DB::destroy(&rocksdb::Options::default(), name));
        Ok(ret)
    }

    pub fn add(&mut self, json: &str, mut batch: &mut Batch) -> Result<String, Error> {
        let mut shredder = Shredder::new();
        let (seq, docid) = if let Some(docid) = try!(shredder.shred(json)) {
            // user supplied doc id, see if we have an existing one.
            if batch.id_str_in_batch.contains(&docid) {
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
            let docid = Uuid::new(UuidVersion::Random)
                .unwrap()
                .simple()
                .to_string();
            try!(shredder.add_id(&docid));
            self.high_doc_seq += 1;
            (self.high_doc_seq, docid)
        };
        // now everything needs to be added to the batch,
        try!(shredder.add_all_to_batch(seq, &mut batch.wb));
        batch.id_str_in_batch.insert(docid.clone());

        Ok(docid)
    }

    /// Returns Ok(true) if the document was found and deleted, Ok(false) if it could not be found
    pub fn delete(&mut self, docid: &str, mut batch: &mut Batch) -> Result<bool, Error> {
        if batch.id_str_in_batch.contains(docid) {
            // oops use trying to delete a doc that's in the batch. Can't happen,
            return Err(Error::Write("Attempt to delete doc with same _id added earlier"
                                        .to_string()));
        }
        if let Some((seq, key_values)) = try!(self.gather_doc_fields(docid)) {
            let mut shredder = Shredder::new();
            try!(shredder.delete_existing_doc(docid, seq, key_values, &mut batch.wb));
            batch.id_str_in_batch.insert(docid.to_string());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Query the index with the string and use the parameters for values if passed.
    pub fn query(&self, query: &str, parameters: Option<String>) -> Result<QueryResults, Error> {
        QueryResults::new_query_results(query, parameters, self.new_snapshot())
    }

    fn gather_doc_fields(&self,
                         docid: &str)
                         -> Result<Option<(u64, BTreeMap<String, Vec<u8>>)>, Error> {
        if let Some(seq) = try!(self.fetch_seq(&docid)) {
            // collect up all the fields for the existing doc
            let kb = KeyBuilder::new();
            let value_key = kb.kp_value_key(seq);
            let mut key_values = BTreeMap::new();

            let mut iter = self.rocks.iterator(IteratorMode::Start);
            // Seek in index to >= entry
            iter.set_mode(IteratorMode::From(value_key.as_bytes(), rocksdb::Direction::Forward));
            loop {
                let (key, value) = match iter.next() {
                    Some((key, value)) => (key, value),
                    None => break,
                };

                if !key.starts_with(value_key.as_bytes()) {
                    break;
                }
                let key = unsafe { str::from_utf8_unchecked(&key) }.to_string();
                let value = value.iter().map(|i| *i).collect();
                key_values.insert(key, value);
            }
            return Ok(Some((seq, key_values)));
        } else {
            return Ok(None);
        }
    }

    // Store the current batch
    pub fn flush(&mut self, mut batch: Batch) -> Result<(), Error> {
        // Flush can only be called if the index is open

        let mut bytes = Vec::with_capacity(8 * 2);
        bytes
            .write(&Index::convert_u64_to_bytes(NOISE_HEADER_VERSION))
            .unwrap();
        bytes
            .write(&Index::convert_u64_to_bytes(self.high_doc_seq))
            .unwrap();
        try!(batch.wb.put(b"HDB", &bytes));

        let status = try!(self.rocks.write(batch.wb));
        Ok(status)
    }

    pub fn all_keys(&self) -> Result<Vec<String>, Error> {
        let mut results = Vec::new();
        for (key, _value) in self.rocks.iterator(rocksdb::IteratorMode::Start) {
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
        unsafe { mem::transmute(buffer) }
    }

    /// Should not be used generally since it not varint. Used for header fields
    /// since only one header is in the database it's not a problem with excess size.
    fn convert_u64_to_bytes(val: u64) -> [u8; 8] {
        unsafe { mem::transmute(val) }
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

    pub fn fetch_seq(&self, id: &str) -> Result<Option<u64>, Error> {

        let key = format!("{}{}", key_builder::KEY_PREFIX_ID_TO_SEQ, id);
        match try!(self.rocks.get(&key.as_bytes())) {
            // If there is an id, it's UTF-8
            Some(bytes) => Ok(Some(bytes.to_utf8().unwrap().parse().unwrap())),
            None => Ok(None),
        }
    }

    fn compaction_filter(_level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        if !(key[0] as char == key_builder::KEY_PREFIX_WORD_COUNT ||
             key[0] as char == key_builder::KEY_PREFIX_FIELD_COUNT) {
            return CompactionDecision::Keep;
        }
        if 0 == Index::convert_bytes_to_i32(&value) {
            CompactionDecision::Remove
        } else {
            CompactionDecision::Keep
        }
    }

    fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
        let value_prefixes = [key_builder::KEY_PREFIX_WORD,
                              key_builder::KEY_PREFIX_NUMBER,
                              key_builder::KEY_PREFIX_TRUE,
                              key_builder::KEY_PREFIX_FALSE,
                              key_builder::KEY_PREFIX_NULL];
        if value_prefixes.contains(&(a[0] as char)) && value_prefixes.contains(&(b[0] as char)) {
            let astr = unsafe { str::from_utf8_unchecked(&a) };
            let bstr = unsafe { str::from_utf8_unchecked(&b) };
            KeyBuilder::compare_keys(astr, bstr)
        } else {
            a.cmp(b)
        }
    }

    fn sum_merge(new_key: &[u8],
                 existing_val: Option<&[u8]>,
                 operands: &mut MergeOperands)
                 -> Vec<u8> {
        if !(new_key[0] as char == key_builder::KEY_PREFIX_FIELD_COUNT ||
             new_key[0] as char == key_builder::KEY_PREFIX_WORD_COUNT) {
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

pub struct IndexRWLock {
    raw: *const Index,
    lock: Mutex<Box<Index>>,
}

impl IndexRWLock {
    pub fn new(index: Index) -> IndexRWLock {
        let index = Box::new(index);
        IndexRWLock {
            raw: index.as_ref() as *const Index,
            lock: Mutex::new(index),
        }
    }

    pub fn read(&self) -> &Index {
        unsafe { &*self.raw }
    }

    pub fn write(&self) -> LockResult<MutexGuard<Box<Index>>> {
        self.lock.lock()
    }
}

unsafe impl Send for IndexRWLock {}
unsafe impl Sync for IndexRWLock {}

#[cfg(test)]
mod tests {
    extern crate rocksdb;
    use super::{Index, OpenOptions, Batch, IndexRWLock};
    use std::str;
    use std::sync::Arc;
    use snapshot::JsonFetcher;
    use json_value::JsonValue;
    use std::sync::mpsc::channel;
    use std::thread;

    #[test]
    fn test_open() {
        let dbname = "target/tests/firstnoisedb";
        let _ = Index::drop(dbname);

        let mut index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
        index.flush(Batch::new()).unwrap();
    }

    #[test]
    fn test_uuid() {
        let dbname = "target/tests/testuuid";
        let _ = Index::drop(dbname);
        let mut batch = Batch::new();

        let mut index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();

        let id = index.add(r#"{"foo":"bar"}"#, &mut batch).unwrap();

        index.flush(batch).unwrap();

        let mut results = index.query(r#"find {foo:=="bar"}"#, None).unwrap();
        let query_id = results.get_next_id().unwrap();
        assert!(query_id.len() == 32);
        assert_eq!(query_id, id);
    }

    #[test]
    fn test_compaction() {
        let dbname = "target/tests/testcompaction";
        let _ = Index::drop(dbname);
        let mut batch = Batch::new();

        let mut index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();

        let id = index.add(r#"{"foo":"bar"}"#, &mut batch).unwrap();
        index.flush(batch).unwrap();

        let mut batch = Batch::new();
        index.delete(&id, &mut batch).unwrap();
        index.flush(batch).unwrap();

        let rocks = &index.rocks;

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

        let mut index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = Batch::new();
        let _ = index
            .add(r#"{"_id":"1", "foo":"array", "baz": [1,2,[3,4,[5]]]}"#,
                 &mut batch)
            .unwrap();

        index.flush(batch).unwrap();
        {

            let mut results = Vec::new();
            for (key, value) in index.rocks.iterator(rocksdb::IteratorMode::Start) {
                if key[0] as char == 'V' {
                    let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
                    results.push((key_string, JsonFetcher::bytes_to_json_value(&*value)));
                }
            }

            let expected = vec![("V1#._id".to_string(), JsonValue::String("1".to_string())),
                                ("V1#.baz$0".to_string(), JsonValue::Number(1.0)),
                                ("V1#.baz$1".to_string(), JsonValue::Number(2.0)),
                                ("V1#.baz$2$0".to_string(), JsonValue::Number(3.0)),
                                ("V1#.baz$2$1".to_string(), JsonValue::Number(4.0)),
                                ("V1#.baz$2$2$0".to_string(), JsonValue::Number(5.0)),
                                ("V1#.foo".to_string(), JsonValue::String("array".to_string()))];
            assert_eq!(results, expected);
        }

        let mut batch = Batch::new();
        let _ = index
            .add(r#"{"_id":"1", "foo":"array", "baz": []}"#, &mut batch)
            .unwrap();
        index.flush(batch).unwrap();

        let mut results = Vec::new();
        for (key, value) in index.rocks.iterator(rocksdb::IteratorMode::Start) {
            if key[0] as char == 'V' {
                let key_string = unsafe { str::from_utf8_unchecked((&key)) }.to_string();
                results.push((key_string, JsonFetcher::bytes_to_json_value(&*value)));
            }
        }
        let expected = vec![("V1#._id".to_string(), JsonValue::String("1".to_string())),
                            ("V1#.baz".to_string(), JsonValue::Array(vec![])),
                            ("V1#.foo".to_string(), JsonValue::String("array".to_string()))];
        assert_eq!(results, expected);
    }

    #[test]
    fn test_empty_doc() {
        let dbname = "target/tests/testemptydoc";
        let _ = Index::drop(dbname);

        let mut index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = Batch::new();
        let id = index.add("{}", &mut batch).unwrap();

        index.flush(batch).unwrap();
        let query = r#"find {_id:==""#.to_string() + &id + "\"} return .";
        let mut results = index.query(&query, None).unwrap();
        let json = results.next().unwrap();
        assert_eq!(json,
                   JsonValue::Object(vec![("_id".to_string(), JsonValue::String(id))]));

    }

    #[test]
    fn test_index_rw_lock() {
        let dbname = "target/tests/testindexrwlock";
        let _ = Index::drop(dbname);

        let index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();

        let index = Arc::new(IndexRWLock::new(index));
        let index_write = index.clone();

        let (sender_w, receiver_r) = channel();
        let (sender_r, receiver_w) = channel();

        // Spawn off a writer
        thread::spawn(move || {
            let mut index = index_write.write().unwrap();
            let mut batch = Batch::new();

            // wait until the main thread has read access
            assert_eq!(1, receiver_w.recv().unwrap());
            println!("reader sent success 1");

            index
                .add(r#"{"_id":"1","foo":"bar"}"#, &mut batch)
                .unwrap();

            index.flush(batch).unwrap();
            println!("sending to reader");
            sender_w.send(2).unwrap();

            // wait until the main thread has successfully performed a query
            assert_eq!(3, receiver_w.recv().unwrap());
            println!("reader sent success 3");
        });

        let index = index.read();

        sender_r.send(1).unwrap();

        // wait until the writer thread has written something

        assert_eq!(2, receiver_r.recv().unwrap());
        println!("writer sent success 2");

        let mut iter = index.query(r#"find {foo: == "bar"}"#, None).unwrap();
        assert_eq!(JsonValue::String("1".to_string()), iter.next().unwrap());

        sender_r.send(3).unwrap();

    }
}
