extern crate uuid;

use self::uuid::Uuid;
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::str;

use std::sync::{LockResult, Mutex, MutexGuard};

use crate::error::Error;
use crate::json_shred::{KeyValues, Shredder};
use crate::key_builder::{self, KeyBuilder};
use crate::query::QueryResults;
use crate::snapshot::Snapshot;
use noise_storage::{BackendBatch, BackendDatabase, DatabaseConfig, Namespace, SeekFrom};

const NOISE_HEADER_VERSION: u64 = 1;

pub struct Index<D: BackendDatabase> {
    name: String,
    high_doc_seq: u64,
    pub db: D,
}

pub struct Batch<D: BackendDatabase> {
    wb: D::Batch,
    id_str_in_batch: HashSet<String>,
}

impl<D: BackendDatabase> Batch<D> {
    fn new(wb: D::Batch) -> Batch<D> {
        Batch {
            wb,
            id_str_in_batch: HashSet::new(),
        }
    }
}

pub enum OpenOptions {
    Create,
}

impl<D: BackendDatabase> Index<D> {
    pub fn open(name: &str, open_options: Option<OpenOptions>) -> Result<Index<D>, Error> {
        let create = matches!(open_options, Some(OpenOptions::Create));

        let config = DatabaseConfig {
            create_if_missing: create,
        };

        let db = D::open(name, &config)?;

        // If we just created the database, write the header
        if db.get(b"HDB")?.is_none() {
            let mut bytes = Vec::with_capacity(8 * 2);
            bytes.write_all(&NOISE_HEADER_VERSION.to_le_bytes())?;
            bytes.write_all(&0u64.to_le_bytes())?;
            db.put(b"HDB", &bytes)?;
        }

        // validate header is there
        let value = db.get(b"HDB")?.unwrap();
        assert_eq!(value.len(), 8 * 2);
        // first 8 is version
        assert_eq!(convert_bytes_to_u64(&value[..8]), NOISE_HEADER_VERSION);
        // next 8 is high seq

        Ok(Index {
            name: name.to_string(),
            high_doc_seq: convert_bytes_to_u64(&value[8..]),
            db,
        })
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn new_snapshot(&self) -> Snapshot<D::Snapshot> {
        Snapshot::new(self.db.snapshot())
    }

    pub fn new_batch(&self) -> Batch<D> {
        Batch::new(self.db.new_batch())
    }

    /// Write a storage batch directly (used by tests in json_shred)
    pub fn write_batch(&self, batch: D::Batch) -> Result<(), Error> {
        self.db.write(batch).map_err(Into::into)
    }

    //This deletes the Rockdbs instance from disk
    pub fn drop(name: &str) -> Result<(), Error> {
        D::destroy(name).map_err(Into::into)
    }

    pub fn add(&mut self, json: &str, batch: &mut Batch<D>) -> Result<String, Error> {
        let mut shredder = Shredder::new();
        let (seq, docid) = if let Some(docid) = shredder.shred(json)? {
            // user supplied doc id, see if we have an existing one.
            if batch.id_str_in_batch.contains(&docid) {
                // oops use trying to add some doc 2x to this batch.
                return Err(Error::Write(
                    "Attempt to insert multiple docs with same _id".to_string(),
                ));
            }
            if let Some((seq, existing_key_values)) = self.gather_doc_fields(&docid)? {
                shredder.merge_existing_doc(existing_key_values);
                (seq, docid)
            } else {
                // no existing document found, so we use the one supplied.
                self.high_doc_seq += 1;
                (self.high_doc_seq, docid)
            }
        } else {
            // no doc id supplied in document, so we create a random one.
            let docid = Uuid::new_v4().simple().to_string();
            shredder.add_id(&docid)?;
            self.high_doc_seq += 1;
            (self.high_doc_seq, docid)
        };
        // now everything needs to be added to the batch,
        shredder.add_all_to_batch(seq, &mut batch.wb)?;
        batch.id_str_in_batch.insert(docid.clone());

        Ok(docid)
    }

    /// Returns Ok(true) if the document was found and deleted, Ok(false) if it could not be found
    pub fn delete(&mut self, docid: &str, batch: &mut Batch<D>) -> Result<bool, Error> {
        if batch.id_str_in_batch.contains(docid) {
            // oops use trying to delete a doc that's in the batch. Can't happen,
            return Err(Error::Write(
                "Attempt to delete doc with same _id added earlier".to_string(),
            ));
        }
        if let Some((seq, key_values)) = self.gather_doc_fields(docid)? {
            let mut shredder = Shredder::new();
            shredder.delete_existing_doc(docid, seq, key_values, &mut batch.wb)?;
            batch.id_str_in_batch.insert(docid.to_string());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Query the index with the string and use the parameters for values if passed.
    pub fn query(
        &self,
        query: &str,
        parameters: Option<String>,
    ) -> Result<QueryResults<D::Snapshot>, Error> {
        QueryResults::new_query_results(query, parameters, self.new_snapshot())
    }

    fn gather_doc_fields(&self, docid: &str) -> Result<Option<(u64, KeyValues)>, Error> {
        if let Some(seq) = self.fetch_seq(docid)? {
            // collect up all the fields for the existing doc
            let kb = KeyBuilder::new();
            let value_key = kb.kp_value_key(seq);
            let mut key_values = BTreeMap::new();

            let mut iter = self.db.iterator();
            // Seek in index to >= entry
            iter.seek(SeekFrom::Key(value_key.as_bytes()));
            for (key, value) in iter {
                if !key.starts_with(value_key.as_bytes()) {
                    break;
                }
                let key = unsafe { str::from_utf8_unchecked(&key) }.to_string();
                let value = value.to_vec();
                key_values.insert(key, value);
            }
            Ok(Some((seq, key_values)))
        } else {
            Ok(None)
        }
    }

    // Store the current batch
    pub fn flush(&mut self, mut batch: Batch<D>) -> Result<(), Error> {
        // Flush can only be called if the index is open

        let mut bytes = Vec::with_capacity(8 * 2);
        bytes.write_all(&NOISE_HEADER_VERSION.to_le_bytes())?;
        bytes.write_all(&self.high_doc_seq.to_le_bytes())?;
        batch.wb.put(Namespace::Default, b"HDB", &bytes)?;

        self.db.write(batch.wb).map_err(Into::into)
    }

    pub fn all_keys(&self) -> Result<Vec<String>, Error> {
        let mut results = Vec::new();
        for (key, _value) in self.db.iterator() {
            let key_string = unsafe { str::from_utf8_unchecked(&key) }.to_string();
            results.push(key_string);
        }
        Ok(results)
    }

    pub fn fetch_seq(&self, id: &str) -> Result<Option<u64>, Error> {
        let key = format!("{}{}", key_builder::KEY_PREFIX_ID_TO_SEQ, id);
        match self.db.get(key.as_bytes())? {
            // If there is an id, it's UTF-8
            Some(bytes) => Ok(Some(str::from_utf8(&bytes).unwrap().parse().unwrap())),
            None => Ok(None),
        }
    }
}

/// Should not be used generally since it not varint. Used for header fields
/// since only one header is in the database it's not a problem with excess size.
fn convert_bytes_to_u64(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() == 8);
    let mut buffer = [0; 8];
    for (n, b) in bytes.iter().enumerate() {
        buffer[n] = *b;
    }
    u64::from_ne_bytes(buffer)
}

/// Used for types where a single writer is allowed to be concurrent with multiple
/// readers. Unlike RwLock, where a writer gets exlusive access to and blocks readers.
pub struct MvccRwLock<T> {
    raw: *const T,
    lock: Mutex<Box<T>>,
}

impl<T> MvccRwLock<T> {
    pub fn new(t: T) -> MvccRwLock<T> {
        let t = Box::new(t);
        MvccRwLock {
            raw: t.as_ref() as *const T,
            lock: Mutex::new(t),
        }
    }

    pub fn read(&self) -> &T {
        unsafe { &*self.raw }
    }

    pub fn write(&self) -> LockResult<MutexGuard<'_, Box<T>>> {
        self.lock.lock()
    }
}

unsafe impl<T> Send for MvccRwLock<T> {}
unsafe impl<T> Sync for MvccRwLock<T> {}

#[cfg(test)]
mod tests {
    use super::{Index, MvccRwLock, OpenOptions};
    use crate::json_value::JsonValue;
    use crate::snapshot::JsonFetcher;
    use crate::storage::Database;
    use noise_storage::BackendDatabase;
    use std::str;
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread;

    type Idx = Index<Database>;

    #[test]
    fn test_open() {
        let dbname = "target/tests/firstnoisedb";
        let _ = Idx::drop(dbname);

        let mut index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();
        index.flush(index.new_batch()).unwrap();
    }

    #[test]
    fn test_uuid() {
        let dbname = "target/tests/testuuid";
        let _ = Idx::drop(dbname);
        let mut index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();
        let mut batch = index.new_batch();

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
        let _ = Idx::drop(dbname);
        let mut index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();
        let mut batch = index.new_batch();

        let id = index.add(r#"{"foo":"bar"}"#, &mut batch).unwrap();
        index.flush(batch).unwrap();

        let mut batch = index.new_batch();
        index.delete(&id, &mut batch).unwrap();
        index.flush(batch).unwrap();

        // apparently you need to do compaction twice when there are merges
        // first one lets the merges happen, the second lets them be collected.
        // this is acceptable since eventually the keys go away.
        // if this test fails non-deterministically we might have a problem.
        index.db.compact();
        index.db.compact();

        let mut iter = index.db.iterator();
        let (key, _value) = iter.next().unwrap();
        assert!(key.starts_with(&b"HDB"[..]));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_updates() {
        let dbname = "target/tests/testupdates";
        let _ = Idx::drop(dbname);

        let mut index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = index.new_batch();
        let _ = index
            .add(
                r#"{"_id":"1", "foo":"array", "baz": [1,2,[3,4,[5]]]}"#,
                &mut batch,
            )
            .unwrap();

        index.flush(batch).unwrap();
        {
            let mut results = Vec::new();
            for (key, value) in index.db.iterator() {
                if key[0] as char == 'V' {
                    let key_string = unsafe { str::from_utf8_unchecked(&key) }.to_string();
                    results.push((key_string, JsonFetcher::bytes_to_json_value(&value)));
                }
            }

            let expected = vec![
                ("V1#._id".to_string(), JsonValue::String("1".to_string())),
                ("V1#.baz$0".to_string(), JsonValue::Number(1.0)),
                ("V1#.baz$1".to_string(), JsonValue::Number(2.0)),
                ("V1#.baz$2$0".to_string(), JsonValue::Number(3.0)),
                ("V1#.baz$2$1".to_string(), JsonValue::Number(4.0)),
                ("V1#.baz$2$2$0".to_string(), JsonValue::Number(5.0)),
                (
                    "V1#.foo".to_string(),
                    JsonValue::String("array".to_string()),
                ),
            ];
            assert_eq!(results, expected);
        }

        let mut batch = index.new_batch();
        let _ = index
            .add(r#"{"_id":"1", "foo":"array", "baz": []}"#, &mut batch)
            .unwrap();
        index.flush(batch).unwrap();

        let mut results = Vec::new();
        for (key, value) in index.db.iterator() {
            if key[0] as char == 'V' {
                let key_string = unsafe { str::from_utf8_unchecked(&key) }.to_string();
                results.push((key_string, JsonFetcher::bytes_to_json_value(&value)));
            }
        }
        let expected = vec![
            ("V1#._id".to_string(), JsonValue::String("1".to_string())),
            ("V1#.baz".to_string(), JsonValue::Array(vec![])),
            (
                "V1#.foo".to_string(),
                JsonValue::String("array".to_string()),
            ),
        ];
        assert_eq!(results, expected);
    }

    #[test]
    fn test_empty_doc() {
        let dbname = "target/tests/testemptydoc";
        let _ = Idx::drop(dbname);

        let mut index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();

        let mut batch = index.new_batch();
        let id = index.add("{}", &mut batch).unwrap();

        index.flush(batch).unwrap();
        let query = r#"find {_id:==""#.to_string() + &id + "\"} return .";
        let mut results = index.query(&query, None).unwrap();
        let json = results.next().unwrap();
        assert_eq!(
            json,
            JsonValue::Object(vec![("_id".to_string(), JsonValue::String(id))])
        );
    }

    #[test]
    fn test_index_rw_lock() {
        let dbname = "target/tests/testindexrwlock";
        let _ = Idx::drop(dbname);

        let index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();

        let index = Arc::new(MvccRwLock::new(index));
        let index_write = index.clone();

        let (sender_w, receiver_r) = channel();
        let (sender_r, receiver_w) = channel();

        // Spawn off a writer
        thread::spawn(move || {
            let mut index = index_write.write().unwrap();
            let mut batch = index.new_batch();

            // wait until the main thread has read access
            assert_eq!(1, receiver_w.recv().unwrap());
            println!("reader sent success 1");

            index.add(r#"{"_id":"1","foo":"bar"}"#, &mut batch).unwrap();

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
