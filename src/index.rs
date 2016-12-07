extern crate rocksdb;

use std::collections::HashMap;
use std::str;

use records_capnp::header;

use error::Error;
use json_shred::{Shredder};

const NOISE_HEADER_VERSION: u64 = 1;

struct Header {
    version: u64,
    high_seq: u64,
}

impl Header {
    fn new() -> Header {
        Header{
            version: NOISE_HEADER_VERSION,
            high_seq: 0,
        }
    }
    fn serialize(&self) -> Vec<u8> {
        let mut message = ::capnp::message::Builder::new_default();
        {
            let mut header = message.init_root::<header::Builder>();
            header.set_version(self.version);
            header.set_high_seq(self.high_seq);
        }
        let mut bytes = Vec::new();
        ::capnp::serialize_packed::write_message(&mut bytes, &message).unwrap();
        bytes
    }
}


pub struct Index {
    write_options: rocksdb::WriteOptions,
    high_doc_seq: u64,
    pub rocks: Option<rocksdb::DB>,
    id_str_to_id_seq: HashMap<String, String>,
    batch: Option<rocksdb::WriteBatch>,
}

pub enum OpenOptions {
    Create
}

impl Index {
    pub fn new() -> Index {
        Index {
            write_options: rocksdb::WriteOptions::new(),
            high_doc_seq: 0,
            rocks: None,
            id_str_to_id_seq: HashMap::new(),
            batch: Some(rocksdb::WriteBatch::default()),
        }
    }
    // NOTE vmx 2016-10-13: Perhpas the name should be specified on `new()` as it is bound
    // to a single instance. The next question would then be if `open()` really makes sense
    // or if it should be combined with `new()`.
    //fn open(&mut self, name: &str, open_options: Option<OpenOptions>) -> Result<DB, String> {
    pub fn open(&mut self, name: &str, open_options: Option<OpenOptions>) -> Result<(), Error> {
        let mut rocks_options = rocksdb::Options::default();
        println!("still here1");
        let rocks = match rocksdb::DB::open(&rocks_options, name) {
            Ok(rocks) => rocks,
            Err(error) => {
                match open_options {
                    Some(OpenOptions::Create) => (),
                    _ => return Err(Error::Rocks(error)),
                }

                rocks_options.create_if_missing(true);
                rocks_options.set_comparator("noise", Index::compare_keys);

                let rocks = try!(rocksdb::DB::open(&rocks_options, name));

                let header = Header::new();
                let status = rocks.put_opt(b"HDB", &*header.serialize(), &self.write_options);
                println!("put was ok? {}", status.is_ok());
                rocks
            }
        };

        // validate header is there
        let value = try!(rocks.get(b"HDB")).unwrap();
        // NOTE vmx 2016-10-13: I'm not really sure why the dereferencing is needed
        // and why we pass on mutable reference of it to `read_message()`
        let mut ref_value = &*value;
        let message_reader = ::capnp::serialize_packed::read_message(
            &mut ref_value, ::capnp::message::ReaderOptions::new()).unwrap();
        let header = message_reader.get_root::<header::Reader>().unwrap();
        assert_eq!(header.get_version(), NOISE_HEADER_VERSION);
        self.high_doc_seq = header.get_high_seq();
        self.rocks = Some(rocks);
        Ok(())
    }

    // NOTE vmx 2016-10-13: As one index is tied to one database, this should be a method
    // without a parameter
    pub fn delete(name: &str) -> Result<(), Error> {
        let ret = try!(rocksdb::DB::destroy(&rocksdb::Options::default(), name));
        Ok(ret)
    }

    pub fn add(&mut self, json: &str) -> Result<(), Error> {
        let mut shredder = Shredder::new();
        // NOTE vmx 2016-10-13: Needed for the lifetime-checker, though not sure if it now really
        // does the right thing. Does the `try!()` still return as epected?
        {
            let docid = try!(shredder.shred(json, self.high_doc_seq + 1,
                                            self.batch.as_mut().unwrap()));
            self.high_doc_seq += 1;
            self.id_str_to_id_seq.insert(format!("I{}", docid), format!("S{}", self.high_doc_seq));
        }
        Ok(())
    }

    // Store the current batch
    pub fn flush(&mut self) -> Result<(), Error> {
        // Flush can only be called if the index is open
        // NOTE vmx 2016-10-17: Perhaps that shouldn't panic?
        assert!(&self.rocks.is_some());
        let rocks = self.rocks.as_ref().unwrap();

        // Look up all doc ids and 'delete' from the seq_to_ids keyspace
        for key in self.id_str_to_id_seq.keys() {
            // TODO vmx 2016-10-17: USe multiget once the Rusts wrapper supports it
            match rocks.get(key.as_bytes()) {
                Ok(Some(seq)) => {
                    try!(self.batch.as_mut().unwrap().delete(&*seq));
                },
                _ => {}
            }
        }

        // Add the ids_to_seq keyspace entries
        for (id, seq) in &self.id_str_to_id_seq {
            try!(self.batch.as_mut().unwrap().put(id.as_bytes(), seq.as_bytes()));
            try!(self.batch.as_mut().unwrap().put(seq.as_bytes(), id.as_bytes()));
        }

        let mut header = Header::new();
        header.high_seq = self.high_doc_seq;
        try!(self.batch.as_mut().unwrap().put(b"HDB", &*header.serialize()));

        let status = try!(rocks.write(self.batch.take().unwrap()));
        // Make sure there's a always a valid WriteBarch after writing it into RocksDB,
        // else calls to `self.batch.as_mut().unwrap()` would panic.
        self.batch = Some(rocksdb::WriteBatch::default());
        self.id_str_to_id_seq.clear();
        Ok(status)
    }

    pub fn fetch_id(&self, seq: u64) ->  Result<Option<String>, String> {
        // Fetching an ID is only possible if the index is open
        // NOTE vmx 2016-10-17: Perhaps that shouldn't panic?
        assert!(&self.rocks.is_some());
        let rocks = self.rocks.as_ref().unwrap();

        let key = format!("S{}", seq);
        match try!(rocks.get(&key.as_bytes())) {
            // If there is an id, it's UTF-8
            Some(id) => Ok(Some(id.to_utf8().unwrap().to_string())),
            None => Ok(None)
        }
    }

    fn compare_keys(a: &[u8], b: &[u8]) -> i32 {
        use std::cmp::Ordering;
        use key_builder::KeyBuilder;
        if a[0] == 'W' as u8 && b[0] == 'W' as u8 {
            let astr = unsafe {str::from_utf8_unchecked(&a)};
            let bstr = unsafe {str::from_utf8_unchecked(&b)};
            KeyBuilder::compare_keys(astr, bstr)
        } else {
            match a.cmp(b) {
                Ordering::Less    => -1,
                Ordering::Greater =>  1,
                Ordering::Equal   =>  0,
            }
        }
    }
}


#[cfg(test)]
mod tests {
    extern crate rocksdb;
    use super::{Index, OpenOptions};

    #[test]
    fn test_open() {
        let mut index = Index::new();
        //let db = super::Index::open("firstnoisedb", Option::None).unwrap();
        index.open("target/tests/firstnoisedb", Some(OpenOptions::Create)).unwrap();
        index.flush().unwrap();
    }
}
