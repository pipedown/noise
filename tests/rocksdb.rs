extern crate rocksdb;
use rocksdb::{DB};

#[test]
fn rocksdb_works() {
    let db = DB::open_default("target/tests/rocksdb/db").unwrap();
    let mut status = db.put(b"first", b"this is cool");
    assert!(status.is_ok(), "Putting a key-value pair was successful");

    let value = db.get(b"first");
    assert_eq!(Some("this is cool"), value.unwrap().unwrap().to_utf8());

    status = db.delete(b"first");
    assert!(status.is_ok(), "Deletion of key was successful");
}
