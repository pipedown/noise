use noise_search::index::{Index, OpenOptions};
use noise_storage::{BackendBatch, BackendDatabase, Namespace};

type Idx = Index<noise_search::storage::Database>;

#[test]
fn rocksdb_works() {
    let dbname = "target/tests/rocksdb/db";
    let _ = Idx::drop(dbname);
    let index = Idx::open(dbname, Some(OpenOptions::Create)).unwrap();

    index.db.put(b"first", b"this is cool").unwrap();

    let value = index.db.get(b"first").unwrap();
    assert_eq!(value.unwrap(), b"this is cool");

    let mut batch = index.db.new_batch();
    batch.delete(Namespace::Default, b"first").unwrap();
    index.db.write(batch).unwrap();

    let value = index.db.get(b"first").unwrap();
    assert!(value.is_none());

    drop(index);
    let _ = Idx::drop(dbname);
}
