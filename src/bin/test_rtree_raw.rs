use noise_search::index::{Batch, Index, OpenOptions};
extern crate rocksdb;
use rocksdb::IteratorMode;
use std::convert::TryInto;

fn decode_rtree_key(key: &[u8]) -> (String, u64) {
    let mut offset = 0;
    
    // Read keypath length (varint)
    let keypath_len = key[offset] as usize;
    offset += 1;
    
    // Read keypath
    let keypath = &key[offset..offset + keypath_len];
    let keypath_str = String::from_utf8_lossy(keypath).to_string();
    offset += keypath_len;
    
    // Read IID (u64)
    let iid_bytes: [u8; 8] = key[offset..offset + 8]
        .try_into()
        .expect("iid bytes");
    let iid = u64::from_le_bytes(iid_bytes);
    
    (keypath_str, iid)
}

fn main() {
    let dbname = "target/tests/test_rtree_raw";
    let _ = Index::drop(dbname);
    
    let mut index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
    let mut batch = Batch::new();
    
    // Add test documents
    println!("Adding documents...");
    index.add(r#"{"_id": "point1", "geometry": {"type": "Point", "coordinates": [10.9, 48.4]}}"#, &mut batch).unwrap();
    index.add(r#"{"_id": "point2", "geometry": {"type": "Point", "coordinates": [50.0, 50.0]}}"#, &mut batch).unwrap();
    index.add(r#"{"_id": "point3", "other": {"type": "Point", "coordinates": [10.0, 10.0]}}"#, &mut batch).unwrap();
    index.flush(batch).unwrap();
    
    // Test 1: List all rtree entries
    println!("\n=== All RTree entries ===");
    let rtree_cf = index.rocks.cf_handle("rtree").unwrap();
    let iter = index.rocks.iterator_cf(rtree_cf, IteratorMode::Start).unwrap();
    for (key, _value) in iter {
        let (keypath, iid) = decode_rtree_key(&key);
        println!("  Keypath: '{}', IID: {}", keypath, iid);
    }
    
    // Test 2: Test RTree iterator with query
    println!("\n=== Testing RTree spatial query ===");
    let snapshot = index.rocks.snapshot();
    
    // Build a query bbox that should match point1
    let query_bbox = vec![
        0.0f64.to_le_bytes().to_vec(),    // west
        0.0f64.to_le_bytes().to_vec(),    // south
        20.0f64.to_le_bytes().to_vec(),   // east
        50.0f64.to_le_bytes().to_vec(),   // north
    ].concat();
    
    println!("Query bbox: [0, 0, 20, 50] (should match point1)");
    
    // Create the full query key
    let keypath = ".geometry";
    let mut query = Vec::new();
    query.push(keypath.len() as u8);
    query.extend_from_slice(keypath.as_bytes());
    query.extend_from_slice(&0u64.to_le_bytes());      // min IID
    query.extend_from_slice(&u64::MAX.to_le_bytes()); // max IID
    query.extend_from_slice(&query_bbox);
    
    println!("Query key length: {}", query.len());
    
    let rtree_iter = snapshot.rtree_iterator(&query);
    let mut count = 0;
    for (key, _value) in rtree_iter {
        count += 1;
        let (keypath, iid) = decode_rtree_key(&key);
        println!("  Result {}: Keypath: '{}', IID: {}", count, keypath, iid);
    }
    
    if count == 0 {
        println!("  No results returned by RTree iterator!");
    }
}
