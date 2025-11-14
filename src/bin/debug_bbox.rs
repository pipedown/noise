use noise_search::index::{Batch, Index, OpenOptions};
extern crate rocksdb;
use std::convert::TryInto;

fn main() {
    // Clean start
    let dbname = "target/tests/debug_bbox_db";
    let _ = Index::drop(dbname);
    
    // Create index
    let mut index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
    let mut batch = Batch::new();
    
    // Add a point
    println!("Adding point at [10.9, 48.4]");
    index.add(r#"{"_id": "point1", "geometry": {"type": "Point", "coordinates": [10.9, 48.4]}}"#, &mut batch).unwrap();
    index.flush(batch).unwrap();
    
    println!("\nChecking what's stored in rtree:");
    let rtree_cf = index.rocks.cf_handle("rtree").unwrap();
    let iter = index.rocks.iterator_cf(rtree_cf, rocksdb::IteratorMode::Start).unwrap();
    
    for (key, _value) in iter {
        println!("Key length: {}", key.len());
        
        // Parse the key
        let mut offset = 0;
        
        // Read keypath length (varint)
        let keypath_len = key[offset] as usize;
        offset += 1;
        
        // Read keypath
        let keypath = &key[offset..offset + keypath_len];
        let keypath_str = String::from_utf8_lossy(keypath);
        println!("Keypath: '{}'", keypath_str);
        offset += keypath_len;
        
        // Read IID (u64)
        let iid_bytes: [u8; 8] = key[offset..offset + 8]
            .try_into()
            .expect("iid bytes");
        let iid = u64::from_le_bytes(iid_bytes);
        println!("IID: {}", iid);
        offset += 8;

        // Read bounding box (4 f64 values)
        if key.len() >= offset + 32 {
            let mut bbox = [0.0_f64; 4];
            for (idx, value) in bbox.iter_mut().enumerate() {
                let start = offset + (idx * 8);
                let bytes: [u8; 8] = key[start..start + 8]
                    .try_into()
                    .expect("bbox bytes");
                *value = f64::from_le_bytes(bytes);
            }
            println!("Bounding box: [{}, {}, {}, {}]", bbox[0], bbox[1], bbox[2], bbox[3]);
            
            // Check if point [10.9, 48.4] should be inside various test bboxes
            println!("\nChecking query matches:");
            let test_bboxes = vec![
                ([-1000.0, -1000.0, 99.0, 1000.0], "[-1000, -1000, 99, 1000]"),
                ([0.0, 0.0, 20.0, 50.0], "[0, 0, 20, 50]"),
                ([10.0, 48.0, 11.0, 49.0], "[10, 48, 11, 49]"),
            ];
            
            for (query_bbox, desc) in test_bboxes {
                let intersects = bbox[0] <= query_bbox[2] && bbox[2] >= query_bbox[0] &&
                                bbox[1] <= query_bbox[3] && bbox[3] >= query_bbox[1];
                println!("  Query {}: intersects = {}", desc, intersects);
            }
        }
    }
}
