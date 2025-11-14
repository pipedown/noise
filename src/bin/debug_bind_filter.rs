use noise_search::index::{Batch, Index, OpenOptions};
extern crate rocksdb;
use std::convert::TryInto;

fn main() {
    let dbname = "target/tests/debug_bind_filter";
    let _ = Index::drop(dbname);
    
    let mut index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
    let mut batch = Batch::new();
    
    // Add document with array of points
    index.add(r#"{"_id": "arraypoint", "area": [{"type": "Point", "coordinates": [10.9, 48.4]}, {"type": "Point", "coordinates": [-5.0, -20.1]}]}"#, &mut batch).unwrap();
    index.flush(batch).unwrap();
    
    // Check what's in the rtree
    println!("=== RTree entries ===");
    let rtree_cf = index.rocks.cf_handle("rtree").unwrap();
    let iter = index.rocks.iterator_cf(rtree_cf, rocksdb::IteratorMode::Start).unwrap();
    
    for (key, value) in iter {
        // Parse key
        let mut offset = 0;
        let keypath_len = key[offset] as usize;
        offset += 1;
        let keypath = String::from_utf8_lossy(&key[offset..offset + keypath_len]);
        offset += keypath_len;
        
        let iid_bytes: [u8; 8] = key[offset..offset + 8]
            .try_into()
            .expect("iid bytes");
        let iid = u64::from_le_bytes(iid_bytes);
        offset += 8;

        let mut bbox = [0.0_f64; 4];
        for (idx, value) in bbox.iter_mut().enumerate() {
            let start = offset + (idx * 8);
            let bytes: [u8; 8] = key[start..start + 8]
                .try_into()
                .expect("bbox bytes");
            *value = f64::from_le_bytes(bytes);
        }

        // Parse value (arraypath)
        let arraypath = if value.len() >= 8 {
            let bytes: [u8; 8] = value[..8].try_into().expect("array path bytes");
            vec![u64::from_le_bytes(bytes)]
        } else {
            vec![]
        };
        
        println!("Keypath: '{}', IID: {}, BBox: {:?}, ArrayPath: {:?}", keypath, iid, bbox, arraypath);
    }
    
    // Check main CF entries for area
    println!("\n=== Main CF area entries ===");
    let main_iter = index.rocks.iterator(rocksdb::IteratorMode::Start);
    for (key, _value) in main_iter {
        let key_str = String::from_utf8_lossy(&key);
        if key_str.contains(".area") && key_str.contains("coordinates") {
            println!("  {}", key_str);
        }
    }
}
