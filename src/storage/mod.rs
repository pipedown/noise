#[cfg(feature = "storage-rocksdb")]
pub type Database = noise_storage_rocksdb::RocksDatabase;

#[cfg(test)]
mod tests {
    use noise_storage::{BackendDatabase, DatabaseConfig};

    use super::Database;

    #[test]
    fn seq_ordering() {
        let dbname = "target/tests/seq_ordering";
        let _ = Database::destroy(dbname);
        let db = Database::open(
            dbname,
            &DatabaseConfig {
                create_if_missing: true,
            },
        )
        .unwrap();

        let seqs: Vec<u64> = vec![1, 2, 9, 10, 11];
        for seq in &seqs {
            let key = format!("W.foo$!hello#{},", seq);
            db.put(key.as_bytes(), b"v").unwrap();
        }

        let mut observed_seqs = Vec::new();
        let iter = db.iterator();
        for (key, _) in iter {
            let key_str = std::str::from_utf8(&key).unwrap();
            if !key_str.starts_with('W') {
                continue;
            }
            let after_hash = key_str.rsplit('#').next().unwrap();
            let seq_str = after_hash.trim_end_matches(',');
            observed_seqs.push(seq_str.parse::<u64>().unwrap());
        }

        assert_eq!(
            observed_seqs, seqs,
            "seq numbers should be in numeric order, got: {:?}",
            observed_seqs,
        );

        let _ = Database::destroy(dbname);
    }
}
