use std::env;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use noise_storage::{
    decode_byte_orderable_f64, encode_byte_orderable_f64, encode_byte_orderable_u64,
    get_length_prefixed_slice, put_length_prefixed_slice, BackendBatch, BackendDatabase,
    BackendSnapshot, DatabaseConfig, Namespace,
};

#[cfg(all(feature = "storage-rocksdb", feature = "storage-memory"))]
compile_error!("enable exactly one of storage-rocksdb / storage-memory");

#[cfg(feature = "storage-rocksdb")]
type Database = noise_storage_rocksdb::RocksDatabase;
#[cfg(feature = "storage-rocksdb")]
const BACKEND: &str = "rocksdb";

#[cfg(all(feature = "storage-memory", not(feature = "storage-rocksdb")))]
type Database = noise_storage_memory::MemoryDatabase;
#[cfg(all(feature = "storage-memory", not(feature = "storage-rocksdb")))]
const BACKEND: &str = "memory";

const RECT_BYTES: usize = 4 * std::mem::size_of::<f64>();
const KEYPATH: &str = "rects";

#[derive(Clone, Copy)]
struct Rect {
    x_min: f64,
    x_max: f64,
    y_min: f64,
    y_max: f64,
}

impl Rect {
    fn from_bytes(buf: &[u8; RECT_BYTES]) -> Self {
        let f = |o: usize| f64::from_le_bytes(buf[o..o + 8].try_into().unwrap());
        Self {
            x_min: f(0),
            x_max: f(8),
            y_min: f(16),
            y_max: f(24),
        }
    }

    fn to_bytes(self) -> [u8; RECT_BYTES] {
        let mut buf = [0u8; RECT_BYTES];
        buf[0..8].copy_from_slice(&self.x_min.to_le_bytes());
        buf[8..16].copy_from_slice(&self.x_max.to_le_bytes());
        buf[16..24].copy_from_slice(&self.y_min.to_le_bytes());
        buf[24..32].copy_from_slice(&self.y_max.to_le_bytes());
        buf
    }
}

fn read_rects(path: &Path) -> io::Result<Vec<Rect>> {
    let bytes = fs::read(path)?;
    let (chunks, remainder) = bytes.as_chunks::<RECT_BYTES>();
    if !remainder.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{}: file size {} is not a multiple of {}",
                path.display(),
                bytes.len(),
                RECT_BYTES,
            ),
        ));
    }
    Ok(chunks.iter().map(Rect::from_bytes).collect())
}

fn serialize_key(keypath: &str, seq: u64, rect: &Rect) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + keypath.len() + 8 + RECT_BYTES);
    put_length_prefixed_slice(&mut key, keypath.as_bytes());
    encode_byte_orderable_u64(&mut key, seq);
    encode_byte_orderable_f64(&mut key, rect.x_min);
    encode_byte_orderable_f64(&mut key, rect.x_max);
    encode_byte_orderable_f64(&mut key, rect.y_min);
    encode_byte_orderable_f64(&mut key, rect.y_max);
    key
}

fn serialize_query(keypath: &str, seq_min: u64, seq_max: u64, query: &Rect) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + keypath.len() + 16 + RECT_BYTES);
    put_length_prefixed_slice(&mut key, keypath.as_bytes());
    encode_byte_orderable_u64(&mut key, seq_min);
    encode_byte_orderable_u64(&mut key, seq_max);
    encode_byte_orderable_f64(&mut key, query.x_min);
    encode_byte_orderable_f64(&mut key, query.x_max);
    encode_byte_orderable_f64(&mut key, query.y_min);
    encode_byte_orderable_f64(&mut key, query.y_max);
    key
}

fn decode_rect(key: &[u8]) -> Rect {
    let (_keypath, end) = get_length_prefixed_slice(key).expect("malformed key");
    let mbb = &key[end + 8..];
    Rect {
        x_min: decode_byte_orderable_f64(&mbb[0..8]),
        x_max: decode_byte_orderable_f64(&mbb[8..16]),
        y_min: decode_byte_orderable_f64(&mbb[16..24]),
        y_max: decode_byte_orderable_f64(&mbb[24..32]),
    }
}

fn io_err(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(err.to_string())
}

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let (Some(data_path), Some(query_path), Some(result_path), None) =
        (args.next(), args.next(), args.next(), args.next())
    else {
        eprintln!("usage: rtree-verify-{BACKEND} <data> <query> <result>");
        std::process::exit(2);
    };

    let data = read_rects(Path::new(&data_path))?;
    let queries = read_rects(Path::new(&query_path))?;

    let db_path = format!("/tmp/rtree_verify_{BACKEND}");
    let _ = Database::destroy(&db_path);
    let db = Database::open(
        &db_path,
        &DatabaseConfig {
            create_if_missing: true,
        },
    )
    .map_err(io_err)?;

    let mut batch = db.new_batch();
    for (seq, rect) in data.iter().enumerate() {
        let key = serialize_key(KEYPATH, seq as u64, rect);
        batch.put(Namespace::Multidim, &key, b"").map_err(io_err)?;
    }
    db.write(batch).map_err(io_err)?;
    db.compact();

    let snapshot = db.snapshot();
    let mut out = BufWriter::new(fs::File::create(&result_path)?);
    let mut result_count = 0usize;
    for query in &queries {
        let query_key = serialize_query(KEYPATH, 0, u64::MAX, query);
        let mut cursor = snapshot.multidim_iterator(&query_key);
        while let Some((key, _value)) = cursor.current() {
            let rect = decode_rect(key);
            cursor.advance();
            out.write_all(&rect.to_bytes())?;
            result_count += 1;
        }
    }
    out.flush()?;

    println!(
        "{} data rects x {} queries -> {} results",
        data.len(),
        queries.len(),
        result_count,
    );
    Ok(())
}
