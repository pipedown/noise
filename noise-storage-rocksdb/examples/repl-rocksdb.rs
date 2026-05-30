use noise_search::repl::repl;
use noise_storage_rocksdb::RocksDatabase;
use std::env;
use std::io::{self, BufReader};

fn main() {
    let test_mode = env::args().any(|argument| argument == "-t");
    repl::<RocksDatabase>(
        &mut BufReader::new(io::stdin()),
        &mut io::stdout(),
        test_mode,
    );
}
