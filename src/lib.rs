mod aggregates;
mod error;
mod filters;
pub mod index;
mod json_shred;
pub mod json_value;
mod key_builder;
mod parser;
pub mod query;
pub mod repl;
mod returnable;
mod snapshot;
mod stems;

// Backend the engine's own unit tests run against. Centralised here so that
// switching backends (for example to an in-memory one) is a one-line change.
#[cfg(test)]
mod test_backend {
    pub use noise_storage_rocksdb::RocksDatabase as Database;
}
