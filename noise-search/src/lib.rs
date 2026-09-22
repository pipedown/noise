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
// switching backends is a one-line change. Rocksdb is covered by the
// cross-backend suite in `noise-storage-rocksdb/tests/backend.rs`.
#[cfg(test)]
mod test_backend {
    pub use noise_storage_memory::MemoryDatabase as Database;
}
