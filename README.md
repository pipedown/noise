Noise
=====

Nested Object Inverted Search Engine

This is a native library meant to be linked into other projects and will
expose a C API.

It's a full text index and query system that understands the semi structured
nature of JSON, and will allow:

 * Stemmed word/fuzzy match
 * Case sensitive exact word and sentence match
 * Arbitrary boolean nesting
 * Greater than/less than matching
 
[Query Language Reference here](https://github.com/pipedown/noise/blob/master/query_language_reference.md)


Installation
------------

### Dependencies

 * [RocksDB](http://rocksdb.org/)
 * [capnp-tool](https://capnproto.org/capnp-tool.html) 


### Build

    cargo build


### Running tests

    cargo test


Storage backends
----------------

The search engine is generic over its storage backend. The reference backend is
[RocksDB](http://rocksdb.org/), provided by the `noise-storage-rocksdb` crate,
which also ships a `repl-rocksdb` example (run it with
`cargo run --example repl-rocksdb -p noise-storage-rocksdb`).

A backend is any type implementing the traits in the `noise-storage` crate:
`BackendDatabase`, `BackendBatch`, `BackendSnapshot` and `CursorBackend`. To\
plug in your own, implement those traits in your *own* crate and use the
generic engine types directly, with no changes to this repository:

    use noise_search::index::{Index, OpenOptions};

    let mut index = Index::<MyBackend>::open("mydb", Some(OpenOptions::Create))?;

The engine crate `noise_search` depends only on `noise-storage`.

`noise-storage-rocksdb` is the reference implementation to model a backend on.
To check your backend against the suite every backend must pass, add
`noise-tests` as a dev-dependency and invoke its macro from a test (as
`noise-storage-rocksdb` does in `tests/backend.rs`):

    noise_tests::noise_backend_tests!(MyBackend);


Contributing
------------

### Commit messages

Commit messages are important as soon as you need to dig into the history
of certain parts of the system. Hence please follow the guidelines of
[How to Write a Git Commit Message](http://chris.beams.io/posts/git-commit/).


License
-------

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

