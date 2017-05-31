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

