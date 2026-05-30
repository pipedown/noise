//! Cross-backend test suite that every Noise storage backend must pass.
//!
//! A backend crate runs the whole suite with a single line in a test file:
//!
//! ```ignore
//! noise_tests::noise_backend_tests!(noise_storage_rocksdb::RocksDatabase);
//! ```
//!
//! To add a test that all backends should run, write a
//! `pub fn name<D: BackendDatabase>()` here and add one line to the
//! `noise_backend_tests!` macro at the bottom.

use std::io::{BufReader, Cursor};

use include_dir::{include_dir, Dir};

use noise_search::repl::repl;
use noise_storage::{BackendDatabase, DatabaseConfig};

/// The repl corpus is embedded so out-of-tree backends can run it without
/// access to this crate's source tree.
static REPL_CORPUS: Dir = include_dir!("$CARGO_MANIFEST_DIR/repl-tests");

/// Iteration must observe the numeric-aware key ordering defined by
/// `noise_storage::compare_keys`: seq numbers sort numerically, not lexically.
pub fn seq_ordering<D: BackendDatabase>() {
    let dbname = "target/tests/seq_ordering";
    let _ = D::destroy(dbname);
    let db = D::open(
        dbname,
        &DatabaseConfig {
            create_if_missing: true,
        },
    )
    .unwrap();

    let seqs: Vec<u64> = vec![1, 2, 9, 10, 11];
    for seq in &seqs {
        let key = format!("W.foo$!hello#{seq},");
        db.put(key.as_bytes(), b"v").unwrap();
    }

    let mut observed = Vec::new();
    let iter = db.iterator();
    for (key, _) in iter {
        let key_str = std::str::from_utf8(&key).unwrap();
        if !key_str.starts_with('W') {
            continue;
        }
        let after_hash = key_str.rsplit('#').next().unwrap();
        let seq_str = after_hash.trim_end_matches(',');
        observed.push(seq_str.parse::<u64>().unwrap());
    }

    assert_eq!(
        observed, seqs,
        "seq numbers should iterate in numeric order, got: {observed:?}",
    );
    let _ = D::destroy(dbname);
}

/// Runs every embedded `.noise` script through the repl in test mode and checks
/// the output matches the script verbatim. On mismatch it fails with the list
/// of offending files; regenerate them in-tree with `./update-test-repl.sh` and
/// review with `git diff`.
pub fn repl_corpus<D: BackendDatabase>() {
    let mut failures = 0;
    let mut total = 0;

    for file in REPL_CORPUS.files() {
        if file.path().extension().and_then(|ext| ext.to_str()) != Some("noise") {
            continue;
        }
        total += 1;
        let name = file.path().display();
        println!("About to run test {name}");

        let expected = file.contents();
        let mut actual = Vec::new();
        repl::<D>(
            &mut BufReader::new(Cursor::new(expected)),
            &mut actual,
            true,
        );

        if expected == actual.as_slice() {
            println!("{name} successful");
        } else {
            failures += 1;
            // The corpus is embedded, so we can't drop a .reject next to the
            // source script; write it to the temp dir for investigation instead.
            let reject = std::env::temp_dir().join(format!(
                "{}.reject",
                file.path().file_stem().unwrap().to_str().unwrap(),
            ));
            std::fs::write(&reject, &actual).unwrap();
            println!(
                "Repl test {name} failure. Failing output written to {}",
                reject.display(),
            );
        }
    }

    assert!(total > 0, "no .noise corpus files were embedded");
    assert!(
        failures == 0,
        "failed {failures} of {total} repl corpus files; \
         regenerate in-tree with ./update-test-repl.sh and review with git diff",
    );
}

/// Stamps out one `#[test]` per cross-backend case for the given backend type.
///
/// To add a case, write a `pub fn name<D: BackendDatabase>()` above and add a
/// `$crate::noise_backend_tests!(@case $db, name);` line below.
#[macro_export]
macro_rules! noise_backend_tests {
    ($db:ty) => {
        $crate::noise_backend_tests!(@case $db, seq_ordering);
        $crate::noise_backend_tests!(@case $db, repl_corpus);
    };
    (@case $db:ty, $name:ident) => {
        #[test]
        fn $name() {
            $crate::$name::<$db>();
        }
    };
}
