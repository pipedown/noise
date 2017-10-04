extern crate noise_search;

use std::io::{Read, Write, BufReader};
use std::fs::{self, File};
use std::env;

use noise_search::repl::repl;

#[test]
fn test_repl() {
    // We load up tests scripts from repl-tests and evaluate them. The output should be idenitical
    // to the test script files. If not, then the test is failed and a new file is written with
    // .reject extension in the same directory where it can be investigated.

    // To update the test files with new command and output, simply edit/add commands and run
    // update-test-repl.sh script from the project root directory. Then examin or do a git diff to
    // see if the output is as expected.

    let mut test_dir = env::current_dir().unwrap();
    test_dir.push("repl-tests");
    let mut failures = 0;
    let mut total = 0;
    // Sort files by last modified date to make debugging easier
    let mut entries: Vec<_> = fs::read_dir(test_dir)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    entries.sort_by_key(|entry| entry.metadata().unwrap().modified().unwrap());
    entries.reverse();
    for entry in entries {
        let mut path = entry.path();
        if path.extension().unwrap().to_str().unwrap() != "noise" {
            continue;
        }
        total += 1;
        let test_name = path.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        println!("About to run test {} ", test_name);
        let mut file = File::open(path.clone()).unwrap();
        let mut file_buffer = Vec::new();
        file.read_to_end(&mut file_buffer).unwrap();

        let mut test_result_buffer = Vec::new();
        let file = File::open(path.clone()).unwrap();
        repl(&mut BufReader::new(file), &mut test_result_buffer, true);

        if file_buffer != test_result_buffer {
            failures += 1;
            path.set_extension("reject");
            let reject = path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            let mut file = File::create(path.clone()).unwrap();
            file.write_all(&test_result_buffer).unwrap();
            file.sync_all().unwrap();

            println!("Repl test {} failure. Failing output written to {} in repl-tests dir.",
                     test_name,
                     reject);
        } else {
            println!("{} successful",
                     path.file_name()
                         .unwrap()
                         .to_str()
                         .unwrap()
                         .to_string());
        }
    }
    if total == 0 {
        panic!("No tests were run!");
    }
    if failures > 0 {
        panic!("Failed {} tests in repl-test out of {}", failures, total);
    }
}
