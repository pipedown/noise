extern crate noise_search;

use noise_search::repl::repl;

use std::env;
use std::io::{self, BufReader};

fn main() {
    let mut test_mode = false;
    for argument in env::args() {
       if argument == "-t" {
           test_mode = true;
       }
    }
    repl(&mut BufReader::new(io::stdin()), &mut io::stdout(), test_mode);
}
