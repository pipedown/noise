use index::{Index, OpenOptions};
use query::Query;
use json_value::{JsonValue, PrettyPrint};

use std::io::{Write, BufRead};


fn is_command(str: &str) -> bool {
    let commands = ["find", "add", "create", "drop", "open",
                    "pretty", "commit", "del", "load", "dumpkeys"];
    for command in commands.iter() {
        if str.starts_with(command) {
            return true;
        }
    }
    false
}

pub fn repl(r: &mut BufRead, w: &mut Write, test_mode: bool) {
    let mut index = Index::new();
    let mut lines = String::new();
    let mut pretty = PrettyPrint::new("", "", "");
    loop {
        // read in command until we get to a end semi-colon
        if r.read_line(&mut lines).unwrap() > 0 {
            if test_mode && lines == "\n" || lines.starts_with("#") {
                // we preserve blank lines and comments in test mode
                w.write_all(lines.as_bytes()).unwrap();
                lines.clear();
                continue;
            }
            if test_mode && !is_command(&lines) {
                // we drop non-command lines
                lines.clear();
                continue;
            } else if !is_command(&lines) {
                w.write_all(b"Unrecognized command!\n").unwrap();
                lines.clear();
                continue;
            }
            // check for end semi-colon
            if !lines.trim_right().ends_with(";") {
                while r.read_line(&mut lines).unwrap() > 0 {
                    // loop until we get the end semi-colon
                    if lines.trim_right().ends_with(";") {
                        break;
                    }
                }
            }
        } else {
            // commit anything written
            if index.is_open() {
                if let Err(reason) = index.flush() {
                    write!(w, "{}\n", reason).unwrap();
                }
            }
            return;
        }
        if test_mode {
            // echo the command
            w.write_all(lines.as_bytes()).unwrap();
        }
        lines = lines.trim_right().to_string();
        if lines.ends_with(";") {
            // strip the semi-colon off
            lines.pop();
        } else {
            write!(w, "Unterminated command, no semi-colon (;) {}\n", lines).unwrap();
        }

        if lines.starts_with("pretty") {
            if lines[6..].trim_left().starts_with("on") {
                pretty = PrettyPrint::new("  ", "\n", " ");
            } else {
                pretty = PrettyPrint::new("", "", "");
            }
        } else if lines.starts_with("create") {
            let dbname = lines[6..].trim_left();
            match index.open(dbname, Some(OpenOptions::Create)) {
                Ok(()) => (),
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if lines.starts_with("drop") {
            let dbname = lines[4..].trim_left();
            match Index::drop(dbname) {
                Ok(()) => (),
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if lines.starts_with("open") {
            let dbname = lines[4..].trim_left();
            match index.open(dbname, None) {
                Ok(()) => (),
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if lines.starts_with("dumpkeys") {
            match index.all_keys() {
                Ok(keys) => {
                    for key in keys {
                        write!(w, "{}\n", key).unwrap();
                    }
                },
                Err(reason) => {
                    write!(w, "{}\n", reason).unwrap();
                },
            }
        } else if lines.starts_with("add") {
            match index.add(&lines[3..]) {
                Ok(id) => write!(w, "{}\n", JsonValue::str_to_literal(&id)).unwrap(),
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if lines.starts_with("del") {
            match index.delete(&lines[3..].trim_left()) {
                Ok(true) => write!(w, "ok\n").unwrap(),
                Ok(false) => write!(w, "not found\n").unwrap(),
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if lines.starts_with("commit") {
            if let Err(reason) = index.flush() {
                write!(w, "{}\n", reason).unwrap();
            }
        } else if lines.starts_with("find") {
            if let Err(reason) = index.flush() {
                write!(w, "{}\n", reason).unwrap();
            } else {
                match Query::get_matches(&lines, &index) {
                    Ok(results) => {
                        let mut results = results.peekable();

                        w.write_all(b"[").unwrap();
                        if results.peek().is_some() {
                            w.write_all(b"\n").unwrap();
                        }
                        pretty.push();
                        while let Some(result) = results.next() {
                            match result {
                                Ok(json) => {
                                    json.render(w, &mut pretty).unwrap();
                                    if results.peek().is_some() {
                                        w.write_all(b",").unwrap();
                                    }
                                    w.write_all(b"\n").unwrap();
                                },
                                Err(reason) => {
                                    write!(w, "{}\n", reason).unwrap();
                                },
                            }
                        }
                        w.write_all(b"]\n").unwrap();
                    },
                    Err(reason) => write!(w, "{}\n", reason).unwrap(),
                }
            }
        }
        lines.clear();
    }
}


