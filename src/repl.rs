use index::{Index, OpenOptions, Batch};
use json_value::{JsonValue, PrettyPrint};

use std::io::{Write, BufRead};
use std::mem;



fn is_command(str: &str) -> bool {
    let commands = ["find", "add", "create", "drop", "open", "pretty", "commit", "del",
                    "dumpkeys", "params"];
    for command in commands.iter() {
        if str.starts_with(command) {
            return true;
        }
    }
    false
}


fn next_command(r: &mut BufRead, w: &mut Write, test_mode: bool) -> Option<String> {
    let mut lines = String::new();
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
            return None;
        }

        if test_mode {
            // echo the command
            w.write_all(lines.as_bytes()).unwrap();
        }

        lines = lines.trim_right().to_string();
        if lines.ends_with(";") {
            // strip the semi-colon off
            lines.pop();
            return Some(lines);
        } else {
            panic!("Unterminated command, no semi-colon (;) {}\n", lines);
        }
    }
}

pub fn repl(r: &mut BufRead, w: &mut Write, test_mode: bool) {
    let mut pretty = PrettyPrint::new("", "", "");
    loop {
        if let Some(cmd) = next_command(r, w, test_mode) {
            if cmd.starts_with("open") {
                let dbname = cmd[4..].trim_left();
                match Index::open(dbname, None) {
                    Ok(index) => {
                        repl_opened(index, r, w, test_mode, pretty);
                        return;
                    }
                    Err(reason) => write!(w, "{}\n", reason).unwrap(),
                }
            } else if cmd.starts_with("create") {
                let dbname = cmd[6..].trim_left();
                match Index::open(dbname, Some(OpenOptions::Create)) {
                    Ok(index) => {
                        repl_opened(index, r, w, test_mode, pretty);
                        return;
                    }
                    Err(reason) => write!(w, "{}\n", reason).unwrap(),
                }
            } else if cmd.starts_with("pretty") {
                if cmd[6..].trim_left().starts_with("on") {
                    pretty = PrettyPrint::new("  ", "\n", " ");
                } else {
                    pretty = PrettyPrint::new("", "", "");
                }
            } else if cmd.starts_with("drop") {
                let dbname = cmd[4..].trim_left();
                match Index::drop(dbname) {
                    Ok(()) => (),
                    Err(reason) => write!(w, "{}\n", reason).unwrap(),
                }
            } else {
                write!(w, "Index isn't open\n").unwrap();
            }
        } else {
            break;
        }
    }
}

fn flush_batch(index: &mut Index, batch: &mut Batch, w: &mut Write) {
    let mut batch2 = Batch::new();
    mem::swap(batch, &mut batch2);
    if let Err(reason) = index.flush(batch2) {
        write!(w, "{}\n", reason).unwrap();
    }
}

fn repl_opened(mut index: Index,
               r: &mut BufRead,
               w: &mut Write,
               test_mode: bool,
               mut pretty: PrettyPrint) {
    let mut batch = Batch::new();
    let mut params = None;
    loop {
        let cmd = if let Some(cmd) = next_command(r, w, test_mode) {
            cmd
        } else {
            flush_batch(&mut index, &mut batch, w);
            return;
        };
        if cmd.starts_with("params") {
            params = Some(cmd[6..].trim_left().to_string());
        } else if cmd.starts_with("pretty") {
            if cmd[6..].trim_left().starts_with("on") {
                pretty = PrettyPrint::new("  ", "\n", " ");
            } else {
                pretty = PrettyPrint::new("", "", "");
            }
        } else if cmd.starts_with("create") {
            flush_batch(&mut index, &mut batch, w);
            let dbname = cmd[6..].trim_left();
            match Index::open(dbname, Some(OpenOptions::Create)) {
                Ok(index_new) => index = index_new,
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if cmd.starts_with("drop") {
            let dbname = cmd[4..].trim_left();
            match Index::drop(dbname) {
                Ok(()) => (),
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if cmd.starts_with("open") {
            let dbname = cmd[4..].trim_left();
            match Index::open(dbname, None) {
                Ok(index_new) => index = index_new,
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if cmd.starts_with("dumpkeys") {
            flush_batch(&mut index, &mut batch, w);
            match index.all_keys() {
                Ok(keys) => {
                    for key in keys {
                        write!(w, "{}\n", key).unwrap();
                    }
                }
                Err(reason) => {
                    write!(w, "{}\n", reason).unwrap();
                }
            }
        } else if cmd.starts_with("add") {
            match index.add(&cmd[3..], &mut batch) {
                Ok(id) => write!(w, "{}\n", JsonValue::str_to_literal(&id)).unwrap(),
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if cmd.starts_with("del") {
            match index.delete(&cmd[3..].trim_left(), &mut batch) {
                Ok(true) => write!(w, "ok\n").unwrap(),
                Ok(false) => write!(w, "not found\n").unwrap(),
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        } else if cmd.starts_with("commit") {
            flush_batch(&mut index, &mut batch, w);
        } else if cmd.starts_with("find") {
            flush_batch(&mut index, &mut batch, w);
            match index.query(&cmd, params.take()) {
                Ok(results) => {
                    let mut results = results.peekable();

                    w.write_all(b"[").unwrap();
                    if results.peek().is_some() {
                        w.write_all(b"\n").unwrap();
                    }
                    pretty.push();
                    while let Some(json) = results.next() {
                        json.render(w, &mut pretty).unwrap();
                        if results.peek().is_some() {
                            w.write_all(b",").unwrap();
                        }
                        w.write_all(b"\n").unwrap();
                    }
                    w.write_all(b"]\n").unwrap();
                }
                Err(reason) => write!(w, "{}\n", reason).unwrap(),
            }
        }
    }
}