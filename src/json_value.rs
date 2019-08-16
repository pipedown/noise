
use std::str;
use std::cmp::Ordering;
use std::io::Write;

use error::Error;



#[derive(PartialEq, PartialOrd, Clone, Debug)]
pub enum JsonValue {
    Number(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
    True,
    False,
    Null,
}

impl JsonValue {
    pub fn str_to_literal(string: &str) -> String {
        let mut ret = String::with_capacity(string.len() * 2 + 2);
        ret.push('"');
        for c in string.chars() {
            if c == '"' || c == '\\' {
                ret.push('\\');
            }
            ret.push(c);
        }
        ret.push('"');
        ret
    }

    fn cmp_always_equal(_a: &JsonValue, _b: &JsonValue) -> Ordering {
        Ordering::Equal
    }

    fn cmp_f64(a: &JsonValue, b: &JsonValue) -> Ordering {
        if let &JsonValue::Number(a_val) = a {
            if let &JsonValue::Number(b_val) = b {
                if a_val < b_val {
                    Ordering::Less
                } else if a_val > b_val {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            } else {
                panic!("cast error in cmp_f64");
            }
        } else {
            panic!("cast error in cmp_f64");
        }
    }

    fn cmp_string(a: &JsonValue, b: &JsonValue) -> Ordering {
        if let &JsonValue::String(ref a_val) = a {
            if let &JsonValue::String(ref b_val) = b {
                // Note we eventually want to switch to a collation library like ICU
                a_val.cmp(&b_val)
            } else {
                panic!("cast error in cmp_string");
            }
        } else {
            panic!("cast error in cmp_string");
        }
    }

    fn cmp_array(a: &JsonValue, b: &JsonValue) -> Ordering {
        if let &JsonValue::Array(ref a_val) = a {
            if let &JsonValue::Array(ref b_val) = b {
                for (a_el, b_el) in a_val.iter().zip(b_val.iter()) {
                    let order = a_el.cmp(&b_el);
                    if order != Ordering::Equal {
                        return order;
                    }
                }
                // if we got here all elements were equal. But one array might be longer
                // so sort it last
                a_val.len().cmp(&b_val.len())
            } else {
                panic!("cast error in cmp_array");
            }
        } else {
            panic!("cast error in cmp_array");
        }
    }

    fn cmp_object(a: &JsonValue, b: &JsonValue) -> Ordering {
        if let &JsonValue::Object(ref a_val) = a {
            if let &JsonValue::Object(ref b_val) = b {
                for (a_el, b_el) in a_val.iter().zip(b_val.iter()) {
                    // compare key
                    let mut order = a_el.0.cmp(&b_el.0);
                    if order != Ordering::Equal {
                        return order;
                    }
                    // compare value
                    order = a_el.1.cmp(&b_el.1);
                    if order != Ordering::Equal {
                        return order;
                    }
                }
                // if we got here all elements were equal. But one object might be longer
                // so sort it last
                a_val.len().cmp(&b_val.len())
            } else {
                panic!("cast error in cmp_object");
            }
        } else {
            panic!("cast error in cmp_object");
        }
    }

    fn type_sort_order(&self) -> (usize, fn(&JsonValue, &JsonValue) -> Ordering) {
        match self {
            &JsonValue::Null => (0, JsonValue::cmp_always_equal),
            &JsonValue::False => (1, JsonValue::cmp_always_equal),
            &JsonValue::True => (2, JsonValue::cmp_always_equal),
            &JsonValue::Number(_) => (3, JsonValue::cmp_f64),
            &JsonValue::String(_) => (4, JsonValue::cmp_string),
            &JsonValue::Array(_) => (5, JsonValue::cmp_array),
            &JsonValue::Object(_) => (6, JsonValue::cmp_object),
        }
    }

    pub fn render(&self, write: &mut dyn Write, pretty: &mut PrettyPrint) -> Result<(), Error> {
        match self {
            &JsonValue::Number(ref num) => {
                write.write_all(pretty.prefix())?;
                write.write_all(num.to_string().as_bytes())?;
            }
            &JsonValue::String(ref string) => {
                write.write_all(pretty.prefix())?;
                write.write_all(JsonValue::str_to_literal(&string).as_bytes())?
            }
            &JsonValue::Array(ref array) => {
                if array.is_empty() {
                    write.write_all(pretty.prefix())?;
                    write.write_all("[]".as_bytes())?;
                    return Ok(());
                }
                write.write_all(pretty.prefix())?;
                write.write_all("[".as_bytes())?;
                write.write_all(pretty.newline())?;
                pretty.push();

                let mut iter = array.iter().peekable();
                loop {
                    match iter.next() {
                        Some(json) => json.render(write, pretty)?,
                        None => break,
                    }
                    if iter.peek().is_some() {
                        write.write_all(",".as_bytes())?;
                    }
                    write.write_all(pretty.newline())?;
                }
                pretty.pop();
                write.write_all(pretty.prefix())?;
                write.write_all("]".as_bytes())?;
            }
            &JsonValue::Object(ref object) => {
                if object.is_empty() {
                    write.write_all(pretty.prefix())?;
                    write.write_all("{}".as_bytes())?;
                    return Ok(());
                }
                write.write_all(pretty.prefix())?;
                write.write_all("{".as_bytes())?;
                write.write_all(pretty.newline())?;
                pretty.push();

                let mut iter = object.iter().peekable();
                loop {
                    match iter.next() {
                        Some(&(ref key, ref json)) => {
                            write.write_all(pretty.prefix())?;
                            write.write_all(JsonValue::str_to_literal(&key).as_bytes())?;
                            write.write_all(":".as_bytes())?;
                            pretty.next_prefix_is_space();
                            json.render(write, pretty)?;
                        }
                        None => break,
                    }
                    if iter.peek().is_some() {
                        write.write_all(",".as_bytes())?;
                    }
                    write.write_all(pretty.newline())?;
                }
                pretty.pop();
                write.write_all(pretty.prefix())?;
                write.write_all("}".as_bytes())?;
            }
            &JsonValue::True => {
                write.write_all(pretty.prefix())?;
                write.write_all("true".as_bytes())?
            }
            &JsonValue::False => {
                write.write_all(pretty.prefix())?;
                write.write_all("false".as_bytes())?
            }
            &JsonValue::Null => {
                write.write_all(pretty.prefix())?;
                write.write_all("null".as_bytes())?
            }
        }
        Ok(())
    }
}

impl Eq for JsonValue {}

impl Ord for JsonValue {
    fn cmp(&self, other: &JsonValue) -> Ordering {
        let (self_order_num, self_cmp_fun) = self.type_sort_order();
        let (other_order_num, _other_cmp_fun) = other.type_sort_order();
        match self_order_num.cmp(&other_order_num) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self_cmp_fun(self, other),
        }
    }
}

pub struct PrettyPrint {
    indention: String,
    newline: String,
    spacing: String,
    buffer: String,
    next_prefix_is_space: bool,
}

impl PrettyPrint {
    pub fn new(indention: &str, newline: &str, spacing: &str) -> PrettyPrint {
        PrettyPrint {
            indention: indention.to_string(),
            newline: newline.to_string(),
            spacing: spacing.to_string(),
            buffer: String::new(),
            next_prefix_is_space: false,
        }
    }

    pub fn push(&mut self) {
        self.buffer += &self.indention;
    }

    pub fn pop(&mut self) {
        let len = self.buffer.len() - self.indention.len();
        self.buffer.truncate(len);
    }

    pub fn next_prefix_is_space(&mut self) {
        self.next_prefix_is_space = true;
    }

    pub fn prefix(&mut self) -> &[u8] {
        if self.next_prefix_is_space {
            self.next_prefix_is_space = false;
            self.spacing.as_bytes()
        } else {
            self.buffer.as_bytes()
        }
    }

    pub fn newline(&mut self) -> &[u8] {
        self.newline.as_bytes()
    }
}
