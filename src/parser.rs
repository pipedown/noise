
use std;
use std::str;
use std::collections::HashMap;
use std::iter::Iterator;
use std::rc::Rc;
use std::usize;

use error::Error;
use key_builder::KeyBuilder;
use stems::Stems;
use json_value::JsonValue;
use query::{Order, OrderInfo, OrderField};
use aggregates::AggregateFun;
use returnable::{Returnable, RetValue, RetObject, RetArray, RetLiteral, RetBind, RetScore,
                 ReturnPath};
use filters::{QueryRuntimeFilter, ExactMatchFilter, StemmedWordFilter, StemmedWordPosFilter,
              StemmedPhraseFilter, DistanceFilter, AndFilter, OrFilter, BindFilter, BoostFilter,
              NotFilter, RangeFilter, RangeOperator, AllDocsFilter, BboxFilter};
use snapshot::Snapshot;


struct Aggregate {
    fun: AggregateFun,
    bind_name: Option<String>,
    keypath: ReturnPath,
    // The concat aggregate has an additional "sep" parameter
    sep: Option<JsonValue>,
    // The default default value is always `null` hence we don't need an option type here
    default: JsonValue,
}

pub struct Parser<'a, 'c> {
    query: &'c str,
    offset: usize,
    params: HashMap<String, JsonValue>,
    kb: KeyBuilder,
    pub snapshot: Rc<Snapshot<'a>>,
    pub needs_scoring: bool,
}

impl<'a, 'c> Parser<'a, 'c> {
    pub fn new(query: &'c str,
               params: HashMap<String, JsonValue>,
               snapshot: Rc<Snapshot<'a>>)
               -> Parser<'a, 'c> {
        Parser {
            query: query,
            offset: 0,
            params: params,
            kb: KeyBuilder::new(),
            snapshot: snapshot,
            needs_scoring: false,
        }
    }

    fn ws(&mut self) {
        for char in self.query[self.offset..].chars() {
            if !char.is_whitespace() {
                break;
            }
            self.offset += char.len_utf8();
        }
    }

    fn consume(&mut self, token: &str) -> bool {
        if self.could_consume(token) {
            self.offset += token.len();
            self.ws();
            true
        } else {
            false
        }
    }

    fn consume_no_ws(&mut self, token: &str) -> bool {
        if self.could_consume(token) {
            self.offset += token.len();
            true
        } else {
            false
        }
    }


    fn must_consume(&mut self, token: &str) -> Result<(), Error> {
        if self.could_consume(token) {
            self.offset += token.len();
            self.ws();
            Ok(())
        } else {
            if self.offset == self.query.len() {
                Err(Error::Parse(format!("Expected '{}' at character {} but query string ended.",
                                         token,
                                         self.offset)))
            } else {
                Err(Error::Parse(format!("Expected '{}' at character {}, found {}.",
                                         token,
                                         self.offset,
                                         &self.query[self.offset..self.offset + 1])))
            }
        }
    }

    fn could_consume(&self, token: &str) -> bool {
        self.query[self.offset..].starts_with(token)
    }

    fn consume_key(&mut self) -> Result<Option<String>, Error> {
        if let Some(key) = self.consume_field() {
            Ok(Some(key))
        } else if let Some(key) = try!(self.consume_string_literal()) {
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }

    fn consume_field(&mut self) -> Option<String> {
        let mut result = String::new();
        {
            let mut chars = self.query[self.offset..].chars();
            if let Some(c) = chars.next() {
                // first char cannot be numeric
                if c.is_alphabetic() || '_' == c || '$' == c {
                    result.push(c);
                    for c in chars {
                        if c.is_alphanumeric() || '_' == c || '$' == c {
                            result.push(c);
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        if result.len() > 0 {
            self.offset += result.len();
            self.ws();
            Some(result)
        } else {
            None
        }
    }

    fn consume_bbox(&mut self) -> Result<[f64; 4], Error> {
        let error_message = "Bounding box needs to be `[west, south, east, north]`.";
        match try!(self.json_array()) {
            JsonValue::Array(vec) => {
                if vec.len() == 4 {
                    let mut bbox = [0f64; 4];
                    for (ii, value) in vec.iter().enumerate() {
                        match value {
                            &JsonValue::Number(num) => bbox[ii] = num,
                            _ => return Err(Error::Parse(error_message.to_string()))
                        }
                    }
                    Ok(bbox)
                } else {
                    Err(Error::Parse(error_message.to_string()))
                }
            }
            _ => Err(Error::Parse(error_message.to_string()))
        }
    }

    fn consume_param(&mut self) -> Result<Option<JsonValue>, Error> {
        if self.consume_no_ws("@") {
            if let Some(field) = self.consume_field() {
                if let Some(param) = self.params.get(&field) {
                    Ok(Some(param.clone()))
                } else {
                    Err(Error::Parse(format!("No matching parameter for @{}.", field)))
                }
            } else {
                Err(Error::Parse("No parameter name after @.".to_string()))
            }
        } else {
            Ok(None)
        }
    }

    fn consume_param_string(&mut self) -> Result<Option<String>, Error> {
        if self.consume_no_ws("@") {
            if let Some(field) = self.consume_field() {
                if let Some(param) = self.params.get(&field) {
                    if let &JsonValue::String(ref string) = param {
                        Ok(Some(string.to_string()))
                    } else {
                        Err(Error::Parse(format!("Parameter @{} must be a string.", field)))
                    }
                } else {
                    Err(Error::Parse(format!("No matching parameter for @{}.", field)))
                }
            } else {
                Err(Error::Parse("No parameter name after @.".to_string()))
            }
        } else {
            Ok(None)
        }
    }

    fn consume_integer(&mut self) -> Result<Option<i64>, Error> {
        let mut result = String::new();
        for char in self.query[self.offset..].chars() {
            if char >= '0' && char <= '9' {
                result.push(char);
            } else {
                break;
            }
        }
        if !result.is_empty() {
            self.offset += result.len();
            self.ws();
            Ok(Some(try!(result.parse())))
        } else {
            Ok(None)
        }
    }

    fn consume_default(&mut self) -> Result<Option<JsonValue>, Error> {
        if self.consume("default") {
            try!(self.must_consume("="));
            if let Some(json) = try!(self.json()) {
                Ok(Some(json))
            } else {
                Err(Error::Parse("Expected json value for default".to_string()))
            }
        } else {
            Ok(None)
        }
    }

    fn consume_aggregate(&mut self) -> Result<Option<Aggregate>, Error> {
        let offset = self.offset;
        let mut aggregate_fun = if self.consume("group") {
            AggregateFun::GroupAsc
        } else if self.consume("sum") {
            AggregateFun::Sum
        } else if self.consume("max_array") {
            AggregateFun::MaxArray
        } else if self.consume("max") {
            AggregateFun::Max
        } else if self.consume("min_array") {
            AggregateFun::MinArray
        } else if self.consume("min") {
            AggregateFun::Min
        } else if self.consume("array_flat") {
            AggregateFun::ArrayFlat
        } else if self.consume("array") {
            AggregateFun::Array
        } else if self.consume("concat") {
            AggregateFun::Concat
        } else if self.consume("avg") {
            AggregateFun::Avg
        } else if self.consume("count") {
            AggregateFun::Count
        } else {
            return Ok(None);
        };

        if self.consume("(") {
            if aggregate_fun == AggregateFun::Count {
                try!(self.must_consume(")"));
                Ok(Some(Aggregate {
                    fun: aggregate_fun,
                    bind_name: None,
                    keypath: ReturnPath::new(),
                    sep: None,
                    default: JsonValue::Null,
                }))
            } else if aggregate_fun == AggregateFun::Concat {
                let bind_name_option = self.consume_field();

                if let Some(rp) = try!(self.consume_keypath()) {
                    let default = self.consume_default()?.unwrap_or(JsonValue::Null);
                    let sep = if self.consume("sep") {
                        try!(self.must_consume("="));
                        JsonValue::String(try!(self.must_consume_string_literal()))
                    } else {
                        JsonValue::String(",".to_string())
                    };
                    try!(self.must_consume(")"));
                    Ok(Some(Aggregate {
                        fun: aggregate_fun,
                        bind_name: bind_name_option,
                        keypath: rp,
                        sep: Some(sep),
                        default: default,
                    }))
                } else {
                    Err(Error::Parse("Expected keypath or bind variable".to_string()))
                }
            } else {
                let bind_name_option = self.consume_field();

                if let Some(rp) = try!(self.consume_keypath()) {
                    let default = self.consume_default()?.unwrap_or(JsonValue::Null);
                    if self.consume("order") {
                        try!(self.must_consume("="));
                        if self.consume("asc") {
                            aggregate_fun = AggregateFun::GroupAsc;
                        } else if self.consume("desc") {
                            aggregate_fun = AggregateFun::GroupDesc;
                        } else {
                            return Err(Error::Parse("Expected asc or desc".to_string()));
                        }
                    }
                    try!(self.must_consume(")"));

                    Ok(Some(Aggregate {
                        fun: aggregate_fun,
                        bind_name: bind_name_option,
                        keypath: rp,
                        sep: None,
                        default: default,
                    }))
                } else {
                    Err(Error::Parse("Expected keypath or bind variable".to_string()))
                }
            }
        } else {
            // this consumed word above might be a Bind var. Unconsume and return nothing.
            self.offset = offset;
            Ok(None)
        }
    }

    fn consume_keypath(&mut self) -> Result<Option<ReturnPath>, Error> {
        let key: String = if self.consume_no_ws(".") {
            if self.consume("[") {
                let key = try!(self.must_consume_string_literal());
                try!(self.must_consume("]"));
                key
            } else {
                if let Some(key) = self.consume_field() {
                    key
                } else {
                    self.ws();
                    // this means return the whole document
                    return Ok(Some(ReturnPath::new()));
                }
            }
        } else {
            return Ok(None);
        };

        let mut ret_path = ReturnPath::new();
        ret_path.push_object_key(key);
        loop {
            if self.consume("[") {
                if let Some(index) = try!(self.consume_integer()) {
                    ret_path.push_array(index as u64);
                    try!(self.must_consume("]"));
                } else {
                    if self.consume("]") {
                        ret_path.push_array_all();
                    } else {
                        return Err(Error::Parse("Expected array index integer or *.".to_string()));
                    }
                }
            } else if self.consume(".") {
                if let Some(key) = self.consume_field() {
                    ret_path.push_object_key(key);
                } else {
                    return Err(Error::Parse("Expected object key.".to_string()));
                }
            } else {
                break;
            }
        }
        self.ws();
        Ok(Some(ret_path))
    }

    // if no boost is specified returns 1.0
    fn consume_boost(&mut self) -> Result<f32, Error> {
        if self.consume("^") {
            if let Some(num) = try!(self.consume_number()) {
                Ok(num as f32)
            } else {
                return Err(Error::Parse("Expected number after ^ symbol.".to_string()));
            }
        } else {
            Ok(1.0)
        }
    }

    fn consume_boost_and_wrap_filter(&mut self,
                                     filter: Box<dyn QueryRuntimeFilter + 'a>)
                                     -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        let boost = try!(self.consume_boost());
        if boost != 1.0 {
            Ok(Box::new(BoostFilter::new(filter, boost)))
        } else {
            Ok(filter)
        }
    }

    fn consume_number(&mut self) -> Result<Option<f64>, Error> {
        // Yes this parsing code is hideously verbose. But it conforms exactly to the json spec
        // and uses the rust f64 parser, which can't tell us how many characters it used or needs.

        // At the end it then uses the std rust String::parse<f64>() method to parse and return
        // the f64 value and advance the self.offset. The rust method is a super set of the
        // allowable json syntax, so it will parse any valid json floating point number. It might
        // return an error if the number is out of bounds.
        let mut result = String::new();
        'outer: loop {
            // this loop isn't a loop, it's just there to scope the self borrow
            // and then jump to the end to do another borrow (self.ws())
            let mut chars = self.query[self.offset..].chars();
            let mut c = if let Some(c) = chars.next() {
                c
            } else {
                return Ok(None);
            };

            // parse the sign
            c = if c == '-' {
                result.push('-');
                if let Some(c) = chars.next() {
                    c
                } else {
                    return Err(Error::Parse("Expected digits after sign (-).".to_string()));
                }
            } else {
                c
            };

            // parse the first digit
            let mut leading_zero = false;
            c = if c == '0' {
                result.push('0');
                leading_zero = true;
                if let Some(c) = chars.next() {
                    c
                } else {
                    break 'outer;
                }
            } else if c >= '1' && c <= '9' {
                result.push(c);
                if let Some(c) = chars.next() {
                    c
                } else {
                    break 'outer;
                }
            } else if result.is_empty() {
                // no sign or digits found. not a number
                return Ok(None);
            } else {
                return Err(Error::Parse("Expected digits after sign (-).".to_string()));
            };

            // parse remaning significant digits
            if !leading_zero {
                // no more digits allowed if first digit is zero
                loop {
                    c = if c >= '0' && c <= '9' {
                        result.push(c);
                        if let Some(c) = chars.next() {
                            c
                        } else {
                            break 'outer;
                        }
                    } else {
                        break;
                    };
                }
            }

            // parse decimal
            c = if c == '.' {
                result.push(c);
                if let Some(c) = chars.next() {
                    c
                } else {
                    return Err(Error::Parse("Expected digits after decimal point.".to_string()));
                }
            } else {
                break 'outer;
            };

            // parse mantissa
            let mut found_mantissa = false;
            loop {
                c = if c >= '0' && c <= '9' {
                    result.push(c);
                    found_mantissa = true;

                    if let Some(c) = chars.next() {
                        c
                    } else {
                        break 'outer;
                    }
                } else {
                    if found_mantissa {
                        break;
                    }
                    return Err(Error::Parse("Expected digits after decimal point.".to_string()));
                };
            }

            // parse exponent symbol
            c = if c == 'e' || c == 'E' {
                result.push(c);
                if let Some(c) = chars.next() {
                    c
                } else {
                    return Err(Error::Parse("Expected exponent after e.".to_string()));
                }
            } else {
                break 'outer;
            };

            // parse exponent sign
            c = if c == '+' || c == '-' {
                result.push(c);
                if let Some(c) = chars.next() {
                    c
                } else {
                    return Err(Error::Parse("Expected exponent after e.".to_string()));
                }
            } else {
                c
            };

            // parse exponent digits
            let mut found_exponent = false;
            loop {
                c = if c >= '0' && c <= '9' {
                    result.push(c);
                    found_exponent = true;
                    if let Some(c) = chars.next() {
                        c
                    } else {
                        break 'outer;
                    }
                } else {
                    if found_exponent {
                        break 'outer;
                    }
                    return Err(Error::Parse("Expected exponent after e.".to_string()));
                }
            }
        }

        self.offset += result.len();
        self.ws();
        Ok(Some(try!(result.parse())))
    }


    fn must_consume_string_literal(&mut self) -> Result<String, Error> {
        if let Some(string) = try!(self.consume_string_literal()) {
            Ok(string)
        } else {
            Err(Error::Parse("Expected string literal.".to_string()))
        }
    }

    fn consume_string_literal(&mut self) -> Result<Option<String>, Error> {
        let mut lit = String::new();
        if !self.could_consume("\"") {
            return Ok(None);
        }
        // can't consume("\"") the leading quote because it will also skip leading whitespace
        // inside the string literal
        self.offset += 1;
        {
            let mut chars = self.query[self.offset..].chars();
            'outer: loop {
                let char = if let Some(char) = chars.next() {
                    char
                } else {
                    break;
                };
                if char == '\\' {
                    self.offset += 1;

                    let char = if let Some(char) = chars.next() {
                        char
                    } else {
                        break;
                    };
                    match char {
                        '\\' | '"' | '/' => lit.push(char),
                        'n' => lit.push('\n'),
                        'b' => lit.push('\x08'),
                        'r' => lit.push('\r'),
                        'f' => lit.push('\x0C'),
                        't' => lit.push('\t'),
                        'v' => lit.push('\x0B'),
                        'u' => {
                            let mut n = 0;
                            for _i in 0..4 {
                                let char = if let Some(char) = chars.next() {
                                    char
                                } else {
                                    break 'outer;
                                };
                                n = match char {
                                    c @ '0'..='9' => n * 16 + ((c as u16) - ('0' as u16)),
                                    c @ 'a'..='f' => n * 16 + (10 + (c as u16) - ('a' as u16)),
                                    c @ 'A'..='F' => n * 16 + (10 + (c as u16) - ('A' as u16)),
                                    _ => {
                                        let msg = format!("Invalid hexidecimal escape: {}", char);
                                        return Err(Error::Parse(msg));
                                    }
                                };

                            }
                            self.offset += 3; // 3 because 1 is always added after the match below
                        }
                        _ => {
                            return Err(Error::Parse(format!("Unknown character escape: {}", char)))
                        }
                    };
                    self.offset += 1;
                } else {
                    if char == '"' {
                        break;
                    } else {
                        lit.push(char);
                        self.offset += char.len_utf8();
                    }
                }
            }
        }
        try!(self.must_consume("\""));
        Ok(Some(lit))
    }

    fn consume_range_operator(&mut self) -> Result<RangeOperator, Error> {
        let inclusive = self.consume("=");
        let json = try!(self.must_consume_json_primitive());
        match json {
            JsonValue::Number(num) => {
                if inclusive {
                    Ok(RangeOperator::Inclusive(num))
                } else {
                    Ok(RangeOperator::Exclusive(num))
                }
            }
            _ => {
                Err(Error::Parse("Range operator on non-number JSON types is not yet implemented!"
                                     .to_string()))
            }
        }
    }

    fn find(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        if !self.consume("find") {
            return Err(Error::Parse("Missing 'find' keyword".to_string()));
        }
        self.not_object()
    }

    fn not_object(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        if self.consume("!") {
            let filter = try!(self.object());
            Ok(Box::new(NotFilter::new(&self.snapshot, filter, self.kb.clone())))
        } else {
            self.object()
        }
    }

    fn object(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        if self.consume("{") {
            if self.consume("}") {
                return Ok(Box::new(AllDocsFilter::new(&self.snapshot)));
            }
            let mut left = try!(self.obool());
            try!(self.must_consume("}"));

            left = try!(self.consume_boost_and_wrap_filter(left));

            if self.consume("&&") {
                let right = try!(self.not_object());
                Ok(Box::new(AndFilter::new(vec![left, right], self.kb.arraypath_len())))

            } else if self.consume("||") {
                let right = try!(self.not_object());
                Ok(Box::new(OrFilter::new(left, right, self.kb.arraypath_len())))
            } else {
                Ok(left)
            }
        } else {
            self.parens()
        }
    }

    fn parens(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        if self.consume("!") {
            let filter = try!(self.parens());
            return Ok(Box::new(NotFilter::new(&self.snapshot, filter, self.kb.clone())));
        }
        try!(self.must_consume("("));
        let filter = try!(self.object());
        try!(self.must_consume(")"));

        self.consume_boost_and_wrap_filter(filter)
    }

    fn obool(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        let mut filter = try!(self.ocompare());
        loop {
            filter = if self.consume("&&") || self.consume(",") {
                let right = try!(self.obool());
                Box::new(AndFilter::new(vec![filter, right], self.kb.arraypath_len()))
            } else if self.consume("||") {
                let right = try!(self.obool());
                Box::new(OrFilter::new(filter, right, self.kb.arraypath_len()))
            } else {
                break;
            }
        }
        Ok(filter)
    }

    fn ocompare(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        if let Some(filter) = try!(self.oparens()) {
            Ok(filter)
        } else if let Some(field) = try!(self.consume_key()) {
            self.kb.push_object_key(&field);
            try!(self.must_consume(":"));
            if let Some(filter) = try!(self.oparens()) {
                self.kb.pop_object_key();
                Ok(filter)
            } else {
                let filter = try!(self.compare());
                self.kb.pop_object_key();
                Ok(filter)
            }
        } else {
            Err(Error::Parse("Expected object key or '('".to_string()))
        }
    }

    fn oparens(&mut self) -> Result<Option<Box<dyn QueryRuntimeFilter + 'a>>, Error> {
        let offset = self.offset;
        if self.consume("!") {
            if let Some(f) = try!(self.oparens()) {
                return Ok(Some(Box::new(NotFilter::new(&self.snapshot, f, self.kb.clone()))));
            } else {
                self.offset = offset;
                return Ok(None);
            }
        }
        let opt_filter = if self.consume("(") {
            let f = try!(self.obool());
            try!(self.must_consume(")"));
            Some(f)
        } else if self.could_consume("[") {
            Some(try!(self.array()))
        } else if self.could_consume("{") {
            Some(try!(self.object()))
        } else {
            if let Some(filter) = try!(self.bind_var()) {
                Some(filter)
            } else {
                None
            }
        };

        if let Some(filter) = opt_filter {
            Ok(Some(try!(self.consume_boost_and_wrap_filter(filter))))
        } else {
            Ok(None)
        }
    }

    fn compare(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        if let Some(filter) = try!(self.equal()) {
            Ok(filter)
        } else if let Some(filter) = try!(self.stemmed()) {
            Ok(filter)
        } else if let Some(filter) = try!(self.bbox()) {
            Ok(filter)
        } else {
            if self.consume(">") {
                let min = try!(self.consume_range_operator());
                let filter = RangeFilter::new(&self.snapshot, self.kb.clone(), Some(min), None);
                Ok(Box::new(filter))
            } else if self.consume("<") {
                let max = try!(self.consume_range_operator());
                let filter = RangeFilter::new(&self.snapshot, self.kb.clone(), None, Some(max));
                Ok(Box::new(filter))
            } else {
                Err(Error::Parse("Expected comparison operator".to_string()))
            }
        }
    }

    fn equal(&mut self) -> Result<Option<Box<dyn QueryRuntimeFilter + 'a>>, Error> {
        let not_equal = self.consume("!=");
        if not_equal || self.consume("==") {
            let json = try!(self.must_consume_json_primitive());
            let boost = try!(self.consume_boost());
            let filter: Box<dyn QueryRuntimeFilter> = match json {
                JsonValue::String(literal) => {
                    let mut filters: Vec<StemmedWordPosFilter> = Vec::new();
                    {
                        let stems = Stems::new(&literal);
                        for stem in stems {
                            filters.push(StemmedWordPosFilter::new(&self.snapshot,
                                                                   &stem.stemmed,
                                                                   &self.kb,
                                                                   boost));
                        }
                    }
                    let filter = StemmedPhraseFilter::new(filters);
                    Box::new(ExactMatchFilter::new(&self.snapshot,
                                                   filter,
                                                   self.kb.clone(),
                                                   literal,
                                                   true))
                }
                JsonValue::Number(num) => {
                    Box::new(RangeFilter::new(&self.snapshot,
                                              self.kb.clone(),
                                              Some(RangeOperator::Inclusive(num)),
                                              Some(RangeOperator::Inclusive(num))))
                }
                JsonValue::True => {
                    Box::new(RangeFilter::new(&self.snapshot,
                                              self.kb.clone(),
                                              Some(RangeOperator::True),
                                              Some(RangeOperator::True)))
                }
                JsonValue::False => {
                    Box::new(RangeFilter::new(&self.snapshot,
                                              self.kb.clone(),
                                              Some(RangeOperator::False),
                                              Some(RangeOperator::False)))
                }
                JsonValue::Null => {
                    Box::new(RangeFilter::new(&self.snapshot,
                                              self.kb.clone(),
                                              Some(RangeOperator::Null),
                                              Some(RangeOperator::Null)))
                }
                _ => panic!("Exact match on other JSON types is not yet implemented!"),
            };
            if not_equal {
                Ok(Some(Box::new(NotFilter::new(&self.snapshot, filter, self.kb.clone()))))
            } else {
                Ok(Some(filter))
            }
        } else {
            Ok(None)
        }
    }

    fn stemmed(&mut self) -> Result<Option<Box<dyn QueryRuntimeFilter + 'a>>, Error> {
        let not_stemmed = self.consume("!~=");
        if not_stemmed || self.consume("~=") {
            // regular search
            let literal = if let Some(string) = try!(self.consume_param_string()) {
                string
            } else {
                try!(self.must_consume_string_literal())
            };
            let boost = try!(self.consume_boost());
            let stems = Stems::new(&literal);
            let stemmed_words: Vec<String> = stems.map(|stem| stem.stemmed).collect();

            let filter: Box<dyn QueryRuntimeFilter> = match stemmed_words.len() {
                0 => panic!("Cannot create a StemmedWordFilter"),
                1 => {
                    Box::new(StemmedWordFilter::new(&self.snapshot,
                                                    &stemmed_words[0],
                                                    &self.kb,
                                                    boost))
                }
                _ => {
                    let mut filters: Vec<StemmedWordPosFilter> = Vec::new();
                    for stemmed_word in stemmed_words {
                        let filter = StemmedWordPosFilter::new(&self.snapshot,
                                                               &stemmed_word,
                                                               &self.kb,
                                                               boost);
                        filters.push(filter);
                    }
                    Box::new(StemmedPhraseFilter::new(filters))
                }
            };
            if not_stemmed {
                Ok(Some(Box::new(NotFilter::new(&self.snapshot, filter, self.kb.clone()))))
            } else {
                Ok(Some(filter))
            }
        } else if not_stemmed || self.consume("~") {
            let word_distance = match try!(self.consume_integer()) {
                Some(int) => int,
                None => {
                    return Err(Error::Parse("Expected integer for proximity search".to_string()));
                }
            };
            try!(self.must_consume("="));

            let literal = if let Some(string) = try!(self.consume_param_string()) {
                string
            } else {
                try!(self.must_consume_string_literal())
            };
            let boost = try!(self.consume_boost());
            let stems = Stems::new(&literal);
            let mut filters: Vec<StemmedWordPosFilter> = Vec::new();
            for stem in stems {
                let filter =
                    StemmedWordPosFilter::new(&self.snapshot, &stem.stemmed, &self.kb, boost);
                filters.push(filter);
            }
            if word_distance > std::u32::MAX as i64 {
                return Err(Error::Parse("Proximity search number too large.".to_string()));
            }
            match filters.len() {
                0 => panic!("Cannot create a DistanceFilter"),
                _ => {
                    let filter = Box::new(DistanceFilter::new(filters, word_distance as u32));
                    if not_stemmed {
                        Ok(Some(Box::new(NotFilter::new(&self.snapshot, filter, self.kb.clone()))))
                    } else {
                        Ok(Some(filter))
                    }
                }
            }
        } else {
            Ok(None)
        }
    }

    fn bbox(&mut self) -> Result<Option<Box<dyn QueryRuntimeFilter + 'a>>, Error> {
        // TODO vmx 2017-10-12: Implement boost
        if self.consume("&&") {
            let bbox = try!(self.consume_bbox());
            Ok(Some(Box::new(BboxFilter::new(Rc::clone(&self.snapshot), self.kb.clone(), bbox))))
        } else {
            Ok(None)
        }
    }

    fn abool(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        let mut filter = try!(self.acompare());
        loop {
            filter = if self.consume("&&") || self.consume(",") {
                let right = try!(self.abool());
                Box::new(AndFilter::new(vec![filter, right], self.kb.arraypath_len()))
            } else if self.consume("||") {
                let right = try!(self.abool());
                Box::new(OrFilter::new(filter, right, self.kb.arraypath_len()))
            } else {
                break;
            }
        }
        Ok(filter)
    }

    fn acompare(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        if let Some(filter) = try!(self.aparens()) {
            Ok(filter)
        } else {
            self.compare()
        }
    }

    fn aparens(&mut self) -> Result<Option<Box<dyn QueryRuntimeFilter + 'a>>, Error> {
        let offset = self.offset;
        if self.consume("!") {
            if let Some(f) = try!(self.aparens()) {
                return Ok(Some(Box::new(NotFilter::new(&self.snapshot, f, self.kb.clone()))));
            } else {
                self.offset = offset;
                return Ok(None);
            }
        }
        let opt_filter = if self.consume("(") {
            let f = try!(self.abool());
            try!(self.must_consume(")"));
            Some(f)
        } else if self.could_consume("[") {
            Some(try!(self.array()))
        } else if self.could_consume("{") {
            Some(try!(self.object()))
        } else {
            if let Some(filter) = try!(self.bind_var()) {
                Some(filter)
            } else {
                None
            }
        };

        if let Some(filter) = opt_filter {
            Ok(Some(try!(self.consume_boost_and_wrap_filter(filter))))
        } else {
            Ok(None)
        }
    }

    fn bind_var(&mut self) -> Result<Option<Box<dyn QueryRuntimeFilter + 'a>>, Error> {
        let offset = self.offset;
        if let Some(bind_name) = self.consume_field() {
            if self.consume("::") {
                let filter = try!(self.array());
                self.kb.push_array();
                let kb_clone = self.kb.clone();
                self.kb.pop_array();
                return Ok(Some(Box::new(BindFilter::new(bind_name, filter, kb_clone))));
            }
            //we got here so unconsume the chars
            self.offset = offset;
        }
        Ok(None)
    }

    fn array(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        if !self.consume("[") {
            return Err(Error::Parse("Expected '['".to_string()));
        }
        self.kb.push_array();
        let filter = try!(self.abool());
        self.kb.pop_array();
        try!(self.must_consume("]"));

        self.consume_boost_and_wrap_filter(filter)
    }

    pub fn order_clause(&mut self) -> Result<HashMap<String, OrderInfo>, Error> {
        let mut order_infos = HashMap::new();
        if self.consume("order") {
            let mut n = 0;
            loop {
                if let Some(rp) = try!(self.consume_keypath()) {
                    // doing the search for source 2x so user can order
                    // anyway they like. Yes it's a hack, but it simple.
                    let mut order = if self.consume("asc") {
                        Order::Asc
                    } else if self.consume("desc") {
                        Order::Desc
                    } else {
                        Order::Asc
                    };

                    let default = self.consume_default()?.unwrap_or(JsonValue::Null);

                    order = if self.consume("asc") {
                        Order::Asc
                    } else if self.consume("desc") {
                        Order::Desc
                    } else {
                        order
                    };

                    order_infos.insert(rp.to_key(),
                                       OrderInfo {
                                           field: OrderField::FetchValue(rp),
                                           order: order,
                                           order_to_apply: n,
                                           default: default,
                                       });
                } else {
                    try!(self.must_consume("score"));
                    try!(self.must_consume("("));
                    try!(self.must_consume(")"));

                    self.needs_scoring = true;

                    let order = if self.consume("asc") {
                        Order::Asc
                    } else if self.consume("desc") {
                        Order::Desc
                    } else {
                        Order::Asc
                    };

                    order_infos.insert("score()".to_string(),
                                       OrderInfo {
                                           field: OrderField::Score,
                                           order_to_apply: n,
                                           order: order,
                                           default: JsonValue::Null,
                                       });
                }

                if !self.consume(",") {
                    break;
                }
                n += 1;
            }
            if order_infos.is_empty() {
                return Err(Error::Parse("Expected field path in order expression.".to_string()));
            }
        }
        Ok(order_infos)
    }

    pub fn return_clause(&mut self) -> Result<Box<dyn Returnable>, Error> {
        if self.consume("return") {
            if let Some(ret_value) = try!(self.ret_value()) {
                Ok(ret_value)
            } else {
                Err(Error::Parse("Expected key, object or array to return.".to_string()))
            }
        } else {
            let mut rp = ReturnPath::new();
            rp.push_object_key("_id".to_string());
            Ok(Box::new(RetValue {
                            rp: rp,
                            ag: None,
                            default: JsonValue::Null,
                            order_info: None,
                        }))
        }
    }

    fn ret_object(&mut self) -> Result<Box<dyn Returnable>, Error> {
        try!(self.must_consume("{"));
        let mut fields: Vec<(String, Box<dyn Returnable>)> = Vec::new();
        loop {
            if let Some(field) = try!(self.consume_key()) {
                try!(self.must_consume(":"));
                if let Some(ret_value) = try!(self.ret_value()) {
                    fields.push((field, ret_value));
                    if !self.consume(",") {
                        break;
                    }
                } else {
                    return Err(Error::Parse("Expected key to return.".to_string()));
                }
            } else {
                break;
            }
        }

        try!(self.must_consume("}"));
        Ok(Box::new(RetObject { fields: fields }))
    }

    fn ret_array(&mut self) -> Result<Box<dyn Returnable>, Error> {
        try!(self.must_consume("["));
        let mut slots = Vec::new();
        loop {
            if let Some(ret_value) = try!(self.ret_value()) {
                slots.push(ret_value);
                if !self.consume(",") {
                    break;
                }
            } else {
                break;
            }
        }
        try!(self.must_consume("]"));
        Ok(Box::new(RetArray { slots: slots }))

    }

    fn ret_value(&mut self) -> Result<Option<Box<dyn Returnable>>, Error> {
        if self.consume("true") {
            return Ok(Some(Box::new(RetLiteral { json: JsonValue::True })));
        } else if self.consume("false") {
            return Ok(Some(Box::new(RetLiteral { json: JsonValue::False })));
        } else if self.consume("null") {
            return Ok(Some(Box::new(RetLiteral { json: JsonValue::Null })));
        } else if self.could_consume("score") {
            let offset = self.offset;
            let _ = self.consume("score");
            if self.consume("(") {
                try!(self.must_consume(")"));
                self.needs_scoring = true;
                return Ok(Some(Box::new(RetScore { order_info: None })));
            } else {
                //wasn't the score, maybe it's a bind variable
                self.offset = offset;
            }
        }

        if let Some(aggregate) = try!(self.consume_aggregate()) {
            match aggregate.bind_name {
                Some(bind_name) => Ok(Some(Box::new(RetBind {
                    bind_name: bind_name,
                    extra_rp: aggregate.keypath,
                    ag: Some((aggregate.fun, aggregate.sep)),
                    default: aggregate.default,
                    order_info: None,
                }))),
                None => Ok(Some(Box::new(RetValue {
                    rp: aggregate.keypath,
                    ag: Some((aggregate.fun, aggregate.sep)),
                    default: aggregate.default,
                    order_info: None,
                }))),
            }
        } else if let Some(bind_name) = self.consume_field() {
            let rp = if let Some(rp) = try!(self.consume_keypath()) {
                rp
            } else {
                ReturnPath::new()
            };
            let default = self.consume_default()?.unwrap_or(JsonValue::Null);

            Ok(Some(Box::new(RetBind {
                                 bind_name: bind_name,
                                 extra_rp: rp,
                                 ag: None,
                                 default: default,
                                 order_info: None,
                             })))
        } else if let Some(rp) = try!(self.consume_keypath()) {
            let default = self.consume_default()?.unwrap_or(JsonValue::Null);

            Ok(Some(Box::new(RetValue {
                                 rp: rp,
                                 ag: None,
                                 default: default,
                                 order_info: None,
                             })))
        } else if self.could_consume("{") {
            Ok(Some(try!(self.ret_object())))
        } else if self.could_consume("[") {
            Ok(Some(try!(self.ret_array())))
        } else if let Some(string) = try!(self.consume_string_literal()) {
            Ok(Some(Box::new(RetLiteral { json: JsonValue::String(string) })))
        } else if let Some(num) = try!(self.consume_number()) {
            Ok(Some(Box::new(RetLiteral { json: JsonValue::Number(num) })))
        } else {
            Ok(None)
        }
    }

    pub fn limit_clause(&mut self) -> Result<usize, Error> {
        if self.consume("limit") {
            if let Some(i) = try!(self.consume_integer()) {
                if i <= 0 {
                    return Err(Error::Parse("limit must be an integer greater than 0".to_string()));
                }
                Ok(i as usize)
            } else {
                return Err(Error::Parse("limit expects an integer greater than 0".to_string()));
            }
        } else {
            Ok(usize::MAX)
        }
    }

    fn json(&mut self) -> Result<Option<JsonValue>, Error> {
        if self.could_consume("{") {
            Ok(Some(try!(self.json_object())))
        } else if self.could_consume("[") {
            Ok(Some(try!(self.json_array())))
        } else {
            Ok(try!(self.json_primitive()))
        }
    }

    fn must_consume_json_primitive(&mut self) -> Result<JsonValue, Error> {
        if let Some(json) = try!(self.json_primitive()) {
            Ok(json)
        } else {
            Err(Error::Parse("Expected JSON primitive.".to_string()))
        }
    }

    /// JSON primites are strings, numbers, booleans and null
    fn json_primitive(&mut self) -> Result<Option<JsonValue>, Error> {
        if let Some(string) = try!(self.consume_string_literal()) {
            Ok(Some(JsonValue::String(string)))
        }
        // The else is needed becaue of https://github.com/rust-lang/rust/issues/37510
        else {
            if self.consume("true") {
                Ok(Some(JsonValue::True))
            } else if self.consume("false") {
                Ok(Some(JsonValue::False))
            } else if self.consume("null") {
                Ok(Some(JsonValue::Null))
            } else if let Some(num) = try!(self.consume_number()) {
                Ok(Some(JsonValue::Number(num)))
            } else if let Some(json) = try!(self.consume_param()) {
                Ok(Some(json))
            } else {
                Ok(None)
            }
        }
    }

    fn json_object(&mut self) -> Result<JsonValue, Error> {
        try!(self.must_consume("{"));
        let mut object = Vec::new();
        if self.consume("}") {
            return Ok(JsonValue::Object(object));
        }
        loop {
            if let Some(field) = try!(self.consume_key()) {
                try!(self.must_consume(":"));
                if let Some(json) = try!(self.json()) {
                    object.push((field, json));
                    if !self.consume(",") {
                        break;
                    }
                } else {
                    return Err(Error::Parse("Invalid json found".to_string()));
                }
            } else {
                return Err(Error::Parse("Invalid json found".to_string()));
            }
        }
        try!(self.must_consume("}"));
        Ok(JsonValue::Object(object))
    }

    fn json_array(&mut self) -> Result<JsonValue, Error> {
        try!(self.must_consume("["));
        let mut array = Vec::new();
        if self.consume("]") {
            return Ok(JsonValue::Array(array));
        }
        loop {
            if let Some(json) = try!(self.json()) {
                array.push(json);
                if !self.consume(",") {
                    break;
                }
            } else {
                return Err(Error::Parse("Invalid json found".to_string()));
            }
        }
        try!(self.must_consume("]"));
        Ok(JsonValue::Array(array))
    }

    pub fn build_filter(&mut self) -> Result<Box<dyn QueryRuntimeFilter + 'a>, Error> {
        self.ws();
        Ok(try!(self.find()))
    }

    pub fn non_ws_left(&mut self) -> Result<(), Error> {
        self.ws();
        if self.offset != self.query.len() {
            Err(Error::Parse(format!("At character {} unexpected {}.",
                                     self.offset,
                                     &self.query[self.offset..])))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Parser;

    use std::collections::HashMap;
    use std::rc::Rc;
    use index::{Index, OpenOptions};

    #[test]
    fn test_whitespace() {
        let dbname = "target/tests/test_whitespace";
        let _ = Index::drop(dbname);

        let index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
        let mut snapshot = Rc::new(index.new_snapshot());

        let query = " \n \t test";
        let mut parser = Parser::new(query, HashMap::new(), snapshot);
        parser.ws();
        assert_eq!(parser.offset, 5);

        snapshot = Rc::new(index.new_snapshot());
        let query = "test".to_string();
        let mut parser = Parser::new(&query, HashMap::new(), snapshot);
        parser.ws();
        assert_eq!(parser.offset, 0);
    }

    #[test]
    fn test_must_consume_string_literal() {
        let dbname = "target/tests/test_must_consume_string_literal";
        let _ = Index::drop(dbname);

        let index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
        let snapshot = Rc::new(index.new_snapshot());

        let query = r#"" \n \t test""#.to_string();
        let mut parser = Parser::new(&query, HashMap::new(), snapshot);
        assert_eq!(parser.must_consume_string_literal().unwrap(),
                   " \n \t test".to_string());
    }

    #[test]
    fn test_bad_query_syntax() {
        let dbname = "target/tests/test_bad_query_syntax";
        let _ = Index::drop(dbname);

        let index = Index::open(dbname, Some(OpenOptions::Create)).unwrap();
        let snapshot = Rc::new(index.new_snapshot());

        let query = r#"find {foo: =="bar""#.to_string();
        let mut parser = Parser::new(&query, HashMap::new(), snapshot);
        assert!(parser.find().is_err());
    }
}
