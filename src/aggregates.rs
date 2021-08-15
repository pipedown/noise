use std::cmp::Ordering;

use json_value::JsonValue;

pub type AggregateInitFun = fn(JsonValue) -> JsonValue;
pub type AggregateActionFun = fn(&mut JsonValue, JsonValue, Option<&JsonValue>);
pub type AggregateExtractFun = fn(&mut JsonValue);

#[derive(PartialEq, Eq, Clone)]
pub enum AggregateFun {
    GroupAsc,
    GroupDesc,
    Sum,
    Max,
    MaxArray,
    Min,
    MinArray,
    Array,
    ArrayFlat,
    Concat,
    Avg,
    Count,
}

pub struct AggregateFunImpls {
    // Initalizes for a computing the aggregate action (optional)
    pub init: Option<AggregateInitFun>,

    // The actual aggregate action function
    pub action: AggregateActionFun,

    // extracts the final aggregate value (optional)
    pub extract: Option<AggregateExtractFun>,
}

impl AggregateFun {
    pub fn get_fun_impls(&self) -> AggregateFunImpls {
        match *self {
            AggregateFun::GroupAsc => panic!("cannot get aggregate fun for grouping!"),
            AggregateFun::GroupDesc => panic!("cannot get aggregate fun for grouping!"),
            AggregateFun::Sum => AggregateFunImpls {
                init: Some(AggregateFun::sum_init),
                action: AggregateFun::sum,
                extract: None,
            },
            AggregateFun::Max => AggregateFunImpls {
                init: None,
                action: AggregateFun::max,
                extract: None,
            },
            AggregateFun::Min => AggregateFunImpls {
                init: None,
                action: AggregateFun::min,
                extract: None,
            },
            AggregateFun::MaxArray => AggregateFunImpls {
                init: Some(AggregateFun::max_array_init),
                action: AggregateFun::max_array,
                extract: None,
            },
            AggregateFun::MinArray => AggregateFunImpls {
                init: Some(AggregateFun::min_array_init),
                action: AggregateFun::min_array,
                extract: None,
            },
            AggregateFun::Array => AggregateFunImpls {
                init: Some(AggregateFun::array_init),
                action: AggregateFun::array,
                extract: None,
            },
            AggregateFun::ArrayFlat => AggregateFunImpls {
                init: Some(AggregateFun::array_flat_init),
                action: AggregateFun::array_flat,
                extract: None,
            },
            AggregateFun::Concat => AggregateFunImpls {
                init: Some(AggregateFun::concat_init),
                action: AggregateFun::concat,
                extract: None,
            },
            AggregateFun::Avg => AggregateFunImpls {
                init: Some(AggregateFun::avg_init),
                action: AggregateFun::avg,
                extract: Some(AggregateFun::avg_final),
            },
            AggregateFun::Count => AggregateFunImpls {
                init: Some(AggregateFun::count_init),
                action: AggregateFun::count,
                extract: None,
            },
        }
    }

    fn sum_init(existing: JsonValue) -> JsonValue {
        let mut base = JsonValue::Number(0.0);
        AggregateFun::sum(&mut base, existing, None);
        base
    }

    fn sum(existing: &mut JsonValue, new: JsonValue, user_arg: Option<&JsonValue>) {
        match new {
            JsonValue::Number(new) => {
                if let JsonValue::Number(ref mut existing) = *existing {
                    *existing += new;
                }
            }
            JsonValue::Array(vec) => {
                for v in vec {
                    AggregateFun::sum(existing, v, user_arg);
                }
            }
            _ => (),
        }
    }

    fn max(existing: &mut JsonValue, new: JsonValue, _user_arg: Option<&JsonValue>) {
        if *existing < new {
            *existing = new
        }
    }

    fn min(existing: &mut JsonValue, new: JsonValue, _user_arg: Option<&JsonValue>) {
        if *existing > new {
            *existing = new
        }
    }

    fn max_array_init(existing: JsonValue) -> JsonValue {
        // The default value is an array, which can never be a value because arrays are always
        // traversed. It's possible we never encounter a value due to only encountering empty
        // arrays, in which case the final value is an empty array meaning no values encountered.
        let mut val = JsonValue::Array(vec![]);
        AggregateFun::max_array(&mut val, existing, None);
        val
    }

    fn max_array(existing: &mut JsonValue, new: JsonValue, user_arg: Option<&JsonValue>) {
        if let JsonValue::Array(vec) = new {
            for v in vec {
                AggregateFun::max_array(existing, v, user_arg);
            }
        } else if let JsonValue::Array(_) = *existing {
            *existing = new;
        } else if (*existing).cmp(&new) == Ordering::Less {
            *existing = new;
        }
    }

    fn min_array_init(existing: JsonValue) -> JsonValue {
        // The default value is an array, which can never be a value because arrays are always
        // traversed. It's possible we never encounter a value due to only encountering empty
        // arrays, in which case the final value is an empty array meaning no values encountered.
        let mut val = JsonValue::Array(vec![]);
        AggregateFun::min_array(&mut val, existing, None);
        val
    }

    fn min_array(existing: &mut JsonValue, new: JsonValue, user_arg: Option<&JsonValue>) {
        if let JsonValue::Array(vec) = new {
            for v in vec {
                AggregateFun::min_array(existing, v, user_arg);
            }
        } else if let JsonValue::Array(_) = *existing {
            *existing = new;
        } else if (*existing).cmp(&new) == Ordering::Greater {
            *existing = new;
        }
    }

    fn array_init(existing: JsonValue) -> JsonValue {
        JsonValue::Array(vec![existing])
    }

    fn array(existing: &mut JsonValue, new: JsonValue, _user_arg: Option<&JsonValue>) {
        if let JsonValue::Array(ref mut existing) = *existing {
            existing.push(new);
        }
    }

    fn array_flat_init(existing: JsonValue) -> JsonValue {
        let mut new = JsonValue::Array(vec![]);
        AggregateFun::array_flat(&mut new, existing, None);
        new
    }

    fn array_flat(existing: &mut JsonValue, new: JsonValue, user_arg: Option<&JsonValue>) {
        if let JsonValue::Array(vec) = new {
            for v in vec.into_iter() {
                AggregateFun::array_flat(existing, v, user_arg);
            }
        } else if let JsonValue::Array(ref mut existing) = *existing {
            existing.push(new);
        }
    }

    fn concat_init(existing: JsonValue) -> JsonValue {
        if let JsonValue::String(_) = existing {
            existing
        } else {
            JsonValue::String(String::new())
        }
    }

    fn concat(existing: &mut JsonValue, new: JsonValue, user_arg: Option<&JsonValue>) {
        if let JsonValue::String(ref mut existing) = *existing {
            if let JsonValue::String(new) = new {
                if let Some(&JsonValue::String(ref user_arg)) = user_arg {
                    existing.push_str(user_arg);
                    existing.push_str(&new);
                }
            }
        }
    }

    fn avg_init(existing: JsonValue) -> JsonValue {
        if let JsonValue::Number(_) = existing {
            JsonValue::Array(vec![existing, JsonValue::Number(1.0)])
        } else if let JsonValue::Array(_) = existing {
            let mut avg = JsonValue::Array(vec![JsonValue::Number(0.0), JsonValue::Number(0.0)]);
            AggregateFun::avg(&mut avg, existing, None);
            avg
        } else {
            JsonValue::Array(vec![JsonValue::Number(0.0), JsonValue::Number(0.0)])
        }
    }

    fn avg(existing: &mut JsonValue, new: JsonValue, user_arg: Option<&JsonValue>) {
        if let JsonValue::Number(new) = new {
            if let JsonValue::Array(ref mut array) = *existing {
                let mut avg = if let JsonValue::Number(ref avg) = array[0] {
                    *avg
                } else {
                    // can't happen but compiler need this here
                    1.0
                };

                let mut count = if let JsonValue::Number(ref count) = array[1] {
                    *count
                } else {
                    // can't happen but compiler need this here
                    1.0
                };

                avg = (avg * count + new) / (count + 1.0);
                count += 1.0;
                array[0] = JsonValue::Number(avg);
                array[1] = JsonValue::Number(count);
            }
        } else if let JsonValue::Array(vec) = new {
            for v in vec.into_iter() {
                AggregateFun::avg(existing, v, user_arg);
            }
        }
    }

    fn avg_final(existing: &mut JsonValue) {
        let json = if let JsonValue::Array(ref mut array) = *existing {
            if let JsonValue::Number(ref avg) = array[0] {
                if let JsonValue::Number(ref count) = array[1] {
                    if *count == 0.0 {
                        JsonValue::Null
                    } else {
                        JsonValue::Number(*avg)
                    }
                } else {
                    // can't happen but compiler need this here
                    JsonValue::Null
                }
            } else {
                // can't happen but compiler need this here
                JsonValue::Null
            }
        } else {
            // can't happen but compiler need this here
            JsonValue::Null
        };
        *existing = json
    }

    fn count_init(_existing: JsonValue) -> JsonValue {
        JsonValue::Number(1.0)
    }

    fn count(existing: &mut JsonValue, _: JsonValue, _user_arg: Option<&JsonValue>) {
        if let JsonValue::Number(ref mut num) = *existing {
            *num += 1.0;
        }
    }
}
