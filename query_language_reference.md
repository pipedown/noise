# Noise Query Language

The Noise query language is an expressive example-based syntax for finding documents, formatting and returning specific information in the documents, performing relevancy scoring, ordering and aggregations.

## Find Clause

All queries have a `find` clause followed by an example based query syntax. It is a combination of expressions that consist of three parts: The key to query, an operator and the value to match.

This query will return the `_id` of every document with a `{"foo": "bar",...}`

```
find {foo: == "bar"}
```

This query will match all documents in the index and return their `_id`.

```
find {}
```

To match on multiple fields, or even nested fields, simply construct the same json structure in query form.

Match on two fields:

```
find {foo: == "bar", fizz: == "buzz"}
```

Match on fields, one nested within another:

```
find {foo: == "bar", fizz: {fazz: == "buzz"}}
```

### Word Match Operator

`~=` is the full text match operator. Use it find a word in a text field.

```
find {body: ~= "word"}
```

Put multiple words in the quoted string to find a phrase in the field.

```
find {body: ~= "a multi word sentence"}
```

To find words that are within a specified distance of each other, put the the maximum word distance in the operator. This example will return results where each word is with the 50 words of the others.

```
find {body: ~50= "bitcoin gold price"}
```

### Comparison Operators

Noise supports the following comparison operators:

|Operator|Description|Types
---------|-----------|-----
|`==`|Equality|Strings, Numbers, true, false, null
|`>`|Less Than|Numbers
|`<`|Greater Than|Numbers
|`>=`|Less Than or Equal|Numbers
|`<=`|Greater Than or Equal|Numbers

Noise does not do type conversions of datatypes. Strings only compare with strings, number only compare with numbers, etc.

### Finding Things in Arrays

Let's say you have document like this with text in an array:

```
{"foo": ["bar", "baz"]}
```

To find element with value `"baz"` in the array, use syntax like this:

```
find {foo:[ == "baz"]}
```

If objects are nested in array, like this:

```
{"foo": [{"fiz": "bar"}, {"fiz": "baz"}]}
```

To find a `{"fiz": "baz"}` in the array, use syntax like this:

```
find {foo: [{fiz: == "baz"}]}
```

### Boolean Logic and Parens

Noise has full support for boolean logic using `&&` (logical AND) and `||` (logical OR) operators and nesting logic with parens.

The comma `,` in objects is actually the same as the `&&` operator. They can be used interchangeably for which ever is more readable.

Find a doc with `"foo"` or `"bar"` in the `body`:

```
find {body: ~= "foo" || body: ~= "bar"}
```

Find a doc that has `"foo"` or `"bar"` and has `"baz"` or `"biz"` in the `body`:

```
find {(body: ~= "foo" || body: ~= "bar") &&
      (body: ~= "baz" || body: ~= "biz")}
```

The fields can be nested as well. Find a doc where either the nested field `fiz` contains either `"baz"` or `"biz"`.

```
find {foo: {fiz: ~= "baz" || fiz: ~= "biz"}}
```


### Not Operator

Use the `!` (logical NOT) to exclude matching criteria.

Find docs where `foo` has value `"bar"` and `fab` does not have value `"baz"`:

```
find {foo: == "bar", fab: !== "baz"}
```

You can use logical not with parentheses to negate everything enclosed. This example finds docs where `foo` has value `"bar"` and `fab` does not have value `"baz"` or `"biz"`':

```
find {foo: == "bar", !(fab: == "baz" || fab: == "biz")}
```

You cannot have every clause be negated. Query need at least one non-negated clauses.

Illegal:

```
find {foo: !~= "bar" && foo: !~= "baz"}
```

Illegal:

```
find {!(foo: ~= "bar" && foo: ~= "baz"})
```

Also double negation is not allowed.

Illegal:

```
find {foo ~= "waz" && !(foo: ~= "bar" && foo: !~= "baz"})
```

### Relevancy Scoring and Boosting

Relevancy scoring uses a combination boolean model and Term Frequency/Inverse Document Frequency (TF/IDF) scoring system, very similar to Lucene and Elastic Search. The details of the scoring model is beyond the scope of the document.

To return results in relevancy score order (most relevant first), simply use the order clause with the `score()` function.

```
find {subject: ~= "hammer" || body: ~= "hammer"}
order score() desc
```

But if you want matches in `subject` fields to score higher than in `body` fields, you can boost the score with the `^` operator. It is a multiplier of the scores of associated clauses.

This boosts `subject` matches by 2x:

```
find {subject: ~= "hammer"^2 || body: ~= "hammer"}
order score() desc
```

You can also boost everything in parenthesis or objects or arrays:

```
find {(subject: ~= "hammer" || subject: ~= "nails")^2 ||
       body: ~= "hammer" ||  body: ~= "nails"}
order score() desc
```
Another way to express the same thing:

```
find {subject: ~= "hammer" || subject: ~= "nails"}^2 ||
     {body: ~= "hammer" || body: ~= "nails"}
order score() desc
```


## Order Clause

To return results in a particular order, use the order clause.

This will order results ascending based on the contents of the `baz` field:

```
find {foo: == "bar"}
order .baz
```

If `baz` doesn't existing, `null` be the value used for ordering.

This will order `baz` descending:

```
find {foo: == "bar"}
order .baz
```

This will order `baz` ascending:

```
find {foo: == "bar"}
order .baz asc
```

This will order `baz` ascending with default value of `1` if no `baz` value exists:

```
find {foo: == "bar"}
order .baz asc default=1
```

This will order `baz` ascending, for values of `baz` that are the same, those results are now ordered as `biz` ascending.

```
find {foo: == "bar"}
order .baz asc, .biz dsc
```

## Return Clause

The return clause is how data or scoring is returned to the client. You can extract the whole document, a single field, multiple fields, and perform aggregations.

For this section these examples the following document will be used:

```json
{
  "_id": "example",
  "foo": "bar",
  "baz": {"biz": "bar"},
  "faz": [
    {"fiz": 213},
    {"biz": 5463},
    {"biz": 73}
  ]
}
```

### Basic Dot Notation

A leading dot indicates the root of the document. To return the whole document, place a single dot in return clause.

This will return the whole document for each document found.

```Thrift
find
    {foo: == "bar"}
return
    .
// [{
//   "_id": "example",
//   "foo": "bar",
//   "baz": {"biz": "bar"},
//   "faz": [
//     {"fiz": 213},
//     {"biz": 5463},
//     {"biz": 73}
//   ]
// }]
```

To return a specific field, place the field name after the dot:

```Thrift
find {foo: == "bar"}
return .baz
// [{"biz": "bar"}]
```

To return a nested field, use another dot:

```Thrift
find {foo: == "bar"}
return .baz.biz
// ["bar"]
```

To return an array element, use the array notation:

```Thrift
find {foo: == "bar"}
return .faz[1]
// [{"biz": 5463}]
```

To return an object field nested in the array, add a dot after the array notation:

```Thrift
find {foo: == "bar"}
return .faz[1].biz
// [5463]
```

To return multiple values, embed the return paths in other JSON structures.

For each match this example returns 2 values inside an array:

```Thrift
find {foo: == "bar"}
return [.baz, .faz]
// [[
//   {"biz": "bar"},
//   [{"fiz": 213}, {"biz": 5463}, {"biz": 73}]
// ]]
```

For each match this example return 2 values inside an object:

```Thrift
find {foo: == "bar"}
return {baz: .baz, faz: .faz}
// [{
//   "baz": {"biz": "bar"},
//   "faz": [{"fiz": 213}, {"biz": 5463}, {"biz": 73}]
// }]
```

### Missing Values

Sometimes you'll want to return a field that doesn't exist on a matching document. When that happens, `null` is returned.

If you'd like a different value to be returned, use the `default=<json>` option, like this:

```Thrift
find {foo: == "bar"}
return .hammer default=0
// [0]
```

Each returned value can have a default as well.

```Thrift
find {foo: == "bar"}
return {baz: .baz default=0, hammer: .hammer default=1}
// [{
//   "baz": {"biz": "bar"},
//   "hammer": 1
// }]
```



### Return a Field from All Objects Inside an Array

If want to return a nested field inside an array, but for each object in the array, use the `[]` with no index.

This will return each biz field as an array of values:

```Thrift
find {foo: == "bar"}
return .faz[].biz
// [[5463, 73]]
```

### Bind Variables: Return Only Matched Array Elements

If you are searching for nested values or objects nested in arrays, and you want to return only the match objects, use the bind syntax before the array in the query. The bound value is always an array, as multiple elements might match.

Say you have a document like this:

```json
{
  "_id": "a",
  "foo": [
    {"fiz": "bar", "val": 4}, {"fiz": "baz", "val": 7}
  ],
  "bar": [
    {"fiz": "baz", "val": 9}
  ]
}

```

You want to return the object where `{"fiz": "bar", ...}` (but not the others), use you a bind variable (`var::[...]`), like this:

```Thrift
find {foo: x::[{fiz: == "bar"}]}
return x
// [[{"fiz": "bar", "val": 4}]]
```

If instead you want to return the `val` field, add the `.val` to the bind variable like this:

```Thrift
find {foo: x::[{fiz: == "bar"}]}
return x.val
// [[4]]
```

You can have any number of bind variables:

```Thrift
find {foo: x::[{fiz: == "bar"}], foo: y::[{fiz: == "baz"}]}
return [x.val, y.val]
// [[[4], [7]]]
```

The same query as the previous one, but returning an object:

```Thrift
find {foo: x::[{fiz: == "bar"}], foo: y::[{fiz: == "baz"}]}
return {x: x.val, y: y.val}
// [{"x": [4], "y": [7]}]
```

You can reuse bind variables in different clauses and they'll be combined:

```Thrift
find {foo: x::[{fiz: == "baz"}] || bar: x::[{fiz: == "baz"}]}
return {x: x.val}
// [{"x": [7, 9]}]
```

## Limit Clause

To limit the number of results, use a limit clause at the end of the query.

This limits the results to the first 10 found:

```Thrift
find {foo: == "bar"}
return .baz
limit 10
```


## Grouping and Aggregation

Noise includes ways to group rows together and aggregate values.

Values you want to group together use `group(...)` function in the `return` clause.

For values that are grouped together you can then perform aggregations on other values and return that aggregation. If a group function is used, all other fields must also be grouped or aggregated.

The aggregation functions available are:

|function      | Description|
---------------|-------------
|`array(...)`|Returns all values in the group as values in an array.|
|`array_flat(...)`|Returns all values in the group as values in an array. However if an array is encountered it extracts all the values inside the array (and further nested arrays) and returns them as a singe flat array|
|`avg(...)`|Averages numeric values in the group. If numeric values are in arrays, it extracts the values from the arrays. Even if arrays are nested in arrays, it extracts through all levels of nested arrays and averages them. |
|`count()`| Returns the count of the grouped rows for each grouping. |
|`concat(... [sep="..."])`| Returns all the strings in the group as a single concatenated string. Other value types are ignored. Use the optional `sep="..."` to specify a separator between string values.|
|`max(...)`|Returns the maximum value in the group. See type ordering below to see how different types are considered. |
|`max_array(...)`|Returns the maximum value in the group, if array is encountered the values inside the array are extracted and considered.|
|`min(...)`|Returns the minimum value in the group. See type ordering below to see how different types are considered.|
|`min_array(...)`|Returns the minimum value in the group, if array is encountered the values inside the array are extracted and considered.|
|`sum(...)`|Sums numeric values in the group. If numeric values are in arrays, it extracts the values from the arrays. Even if arrays are nested in arrays, it extracts through all levels of nested arrays and sums them.|

To perform grouping and/or aggregate, each field returned will need either a grouping or a aggregate function. It's an error it on some returned fields but not others.

Groupings are are ordered first on the leftmost `group(...)` function, then on the next leftmost, etc.

You do not need to use `group(...)` to perform aggregates. If you have no `group(...)` defined, then all rows are aggregated into a single row.



### Max/Min Type Ordering
The ordering of types for `max(...)` and `min(...)` is as follows:

null < false < true < number < string < array < object


## Group/Aggregate Examples


Let's say we have documents like this:

```json
{"foo":"group1", "baz": "a", "bar": 1}
{"foo":"group1", "baz": "b", "bar": 2}
{"foo":"group1", "baz": "c", "bar": 3}
{"foo":"group1", "baz": "a", "bar": 1}
{"foo":"group1", "baz": "b", "bar": 2}
{"foo":"group1", "baz": "c", "bar": 3}
{"foo":"group1", "baz": "a", "bar": 1}
{"foo":"group1", "baz": "b", "bar": 2}
{"foo":"group1", "baz": "c", "bar": 3}
{"foo":"group1", "baz": "a", "bar": 1}
{"foo":"group1", "baz": "b", "bar": 2}
{"foo":"group1", "baz": "c", "bar": 3}
{"foo":"group2", "baz": "a", "bar": "a"}
{"foo":"group2", "baz": "a", "bar": "b"}
{"foo":"group2", "baz": "b", "bar": "a"}
{"foo":"group2", "baz": "b", "bar": "b"}
{"foo":"group2", "baz": "a", "bar": "a"}
{"foo":"group2", "baz": "a", "bar": "c"}
{"foo":"group2", "baz": "b", "bar": "d"}
{"foo":"group2", "baz": "b", "bar": "e"}
{"foo":"group2", "baz": "a", "bar": "f"}
{"foo":"group3", "baz": "a", "bar": "a"}
("foo":"group3",             "bar": "b"}
{"foo":"group3", "baz": "b", "bar": "a"}
{"foo":"group3", "baz": "b", "bar": "b"}
{"foo":"group3", "baz": "a", "bar": "a"}
{"foo":"group3", "baz": "a"            }
{"foo":"group3", "baz": "b", "bar": "d"}
{"foo":"group3", "baz": "b", "bar": "e"}
{"foo":"group3", "baz": "a", "bar": "f"}
```

### Count

Query:
```
find {foo: == "group1"}
return {baz: group(.baz), count: count()}
```
Results:

```json
{"baz":"a","bar":4}
{"baz":"b","bar":4}
{"baz":"c","bar":4}

```

### Sum

Query:

```
find {foo: == "group1"}
return {baz: group(.baz), bar: sum(.bar)}
```

Results:

```json
{"baz":"a","bar":4}
{"baz":"b","bar":8}
{"baz":"c","bar":12}

```

### Avg

Query:

```
find {foo: == "group1"}
return {avg: avg(.bar)}
```

Results:

```json
{"bar":2}
```

### Concat

Query:

```
find {foo: =="group1"}
return {baz: group(.baz), concat: concat(.baz sep="|")}
```

Results:

```json
{"baz":"a","concat":"a|a|a|a"}
{"baz":"b","concat":"b|b|b|b"}
{"baz":"c","concat":"c|c|c|c"}
```

### Max

Query:

```
find {foo: =="group1"}
return {max: max(.bar)}
```
Results:

```json
{"max":3}
```

Query:

```
find {foo: =="group1"}
return {max: max(.baz)}
```

Results:

```json
{"max":"c"}
```

### Min

Query:

```
find {foo: =="group1"}
return {min: min(.bar)}
```

Results:

```json
{"min":1}
```

### Group Ordering

Query:

```
find {foo: =="group2"}
return [group(.baz order=asc), group(.bar order=desc), count()]
```

Results:

```json
["a","f",1]
["a","c",1]
["a","b",1]
["a","a",2]
["b","e",1]
["b","d",1]
["b","b",1]
["b","a",1]
```

### Default Values

Query:

```
find {foo: =="group2"}
return [group(.baz order=asc) default="a", group(.bar order=desc) default="c", count()];
```

Results:

```json
["a","f",1]
["a","c",1]
["a","b",1]
["a","a",2]
["b","e",1]
["b","d",1]
["b","b",1]
["b","a",1]
```

### Arrays

When performing aggregations on arrays, some functions will extract values out of the arrays (and arrays nested in arrays).

We have documents like this:

```json
{"foo":"array1", "baz": ["a","b",["c","d",["e"]]]}
{"foo":"array1", "baz": ["f","g",["h","i"],"j"]}
{"foo":"array2", "baz": [1,2,[3,4,[5]]]}
{"foo":"array2", "baz": [6,7,[8,9],10]};
```

Query:

```
find {foo: =="array1"}
return array(.baz)
```

Results:

```json
[["f","g",["h","i"],"j"],["a","b",["c","d",["e"]]]]
```

Query:

```
find {foo: =="array1"}
return array_flat(.baz)
```

Results:

```json
["f","g","h","i","j","a","b","c","d","e"]
```

Query:

```
find {foo: =="array1"}
return max(.baz)
```

Results:

```json
["f","g",["h","i"],"j"]
```

Query:

```json
find {foo: =="array1"}
return max_array(.baz)
```

Results:

```json
"j"
```

Query:

```
find {foo: =="array1"}
return min_array(.baz)
```

Results:

```json
"a"
```

Query:

```
find {foo: =="array2"}
return avg(.baz)
```

Results:

```json
5.5
```

Query:

```

find {foo: =="array2"}
return sum(.baz)
```

Results:

```json
55
```