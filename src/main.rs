extern crate noise;

use noise::index::{Index, OpenOptions};
use noise::query::Query;

fn main() {
    let dbname = "querytestdb";
    let _ = Index::delete(dbname);

    let mut index = Index::new();
    index.open(dbname, Some(OpenOptions::Create)).unwrap();
    let _ = index.add(r#"{"_id": "foo", "hello": "world"}"#);
    index.flush().unwrap();

    let mut query_results = Query::get_matches(r#"hello="world""#.to_string(), &index).unwrap();
    //let mut query_results = Query::get_matches(r#"a.b[foo="bar"]"#.to_string(), &index).unwrap();
    println!("query results: {:?}", query_results.get_next_id());
}
