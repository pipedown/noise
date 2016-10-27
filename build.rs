extern crate capnpc;

fn main() {
    ::capnpc::compile("capnp", &["capnp/records.capnp"]).unwrap();
}
