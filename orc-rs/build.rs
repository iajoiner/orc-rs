extern crate prost_build;

fn main() {
    prost_build::compile_protos(&["../proto/orc_proto.proto"],
                                &["src/"]).unwrap();
}