use bytes::Buf;
use prost::Message;
use std::io::Cursor;

// Include the `orc` module, which is generated from orc_proto.proto.
pub mod orc_proto {
    include!(concat!(env!("OUT_DIR"), "/orc.proto.rs"));
}

pub fn is_orc_file(path: &str) -> bool {
    let mut f = File::open(path)?;
    
}