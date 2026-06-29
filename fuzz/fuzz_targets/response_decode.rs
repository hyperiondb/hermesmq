#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;

// Client-side decode of the server wire response.
fuzz_target!(|data: &[u8]| {
    let _ = hermesmq_proto::Response::decode(data);
});
