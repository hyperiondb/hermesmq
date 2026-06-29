#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;

// Server-side decode of the client wire request (untrusted network bytes).
fuzz_target!(|data: &[u8]| {
    let _ = hermesmq_proto::Request::decode(data);
});
