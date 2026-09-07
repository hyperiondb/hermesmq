#![no_main]

use libfuzzer_sys::fuzz_target;

// Raw bytes off the peer TCP port. That port has no auth and no TLS, so this
// decoder is the first thing an unauthenticated attacker reaches.
fuzz_target!(|data: &[u8]| {
    let _ = hermesmq_core::engine::fuzz_decode_peer_request(data);
    let _ = hermesmq_core::engine::fuzz_decode_peer_response(data);
});
