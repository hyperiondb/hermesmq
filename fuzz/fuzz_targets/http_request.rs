#![no_main]

use libfuzzer_sys::fuzz_target;

// The HTTP port is unauthenticated. Its whole parser is "take the first line,
// split on whitespace, take field two, cut at the query string", and the result
// is interpolated into nothing but a fixed route match. Check that the parse
// cannot carry a CR or LF out of the request, which is what would turn the path
// into an injected response header.
fuzz_target!(|data: &[u8]| {
    let path = hermesmq_core::http::parse_path(data);
    assert!(
        !path.contains('\r') && !path.contains('\n'),
        "parsed path carries a line break: {path:?}"
    );
    assert!(
        !path.chars().any(char::is_whitespace),
        "parsed path carries whitespace: {path:?}"
    );
    assert!(!path.contains('?'), "parsed path kept a query string: {path:?}");
});
