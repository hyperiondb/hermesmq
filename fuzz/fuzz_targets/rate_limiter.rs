#![no_main]

use hermesmq_core::client::RateLimiter;
use libfuzzer_sys::fuzz_target;

// The token bucket in front of poll. The rate reaches it as a client-chosen
// protobuf double, converted with `(rate_per_sec * 1000.0).round() as u64` and
// back again, so NaN, infinity, negative and subnormal values all arrive here.
//
// A request of zero tokens is included: it must be granted as zero rather than
// answered with a retry delay, so no caller can be told to retry immediately.
fuzz_target!(|input: (f64, u32, Vec<(u32, u32)>)| {
    let (client_rate, client_burst, calls) = input;

    // Exactly the conversion the SetRateLimit handler performs.
    let rate_milli = (client_rate * 1000.0).round() as u64;
    if rate_milli == 0 {
        // rate_config() reports no limit at all in this case, so take() is never
        // reached.
        return;
    }
    let rate = rate_milli as f64 / 1000.0;
    let burst = client_burst.max(1) as f64;

    let limiter = RateLimiter::default();
    for (want, refund) in calls.iter().take(64) {
        let want = *want;
        match limiter.take("t", rate, burst, want) {
            Ok(grant) => {
                assert!(grant <= want, "granted {grant} of a requested {want}");
                assert!(
                    grant >= 1 || want == 0,
                    "an Ok grant must be at least one token unless none were asked for"
                );
            }
            Err(retry_ms) => {
                assert!(want > 0, "a request of zero tokens must never be rate limited");
                assert!(retry_ms > 0, "a rate-limited reply must ask for a nonzero wait");
            }
        }
        limiter.refund("t", *refund, burst);
        let Some(tokens) = limiter.debug_tokens("t") else {
            // A request of zero tokens is answered before a bucket is created.
            continue;
        };
        assert!(!tokens.is_nan(), "the bucket went to NaN");
        assert!(
            tokens <= burst + f64::EPSILON,
            "the bucket holds {tokens} tokens above its burst of {burst}"
        );
        assert!(tokens >= 0.0, "the bucket went negative at {tokens}");
    }
});
