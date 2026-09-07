#![no_main]

use arbitrary::Arbitrary;
use hermesmq_core::client::SubTracking;
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Track {
    Deliver { offset: u8, lease: u8, vis: u16 },
    Resolve { lease: u8, ack: bool },
    Tick(u16),
    Count(u16),
}

// Per-connection lease bookkeeping for a subscription. Three maps have to stay
// in sync across deliver/resolve/live_count; a leak here grows for as long as a
// client keeps its subscription connection open.
fuzz_target!(|steps: Vec<Track>| {
    let mut t = SubTracking::default();
    let mut now: u64 = 0;
    for step in steps.iter().take(512) {
        match step {
            Track::Deliver { offset, lease, vis } => {
                let deadline = now.saturating_add(*vis as u64);
                t.deliver(*offset as u64, *lease as u64, deadline, now);
            }
            Track::Resolve { lease, ack } => {
                let until = if *ack {
                    Some(now.saturating_add(5_000))
                } else {
                    None
                };
                t.resolve(*lease as u64, until);
            }
            Track::Tick(dt) => now = now.saturating_add(*dt as u64),
            Track::Count(grace) => {
                let (by_offset, _, _) = t.debug_sizes();
                let live = t.live_count(now, *grace as u64);
                assert!(
                    live <= by_offset,
                    "live_count reported {live} of {by_offset} tracked offsets"
                );
            }
        }
        if let Err(e) = t.debug_check_invariants() {
            panic!("subscription tracking broken after {step:?}: {e}");
        }
    }

    // Resolving every alias with the clock well past every deadline must drain
    // all three maps; anything left behind is a per-connection leak.
    now = now.saturating_add(1_000_000);
    for lease in 0..=u8::MAX {
        t.resolve(lease as u64, None);
    }
    t.live_count(now, 0);
    let (by_offset, lease_to_offset, acked_recently) = t.debug_sizes();
    assert_eq!(
        (by_offset, lease_to_offset, acked_recently),
        (0, 0, 0),
        "subscription tracking did not drain"
    );
});
