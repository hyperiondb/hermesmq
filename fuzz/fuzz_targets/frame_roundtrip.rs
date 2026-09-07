#![no_main]

use hermesmq_core::frame::{read_frame, write_frame, MAX_CLIENT_FRAME};
use libfuzzer_sys::fuzz_target;
use tokio::io::AsyncWriteExt;

// The length-prefixed codec in front of both the client and the peer port.
fuzz_target!(|data: &[u8]| {
    let data = &data[..data.len().min(64 * 1024)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let (mut a, mut b) = tokio::io::duplex(1024 * 1024);
        write_frame(&mut a, data).await.expect("write must succeed");
        let got = read_frame(&mut b, MAX_CLIENT_FRAME)
            .await
            .expect("a frame written must read back");
        assert_eq!(&got[..], data, "the frame codec changed the payload");

        // A declared length above the cap is rejected before any body is read.
        let (mut a, mut b) = tokio::io::duplex(64);
        let oversized = (MAX_CLIENT_FRAME as u32 + 1).to_be_bytes();
        a.write_all(&oversized).await.expect("header write");
        a.flush().await.expect("header flush");
        assert!(
            read_frame(&mut b, MAX_CLIENT_FRAME).await.is_err(),
            "an oversized declared length must be rejected"
        );

        // A body that stops short must be an error, not a short buffer.
        if !data.is_empty() {
            let (mut a, mut b) = tokio::io::duplex(1024 * 1024);
            let header = (data.len() as u32).to_be_bytes();
            a.write_all(&header).await.expect("header write");
            a.write_all(&data[..data.len() - 1]).await.expect("body write");
            a.flush().await.expect("body flush");
            drop(a);
            assert!(
                read_frame(&mut b, MAX_CLIENT_FRAME).await.is_err(),
                "a truncated body must be an error"
            );
        }
    });
});
