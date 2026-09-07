use std::io;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const MAX_CLIENT_FRAME: usize = 2 * 1024 * 1024;
pub const MAX_PEER_FRAME: usize = 64 * 1024 * 1024;

/// How much of a declared body is reserved at a time. The reader grows its
/// buffer as bytes actually arrive, so a declared length costs nothing until
/// the sender backs it up.
const BODY_CHUNK: usize = 64 * 1024;

/// How long a body may take to arrive once its header has been read.
pub const BODY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

pub async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(bytes).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R, max_frame: usize) -> io::Result<Bytes> {
    let n = read_len(r, max_frame).await?;
    read_body(r, n).await
}

/// [`read_frame`] with a deadline on the body.
///
/// The header may wait indefinitely — an idle but healthy connection sends
/// nothing for long stretches. Once a length has been declared the sender has
/// `body_timeout` to deliver it, which is what stops a connection from holding
/// a reservation open by sending four bytes and stalling.
pub async fn read_frame_deadline<R: AsyncReadExt + Unpin>(
    r: &mut R,
    max_frame: usize,
    body_timeout: Duration,
) -> io::Result<Bytes> {
    let n = read_len(r, max_frame).await?;
    match tokio::time::timeout(body_timeout, read_body(r, n)).await {
        Ok(res) => res,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "frame body did not arrive before the deadline",
        )),
    }
}

async fn read_len<R: AsyncReadExt + Unpin>(r: &mut R, max_frame: usize) -> io::Result<usize> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > max_frame {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame exceeds maximum size"));
    }
    Ok(n)
}

async fn read_body<R: AsyncReadExt + Unpin>(r: &mut R, n: usize) -> io::Result<Bytes> {
    let mut buf = BytesMut::with_capacity(n.min(BODY_CHUNK));
    while buf.len() < n {
        let filled = buf.len();
        let want = (n - filled).min(BODY_CHUNK);
        buf.resize(filled + want, 0);
        r.read_exact(&mut buf[filled..]).await?;
    }
    Ok(buf.freeze())
}
