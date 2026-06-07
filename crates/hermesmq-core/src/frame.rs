use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) const MAX_FRAME: usize = 64 * 1024 * 1024;

pub(crate) fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

pub(crate) async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(bytes).await?;
    w.flush().await?;
    Ok(())
}

pub(crate) async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame exceeds maximum size"));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}
