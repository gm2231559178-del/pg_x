use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::error::{ReplError, ReplResult};

#[cfg(feature = "tls")]
pub const SSL_REQUEST_CODE: i32 = 80877103;

/// Send an SSLRequest message.  The server replies with a single byte:
/// - `'S'` — SSL/TLS negotiation accepted; proceed with handshake.
/// - `'N'` — SSL not available; continue without encryption.
#[cfg(feature = "tls")]
pub async fn write_ssl_request<W: AsyncWrite + Unpin>(wr: &mut W) -> ReplResult<()> {
    let buf: [u8; 8] = {
        let mut b = [0u8; 8];
        b[0..4].copy_from_slice(&8i32.to_be_bytes()); // length
        b[4..8].copy_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
        b
    };
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB

#[derive(Debug, Clone)]
pub struct BackendMessage {
    pub tag: u8,
    pub payload: Bytes,
}

pub async fn read_backend_message_into<R: AsyncRead + Unpin>(
    rd: &mut R,
    buf: &mut BytesMut,
) -> ReplResult<BackendMessage> {
    let mut hdr = [0u8; 5];
    rd.read_exact(&mut hdr).await?;
    let tag = hdr[0];
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]);

    if len < 4 {
        return Err(ReplError::Protocol(format!(
            "invalid message length: {len}"
        )));
    }
    let payload_len = (len - 4) as usize;
    if payload_len > MAX_MESSAGE_SIZE {
        return Err(ReplError::Protocol(format!(
            "message too large: {payload_len}"
        )));
    }

    buf.clear();
    buf.resize(payload_len, 0);
    rd.read_exact(&mut buf[..]).await?;
    Ok(BackendMessage {
        tag,
        payload: buf.split().freeze(),
    })
}

pub async fn write_startup_message<W: AsyncWrite + Unpin>(
    wr: &mut W,
    protocol_version: i32,
    params: &[(&str, &str)],
) -> ReplResult<()> {
    let mut buf = BytesMut::with_capacity(256);
    buf.put_i32(0);
    buf.put_i32(protocol_version);
    for (k, v) in params {
        buf.extend_from_slice(k.as_bytes());
        buf.put_u8(0);
        buf.extend_from_slice(v.as_bytes());
        buf.put_u8(0);
    }
    buf.put_u8(0);
    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_query<W: AsyncWrite + Unpin>(wr: &mut W, sql: &str) -> ReplResult<()> {
    let mut buf = BytesMut::with_capacity(sql.len() + 6);
    buf.put_u8(b'Q');
    buf.put_i32(0);
    buf.extend_from_slice(sql.as_bytes());
    buf.put_u8(0);
    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_password_message<W: AsyncWrite + Unpin>(
    wr: &mut W,
    payload: &[u8],
) -> ReplResult<()> {
    let mut buf = BytesMut::with_capacity(payload.len() + 5);
    buf.put_u8(b'p');
    buf.put_i32(0);
    buf.extend_from_slice(payload);
    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_copy_data<W: AsyncWrite + Unpin>(wr: &mut W, payload: &[u8]) -> ReplResult<()> {
    let mut buf = BytesMut::with_capacity(payload.len() + 5);
    buf.put_u8(b'd');
    buf.put_i32(0);
    buf.extend_from_slice(payload);
    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_copy_done<W: AsyncWrite + Unpin>(wr: &mut W) -> ReplResult<()> {
    let buf: [u8; 5] = {
        let mut b = [0u8; 5];
        b[0] = b'c';
        b[1..5].copy_from_slice(&4i32.to_be_bytes());
        b
    };
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}
