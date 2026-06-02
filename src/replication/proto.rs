use bytes::{Buf, Bytes};
use std::time::{SystemTime, UNIX_EPOCH};

use super::error::{ReplError, ReplResult};
use super::lsn::Lsn;

pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

#[derive(Debug, Clone)]
pub enum ReplicationCopyData {
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        #[allow(dead_code)]
        server_time_micros: i64,
        data: Bytes,
    },
    KeepAlive {
        wal_end: Lsn,
        #[allow(dead_code)]
        server_time_micros: i64,
        reply_requested: bool,
    },
}

pub fn parse_copy_data(payload: Bytes) -> ReplResult<ReplicationCopyData> {
    if payload.is_empty() {
        return Err(ReplError::Protocol("empty CopyData payload".into()));
    }
    let mut b = payload;
    let kind = b.get_u8();
    match kind {
        b'w' => {
            if b.remaining() < 24 {
                return Err(ReplError::Protocol(format!(
                    "XLogData too short: {} bytes",
                    b.remaining()
                )));
            }
            let wal_start = Lsn(b.get_i64() as u64);
            let wal_end = Lsn(b.get_i64() as u64);
            let server_time_micros = b.get_i64();
            let data = b.copy_to_bytes(b.remaining());
            Ok(ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            })
        }
        b'k' => {
            if b.remaining() < 17 {
                return Err(ReplError::Protocol(format!(
                    "KeepAlive too short: {} bytes",
                    b.remaining()
                )));
            }
            let wal_end = Lsn(b.get_i64() as u64);
            let server_time_micros = b.get_i64();
            let reply_requested = b.get_u8() != 0;
            Ok(ReplicationCopyData::KeepAlive {
                wal_end,
                server_time_micros,
                reply_requested,
            })
        }
        _ => Err(ReplError::Protocol(format!(
            "unknown CopyData kind: 0x{kind:02x}"
        ))),
    }
}

pub fn encode_standby_status_update(
    applied: Lsn,
    client_time_micros: i64,
    reply_requested: bool,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(34);
    out.push(b'r');
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes()); // write
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes()); // flush
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes()); // apply
    out.extend_from_slice(&client_time_micros.to_be_bytes());
    out.push(if reply_requested { 1 } else { 0 });
    out
}

pub fn current_pg_timestamp() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let unix_micros = (now.as_secs() as i64) * 1_000_000 + now.subsec_micros() as i64;
    unix_micros - PG_EPOCH_MICROS
}

/// Parse pgoutput Begin and Commit boundary messages from a raw XLogData payload.
/// Returns Some(boundary) only for 'B' (Begin) and 'C' (Commit); None for all other
/// message types (Insert, Update, Delete, Relation, etc.), which are handled by the decoder.
pub fn parse_pgoutput_boundary(data: &Bytes) -> ReplResult<Option<PgOutputBoundary>> {
    if data.is_empty() {
        return Ok(None);
    }
    let tag = data[0];
    let mut p = &data[1..];

    fn take_i8(p: &mut &[u8]) -> ReplResult<i8> {
        if p.is_empty() {
            return Err(ReplError::Protocol("truncated i8".into()));
        }
        let v = p[0] as i8;
        *p = &p[1..];
        Ok(v)
    }
    fn take_i32(p: &mut &[u8]) -> ReplResult<i32> {
        if p.len() < 4 {
            return Err(ReplError::Protocol("truncated i32".into()));
        }
        let (h, t) = p.split_at(4);
        *p = t;
        Ok(i32::from_be_bytes(h.try_into().unwrap()))
    }
    fn take_i64(p: &mut &[u8]) -> ReplResult<i64> {
        if p.len() < 8 {
            return Err(ReplError::Protocol("truncated i64".into()));
        }
        let (h, t) = p.split_at(8);
        *p = t;
        Ok(i64::from_be_bytes(h.try_into().unwrap()))
    }

    match tag {
        b'B' => {
            let final_lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let commit_time = take_i64(&mut p)?;
            let xid = take_i32(&mut p)? as u32;
            Ok(Some(PgOutputBoundary::Begin {
                final_lsn,
                commit_time,
                xid,
            }))
        }
        b'C' => {
            let _flags = take_i8(&mut p)?;
            let lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let end_lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let commit_time = take_i64(&mut p)?;
            Ok(Some(PgOutputBoundary::Commit {
                lsn,
                end_lsn,
                commit_time,
            }))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Clone)]
pub enum PgOutputBoundary {
    Begin {
        final_lsn: Lsn,
        commit_time: i64,
        xid: u32,
    },
    Commit {
        lsn: Lsn,
        end_lsn: Lsn,
        commit_time: i64,
    },
}
