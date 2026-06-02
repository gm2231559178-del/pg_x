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

#[derive(Debug, Clone, PartialEq)]
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_copy_data ──────────────────────────────────────────────────────

    #[test]
    fn parse_copy_data_empty() {
        let err = parse_copy_data(Bytes::new()).unwrap_err();
        assert!(matches!(err, ReplError::Protocol(_)));
    }

    #[test]
    fn parse_xlog_data() {
        // 'w' + wal_start(8) + wal_end(8) + server_time(8) + payload
        let mut buf = Vec::with_capacity(25);
        buf.push(b'w');
        buf.extend_from_slice(&0u64.to_be_bytes()); // wal_start
        buf.extend_from_slice(&1u64.to_be_bytes()); // wal_end
        buf.extend_from_slice(&100i64.to_be_bytes()); // server_time
        buf.push(b'X'); // one byte payload
        let result = parse_copy_data(Bytes::from(buf)).unwrap();
        match result {
            ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                data,
                ..
            } => {
                assert_eq!(wal_start, Lsn(0));
                assert_eq!(wal_end, Lsn(1));
                assert_eq!(data.as_ref(), b"X");
            }
            other => panic!("expected XLogData, got {other:?}"),
        }
    }

    #[test]
    fn parse_keepalive() {
        // 'k' + wal_end(8) + server_time(8) + reply_requested(1)
        let mut buf = Vec::with_capacity(17);
        buf.push(b'k');
        buf.extend_from_slice(&0xDEAD_BEEFu64.to_be_bytes());
        buf.extend_from_slice(&500i64.to_be_bytes());
        buf.push(1u8);
        let result = parse_copy_data(Bytes::from(buf)).unwrap();
        match result {
            ReplicationCopyData::KeepAlive {
                wal_end,
                reply_requested,
                ..
            } => {
                assert_eq!(wal_end, Lsn(0xDEAD_BEEF));
                assert!(reply_requested);
            }
            other => panic!("expected KeepAlive, got {other:?}"),
        }
    }

    #[test]
    fn parse_unknown_kind() {
        let buf = vec![b'?'];
        let err = parse_copy_data(Bytes::from(buf)).unwrap_err();
        assert!(matches!(err, ReplError::Protocol(_)));
    }

    #[test]
    fn parse_copy_data_too_short() {
        // 'w' with only 1 byte of payload (needs 24)
        let buf = vec![b'w', 0u8];
        let err = parse_copy_data(Bytes::from(buf)).unwrap_err();
        assert!(matches!(err, ReplError::Protocol(_)));
    }

    // ── encode_standby_status_update ─────────────────────────────────────────

    #[test]
    fn encode_standby_status_update_len() {
        let out = encode_standby_status_update(Lsn(0xABCD), 1_000_000, true);
        assert_eq!(out.len(), 34); // 'r' + 3*8 + 8 + 1
        assert_eq!(out[0], b'r');
    }

    #[test]
    fn encode_standby_status_update_reply_flag() {
        let yes = encode_standby_status_update(Lsn(0), 0, true);
        let no = encode_standby_status_update(Lsn(0), 0, false);
        assert_eq!(yes.last(), Some(&1u8));
        assert_eq!(no.last(), Some(&0u8));
    }

    // ── parse_pgoutput_boundary ──────────────────────────────────────────────

    #[test]
    fn boundary_empty() {
        assert!(parse_pgoutput_boundary(&Bytes::new()).unwrap().is_none());
    }

    #[test]
    fn boundary_begin() {
        let mut buf = Vec::new();
        buf.push(b'B');
        buf.extend_from_slice(&0u64.to_be_bytes()); // final_lsn
        buf.extend_from_slice(&1234i64.to_be_bytes()); // commit_time
        buf.extend_from_slice(&42i32.to_be_bytes()); // xid
        let result = parse_pgoutput_boundary(&Bytes::from(buf)).unwrap();
        match result {
            Some(PgOutputBoundary::Begin {
                commit_time, xid, ..
            }) => {
                assert_eq!(commit_time, 1234);
                assert_eq!(xid, 42);
            }
            other => panic!("expected Begin, got {other:?}"),
        }
    }

    #[test]
    fn boundary_commit() {
        let mut buf = Vec::new();
        buf.push(b'C');
        buf.push(0u8); // flags
        buf.extend_from_slice(&1u64.to_be_bytes()); // lsn
        buf.extend_from_slice(&2u64.to_be_bytes()); // end_lsn
        buf.extend_from_slice(&5678i64.to_be_bytes()); // commit_time
        let result = parse_pgoutput_boundary(&Bytes::from(buf)).unwrap();
        match result {
            Some(PgOutputBoundary::Commit {
                lsn,
                end_lsn,
                commit_time,
            }) => {
                assert_eq!(lsn, Lsn(1));
                assert_eq!(end_lsn, Lsn(2));
                assert_eq!(commit_time, 5678);
            }
            other => panic!("expected Commit, got {other:?}"),
        }
    }

    #[test]
    fn boundary_other_tag() {
        let buf = vec![b'I']; // Insert — not a boundary
        assert!(parse_pgoutput_boundary(&Bytes::from(buf))
            .unwrap()
            .is_none());
    }
}
