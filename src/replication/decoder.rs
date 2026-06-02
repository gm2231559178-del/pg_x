//! Decoder for the PostgreSQL `pgoutput` logical replication plugin wire format.
//!
//! pgoutput message type tags (first byte of the XLogData payload):
//!
//! | Byte | Message   |
//! |------|-----------|
//! | `B`  | Begin     |
//! | `C`  | Commit    |
//! | `R`  | Relation  |
//! | `I`  | Insert    |
//! | `U`  | Update    |
//! | `D`  | Delete    |
//! | `T`  | Truncate  |
//! | `O`  | Origin    |  (skipped)
//! | `Y`  | Type      |  (skipped)
//! | `M`  | Message   |  (skipped)
//!
//! ## Tuple column kinds
//!
//! Each column in a tuple is prefixed by a tag byte:
//!
//! | Tag | Meaning                                                |
//! |-----|--------------------------------------------------------|
//! |`'t'`| Text-encoded value                                     |
//! |`'n'`| SQL NULL                                               |
//! |`'u'`| Unchanged (TOAST or non-replica-identity, not sent)    |
//! |`'b'`| Binary value (proto_version ≥ 2, hex-encoded in output)|

use anyhow::{bail, Result};
use std::collections::HashMap;

use super::event::{ColVal, ColumnDef, Row, WalEvent};

// ─────────────────────────────────────────────────────────────────────────────
// Low-level byte reader
// ─────────────────────────────────────────────────────────────────────────────

struct Buf<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Buf<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.pos >= self.data.len() {
            bail!("unexpected end of buffer reading u8");
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }
    fn read_u16(&mut self) -> Result<u16> {
        self.ensure(2)?;
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }
    fn read_i32(&mut self) -> Result<i32> {
        self.ensure(4)?;
        let v = i32::from_be_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    fn read_u32(&mut self) -> Result<u32> {
        self.ensure(4)?;
        let v = u32::from_be_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    fn read_u64(&mut self) -> Result<u64> {
        self.ensure(8)?;
        let v = u64::from_be_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
    fn read_i64(&mut self) -> Result<i64> {
        self.ensure(8)?;
        let v = i64::from_be_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
    fn read_cstring(&mut self) -> Result<String> {
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.data.len() {
            bail!("unterminated cstring in replication buffer");
        }
        let s = std::str::from_utf8(&self.data[start..self.pos])
            .unwrap_or("")
            .to_string();
        self.pos += 1;
        Ok(s)
    }
    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        self.ensure(n)?;
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }
    fn ensure(&self, n: usize) -> Result<()> {
        if self.pos + n > self.data.len() {
            bail!(
                "buffer underrun: need {} bytes, have {}",
                n,
                self.data.len() - self.pos
            );
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Relation cache  (OID → schema / table / columns)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

pub type RelationCache = HashMap<u32, RelationInfo>;

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Decode one pgoutput message from a raw XLogData payload.
///
/// `data` is the bytes in `ReplicationEvent::XLogData { data, .. }` — the
/// pgoutput payload after the 24-byte XLogData header has been stripped.
///
/// Returns:
/// - `Ok(Some(event))` — a decoded WAL event
/// - `Ok(None)`        — intentionally skipped message (Origin / Type / Message)
/// - `Err(_)`          — malformed data
pub fn decode_pgoutput(data: &[u8], rel_cache: &mut RelationCache) -> Result<Option<WalEvent>> {
    let mut buf = Buf::new(data);
    dispatch(&mut buf, rel_cache)
}

// ─────────────────────────────────────────────────────────────────────────────
// Message dispatcher
// ─────────────────────────────────────────────────────────────────────────────

fn dispatch(buf: &mut Buf<'_>, rel_cache: &mut RelationCache) -> Result<Option<WalEvent>> {
    let tag = buf.read_u8()?;
    match tag {
        b'B' => decode_begin(buf),
        b'C' => decode_commit(buf),
        b'R' => decode_relation(buf, rel_cache),
        b'I' => decode_insert(buf, rel_cache),
        b'U' => decode_update(buf, rel_cache),
        b'D' => decode_delete(buf, rel_cache),
        b'T' => decode_truncate(buf, rel_cache),
        b'O' | b'Y' | b'M' => Ok(None),
        other => bail!(
            "unknown pgoutput message type: 0x{:02X} ('{}')",
            other,
            other as char
        ),
    }
}

// ── Begin ──────────────────────────────────────────────────────────────────

fn decode_begin(buf: &mut Buf<'_>) -> Result<Option<WalEvent>> {
    let lsn = buf.read_u64()?;
    let commit_time = buf.read_i64()?;
    let xid = buf.read_u32()?;
    Ok(Some(WalEvent::Begin {
        lsn: fmt_lsn(lsn),
        commit_time,
        xid,
    }))
}

// ── Commit ─────────────────────────────────────────────────────────────────

fn decode_commit(buf: &mut Buf<'_>) -> Result<Option<WalEvent>> {
    let _flags = buf.read_u8()? as i8;
    let lsn = buf.read_u64()?;
    let end_lsn = buf.read_u64()?;
    let commit_time = buf.read_i64()?;
    Ok(Some(WalEvent::Commit {
        lsn: fmt_lsn(lsn),
        end_lsn: fmt_lsn(end_lsn),
        commit_time,
    }))
}

// ── Relation ───────────────────────────────────────────────────────────────

fn decode_relation(buf: &mut Buf<'_>, rel_cache: &mut RelationCache) -> Result<Option<WalEvent>> {
    let rel_id = buf.read_u32()?;
    let schema = buf.read_cstring()?;
    let table = buf.read_cstring()?;
    let _replica_identity = buf.read_u8()?;
    let ncols = buf.read_u16()?;

    let mut columns = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        let flags = buf.read_u8()?;
        let is_key = (flags & 0x01) != 0;
        let name = buf.read_cstring()?;
        let type_id = buf.read_u32()?;
        let type_mod = buf.read_i32()?;
        columns.push(ColumnDef {
            name,
            is_key,
            type_id,
            type_modifier: type_mod,
        });
    }

    rel_cache.insert(
        rel_id,
        RelationInfo {
            schema: schema.clone(),
            table: table.clone(),
            columns: columns.clone(),
        },
    );

    Ok(Some(WalEvent::Relation {
        rel_id,
        schema,
        table,
        columns,
    }))
}

// ── Tuple decoding (shared by Insert / Update / Delete) ───────────────────

fn decode_tuple(buf: &mut Buf<'_>, columns: &[ColumnDef]) -> Result<Row> {
    let ncols = buf.read_u16()? as usize;
    let mut row = HashMap::with_capacity(ncols);

    for i in 0..ncols {
        let col = columns
            .get(i)
            .map(|c| c.name.as_str())
            .unwrap_or("?")
            .to_string();
        let val = match buf.read_u8()? {
            b'n' => ColVal::Null,
            b'u' => ColVal::Unchanged,
            b't' => {
                let len = buf.read_u32()? as usize;
                let bytes = buf.read_bytes(len)?;
                ColVal::Text(String::from_utf8_lossy(bytes).into_owned())
            }
            b'b' => {
                // Binary value (proto_version ≥ 2) — hex-encode for portability
                let len = buf.read_u32()? as usize;
                let bytes = buf.read_bytes(len)?;
                ColVal::Text(format!("\\x{}", hex::encode(bytes)))
            }
            other => bail!("unknown tuple column kind: 0x{:02X}", other),
        };
        row.insert(col, val);
    }
    Ok(row)
}

fn unknown_rel(rel_id: u32) -> RelationInfo {
    RelationInfo {
        schema: format!("unknown_{rel_id}"),
        table: "unknown".into(),
        columns: vec![],
    }
}

// ── Insert ─────────────────────────────────────────────────────────────────

fn decode_insert(buf: &mut Buf<'_>, rel_cache: &RelationCache) -> Result<Option<WalEvent>> {
    let rel_id = buf.read_u32()?;
    let _flag = buf.read_u8()?; // always b'N'
    let info = rel_cache
        .get(&rel_id)
        .cloned()
        .unwrap_or_else(|| unknown_rel(rel_id));
    let new = decode_tuple(buf, &info.columns)?;
    Ok(Some(WalEvent::Insert {
        rel_id,
        schema: info.schema,
        table: info.table,
        new,
    }))
}

// ── Update ─────────────────────────────────────────────────────────────────

fn decode_update(buf: &mut Buf<'_>, rel_cache: &RelationCache) -> Result<Option<WalEvent>> {
    let rel_id = buf.read_u32()?;
    let info = rel_cache
        .get(&rel_id)
        .cloned()
        .unwrap_or_else(|| unknown_rel(rel_id));

    // Optional old-tuple indicator:
    //   'O' = full old row  (REPLICA IDENTITY FULL)
    //   'K' = key cols only (REPLICA IDENTITY DEFAULT or INDEX)
    //   'N' = no old tuple sent; this byte is already the new-tuple flag
    let flag = buf.read_u8()?;
    let old = match flag {
        b'O' | b'K' => {
            let old_row = decode_tuple(buf, &info.columns)?;
            let _new_flag = buf.read_u8()?; // consume the 'N' that follows
            Some(old_row)
        }
        b'N' => None, // 'N' IS the new-tuple flag; no old row present
        other => bail!("unexpected update tuple flag: 0x{:02X}", other),
    };

    let new = decode_tuple(buf, &info.columns)?;
    Ok(Some(WalEvent::Update {
        rel_id,
        schema: info.schema,
        table: info.table,
        old,
        new,
    }))
}

// ── Delete ─────────────────────────────────────────────────────────────────

fn decode_delete(buf: &mut Buf<'_>, rel_cache: &RelationCache) -> Result<Option<WalEvent>> {
    let rel_id = buf.read_u32()?;
    let _flag = buf.read_u8()?; // 'O' or 'K'
    let info = rel_cache
        .get(&rel_id)
        .cloned()
        .unwrap_or_else(|| unknown_rel(rel_id));
    let old = decode_tuple(buf, &info.columns)?;
    Ok(Some(WalEvent::Delete {
        rel_id,
        schema: info.schema,
        table: info.table,
        old,
    }))
}

// ── Truncate ───────────────────────────────────────────────────────────────

fn decode_truncate(buf: &mut Buf<'_>, rel_cache: &RelationCache) -> Result<Option<WalEvent>> {
    let nrels = buf.read_u32()?;
    let options = buf.read_u8()?;
    let cascade = (options & 0x01) != 0;
    let restart_seqs = (options & 0x02) != 0;

    let mut rel_ids = Vec::with_capacity(nrels as usize);
    let mut tables = Vec::with_capacity(nrels as usize);
    for _ in 0..nrels {
        let rid = buf.read_u32()?;
        rel_ids.push(rid);
        match rel_cache.get(&rid) {
            Some(info) => tables.push(format!("{}.{}", info.schema, info.table)),
            None => tables.push(format!("<rel:{rid}>")),
        }
    }

    Ok(Some(WalEvent::Truncate {
        rel_ids,
        tables,
        cascade,
        restart_seqs,
    }))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

pub fn fmt_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}
