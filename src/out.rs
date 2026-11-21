//! Minimal hand-written Parquet writer: one row group, one PLAIN data page (v1) per column,
//! no compression, no statistics. Metadata is Thrift compact protocol.

use crate::parse::decode;
use crate::URow;
use std::io::Write;

pub struct CampRow {
    pub camp: usize,
    pub day: i32,
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

// ------------------------------------------------------------------ thrift compact

struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

const I32: u8 = 5;
const I64: u8 = 6;
const BIN: u8 = 8;
const LIST: u8 = 9;
const STRUCT: u8 = 12;

impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, I32);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, I64);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, BIN);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, STRUCT);
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, LIST);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    /// A struct element inside a list.
    fn elem_begin(&mut self) {
        self.last.push(0);
    }
}

// ------------------------------------------------------------------ columns

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Str,
    OptStr,
    Date,
    I64,
}

struct Column {
    name: &'static str,
    kind: Kind,
    data: Vec<u8>, // page body (def levels + values)
    nvals: usize,
}

const PT_INT32: i32 = 1;
const PT_INT64: i32 = 2;
const PT_BYTE_ARRAY: i32 = 6;

fn page_header(body_len: usize, nvals: usize, has_def: bool) -> Vec<u8> {
    let mut t = T::new();
    t.i32(1, 0); // DATA_PAGE
    t.i32(2, body_len as i32);
    t.i32(3, body_len as i32);
    t.begin(5);
    t.i32(1, nvals as i32);
    t.i32(2, 0); // PLAIN
    t.i32(3, 3); // RLE (definition levels)
    t.i32(4, 3); // RLE (repetition levels)
    t.end();
    let _ = has_def;
    t.b.push(0);
    t.b
}

/// Definition levels (bit width 1) as one bit-packed run, prefixed by its byte length.
fn def_levels(defs: &[bool]) -> Vec<u8> {
    let groups = defs.len().div_ceil(8);
    let mut run = Vec::with_capacity(groups + 10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        run.push((h as u8) | 0x80);
        h >>= 7;
    }
    run.push(h as u8);
    for g in 0..groups {
        let mut byte = 0u8;
        for i in 0..8 {
            let k = g * 8 + i;
            if k < defs.len() && defs[k] {
                byte |= 1 << i;
            }
        }
        run.push(byte);
    }
    let mut out = Vec::with_capacity(run.len() + 4);
    out.extend_from_slice(&(run.len() as u32).to_le_bytes());
    out.extend_from_slice(&run);
    out
}

fn write_file(path: &std::path::Path, cols: &[Column], nrows: usize) {
    let mut f = Vec::with_capacity(cols.iter().map(|c| c.data.len() + 64).sum::<usize>() + 4096);
    f.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new(); // (offset, total size)
    for c in cols {
        let off = f.len();
        let h = page_header(c.data.len(), c.nvals, c.kind == Kind::OptStr);
        f.extend_from_slice(&h);
        f.extend_from_slice(&c.data);
        chunks.push((off, f.len() - off));
    }
    // FileMetaData
    let mut t = T::new();
    t.i32(1, 1);
    t.list(2, STRUCT, cols.len() + 1);
    t.elem_begin();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.elem_begin();
        let (pt, conv) = match c.kind {
            Kind::Str | Kind::OptStr => (PT_BYTE_ARRAY, Some(0)), // UTF8
            Kind::Date => (PT_INT32, Some(6)),                    // DATE
            Kind::I64 => (PT_INT64, None),
        };
        t.i32(1, pt);
        t.i32(3, if c.kind == Kind::OptStr { 1 } else { 0 });
        t.bin(4, c.name.as_bytes());
        if let Some(cv) = conv {
            t.i32(6, cv);
            t.begin(10); // LogicalType union
            match c.kind {
                Kind::Date => {
                    t.begin(6); // DATE
                    t.end();
                }
                _ => {
                    t.begin(1); // STRING
                    t.end();
                }
            }
            t.end();
        }
        t.end();
    }
    t.i64(3, nrows as i64);
    t.list(4, STRUCT, 1);
    t.elem_begin();
    t.list(1, STRUCT, cols.len());
    let mut total = 0i64;
    for (c, &(off, size)) in cols.iter().zip(&chunks) {
        total += size as i64;
        t.elem_begin();
        t.i64(2, off as i64);
        t.begin(3);
        t.i32(1, match c.kind {
            Kind::Str | Kind::OptStr => PT_BYTE_ARRAY,
            Kind::Date => PT_INT32,
            Kind::I64 => PT_INT64,
        });
        t.list(2, I32, 2);
        t.zz(0); // PLAIN
        t.zz(3); // RLE
        t.list(3, BIN, 1);
        t.varint(c.name.len() as u64);
        t.b.extend_from_slice(c.name.as_bytes());
        t.i32(4, 0); // UNCOMPRESSED
        t.i64(5, nrows as i64);
        t.i64(6, size as i64);
        t.i64(7, size as i64);
        t.i64(9, off as i64);
        t.end();
        t.end();
    }
    t.i64(2, total);
    t.i64(3, nrows as i64);
    t.end();
    t.bin(6, b"shopping-sessions");
    t.b.push(0);
    f.extend_from_slice(&t.b);
    f.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
    f.extend_from_slice(b"PAR1");
    let mut file = std::fs::File::create(path).expect("create parquet");
    file.write_all(&f).expect("write parquet");
}

fn push_str(v: &mut Vec<u8>, s: &[u8]) {
    v.extend_from_slice(&(s.len() as u32).to_le_bytes());
    v.extend_from_slice(s);
}

fn i64_col(name: &'static str, it: impl Iterator<Item = i64>, n: usize) -> Column {
    let mut data = Vec::with_capacity(n * 8);
    for v in it {
        data.extend_from_slice(&v.to_le_bytes());
    }
    Column { name, kind: Kind::I64, data, nvals: n }
}

fn date_col(name: &'static str, it: impl Iterator<Item = i32>, n: usize) -> Column {
    let mut data = Vec::with_capacity(n * 4);
    for v in it {
        data.extend_from_slice(&v.to_le_bytes());
    }
    Column { name, kind: Kind::Date, data, nvals: n }
}

#[inline(never)]
pub fn write_users(path: &std::path::Path, rows: &[URow], cat_names: &[Vec<u8>]) {
    let n = rows.len();
    let mut uid = Vec::with_capacity(n * 14);
    let mut b = Vec::with_capacity(16);
    for r in rows {
        b.clear();
        decode(r.person, &mut b);
        push_str(&mut uid, &b);
    }
    let defs: Vec<bool> = rows.iter().map(|r| r.top != u16::MAX).collect();
    let mut top = def_levels(&defs);
    for r in rows {
        if r.top != u16::MAX {
            push_str(&mut top, &cat_names[r.top as usize]);
        }
    }
    let cols = [
        Column { name: "user_id", kind: Kind::Str, data: uid, nvals: n },
        date_col("day", rows.iter().map(|r| r.day), n),
        i64_col("sessions", rows.iter().map(|r| r.sessions as i64), n),
        i64_col("events", rows.iter().map(|r| r.events as i64), n),
        i64_col("revenue_cents", rows.iter().map(|r| r.revenue), n),
        Column { name: "top_category", kind: Kind::OptStr, data: top, nvals: n },
    ];
    write_file(path, &cols, n);
}

pub fn write_campaigns(path: &std::path::Path, rows: &[CampRow], camp_ids: &[Vec<u8>], channels: &[Vec<u8>]) {
    let n = rows.len();
    let mut id = Vec::new();
    let mut ch = Vec::new();
    for r in rows {
        if r.camp >= camp_ids.len() {
            push_str(&mut id, b"organic");
            push_str(&mut ch, b"organic");
        } else {
            push_str(&mut id, &camp_ids[r.camp]);
            push_str(&mut ch, &channels[r.camp]);
        }
    }
    let cols = [
        Column { name: "campaign_id", kind: Kind::Str, data: id, nvals: n },
        Column { name: "channel", kind: Kind::Str, data: ch, nvals: n },
        date_col("day", rows.iter().map(|r| r.day), n),
        i64_col("sessions", rows.iter().map(|r| r.sessions), n),
        i64_col("purchasing_sessions", rows.iter().map(|r| r.purchasing), n),
        i64_col("revenue_cents", rows.iter().map(|r| r.revenue), n),
        i64_col("assisted_sessions", rows.iter().map(|r| r.assisted), n),
    ];
    write_file(path, &cols, n);
}
