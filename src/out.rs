//! Minimal hand-written Parquet writer: one row group, one PLAIN data page (v1) per column,
//! no compression, no statistics. Metadata is Thrift compact protocol.

use crate::parse::decode;
use crate::{Pool, BLOCK_BYTES};
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

/// A column chunk's page body, as pieces written back to back (no concatenation copy).
struct Column<'a> {
    name: &'static str,
    kind: Kind,
    parts: Vec<&'a [u8]>,
    nvals: usize,
}

impl Column<'_> {
    fn len(&self) -> usize {
        self.parts.iter().map(|p| p.len()).sum()
    }
}

const PT_INT32: i32 = 1;
const PT_INT64: i32 = 2;
const PT_BYTE_ARRAY: i32 = 6;

fn page_header(body_len: usize, nvals: usize) -> Vec<u8> {
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
    t.b.push(0);
    t.b
}

/// Prefix of definition levels (bit width 1) stored as one bit-packed run whose
/// `packed_len` bytes follow: the 4-byte length, then the run header varint.
fn def_header(nvals: usize, packed_len: usize) -> Vec<u8> {
    let groups = nvals.div_ceil(8);
    assert_eq!(groups, packed_len);
    let mut run = Vec::with_capacity(10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        run.push((h as u8) | 0x80);
        h >>= 7;
    }
    run.push(h as u8);
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&((run.len() + packed_len) as u32).to_le_bytes());
    out.extend_from_slice(&run);
    out
}

fn write_file(path: &std::path::Path, cols: &[Column], nrows: usize) {
    let mut file = std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(path).expect("create parquet"));
    file.write_all(b"PAR1").expect("write parquet");
    let mut pos = 4usize;
    let mut chunks = Vec::new(); // (offset, total size)
    for c in cols {
        let off = pos;
        let body = c.len();
        let h = page_header(body, c.nvals);
        file.write_all(&h).expect("write parquet");
        for p in &c.parts {
            file.write_all(p).expect("write parquet");
        }
        pos += h.len() + body;
        chunks.push((off, pos - off));
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
    let mut tail = t.b;
    let ml = tail.len() as u32;
    tail.extend_from_slice(&ml.to_le_bytes());
    tail.extend_from_slice(b"PAR1");
    file.write_all(&tail).expect("write parquet");
    file.flush().expect("write parquet");
}

fn push_str(v: &mut Vec<u8>, s: &[u8]) {
    v.extend_from_slice(&(s.len() as u32).to_le_bytes());
    v.extend_from_slice(s);
}

/// Bytes appended into a chain of pool blocks (recycled memory, no grow-and-copy).
pub struct Chain {
    done: Vec<*const u8>, // full blocks
    cur: *mut u8,
    len: usize, // bytes used in cur; BLOCK_BYTES when there is no current block
}

impl Chain {
    fn new() -> Self {
        Chain { done: Vec::new(), cur: std::ptr::null_mut(), len: BLOCK_BYTES }
    }
    #[inline(always)]
    fn put(&mut self, b: &[u8], pool: &mut Pool) {
        if self.len + b.len() <= BLOCK_BYTES {
            unsafe { std::ptr::copy_nonoverlapping(b.as_ptr(), self.cur.add(self.len), b.len()) };
            self.len += b.len();
        } else {
            self.put_slow(b, pool);
        }
    }
    #[cold]
    #[inline(never)]
    fn put_slow(&mut self, mut b: &[u8], pool: &mut Pool) {
        while !b.is_empty() {
            if self.len == BLOCK_BYTES {
                if !self.cur.is_null() {
                    self.done.push(self.cur);
                }
                self.cur = pool.get();
                self.len = 0;
            }
            let k = (BLOCK_BYTES - self.len).min(b.len());
            unsafe { std::ptr::copy_nonoverlapping(b.as_ptr(), self.cur.add(self.len), k) };
            self.len += k;
            b = &b[k..];
        }
    }
    fn parts(&self) -> Vec<&[u8]> {
        let mut v: Vec<&[u8]> = self.done.iter().map(|&p| unsafe { std::slice::from_raw_parts(p, BLOCK_BYTES) }).collect();
        if !self.cur.is_null() {
            v.push(unsafe { std::slice::from_raw_parts(self.cur, self.len) });
        }
        v
    }
}

unsafe impl Send for Chain {}

/// user_daily columns, appended row by row in their final PLAIN encodings.
pub struct UCols {
    pub n: usize,
    uid: Chain,
    day: Chain,
    sessions: Chain,
    events: Chain,
    revenue: Chain,
    defs: Chain, // bit-packed definition levels of top_category, full bytes
    pend: u8,    // definition bits of the last n % 8 rows
    top: Chain,
    tmp: Vec<u8>,
}

impl UCols {
    pub fn new() -> Self {
        UCols {
            n: 0,
            uid: Chain::new(),
            day: Chain::new(),
            sessions: Chain::new(),
            events: Chain::new(),
            revenue: Chain::new(),
            defs: Chain::new(),
            pend: 0,
            top: Chain::new(),
            tmp: Vec::with_capacity(64),
        }
    }
    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub fn push(&mut self, pool: &mut Pool, person: u64, day: i32, sessions: u32, events: u32, revenue: i64, top: Option<&[u8]>) {
        self.tmp.clear();
        decode(person, &mut self.tmp);
        self.uid.put(&(self.tmp.len() as u32).to_le_bytes(), pool);
        self.uid.put(&self.tmp, pool);
        self.day.put(&day.to_le_bytes(), pool);
        self.sessions.put(&(sessions as i64).to_le_bytes(), pool);
        self.events.put(&(events as i64).to_le_bytes(), pool);
        self.revenue.put(&revenue.to_le_bytes(), pool);
        if let Some(t) = top {
            self.pend |= 1 << (self.n % 8);
            self.top.put(&(t.len() as u32).to_le_bytes(), pool);
            self.top.put(t, pool);
        }
        self.n += 1;
        if self.n % 8 == 0 {
            self.defs.put(&[self.pend], pool);
            self.pend = 0;
        }
    }
}

impl Default for UCols {
    fn default() -> Self {
        Self::new()
    }
}

#[inline(never)]
pub fn write_users(path: &std::path::Path, c: &UCols) {
    let n = c.n;
    let mut defs = c.defs.parts();
    if n % 8 != 0 {
        defs.push(std::slice::from_ref(&c.pend));
    }
    let dh = def_header(n, defs.iter().map(|p| p.len()).sum());
    let mut top: Vec<&[u8]> = vec![&dh];
    top.extend(defs);
    top.extend(c.top.parts());
    let cols = [
        Column { name: "user_id", kind: Kind::Str, parts: c.uid.parts(), nvals: n },
        Column { name: "day", kind: Kind::Date, parts: c.day.parts(), nvals: n },
        Column { name: "sessions", kind: Kind::I64, parts: c.sessions.parts(), nvals: n },
        Column { name: "events", kind: Kind::I64, parts: c.events.parts(), nvals: n },
        Column { name: "revenue_cents", kind: Kind::I64, parts: c.revenue.parts(), nvals: n },
        Column { name: "top_category", kind: Kind::OptStr, parts: top, nvals: n },
    ];
    write_file(path, &cols, n);
}

fn le_col<const W: usize>(it: impl Iterator<Item = [u8; W]>) -> Vec<u8> {
    let mut data = Vec::new();
    for v in it {
        data.extend_from_slice(&v);
    }
    data
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
    let day = le_col(rows.iter().map(|r| r.day.to_le_bytes()));
    let se = le_col(rows.iter().map(|r| r.sessions.to_le_bytes()));
    let pu = le_col(rows.iter().map(|r| r.purchasing.to_le_bytes()));
    let re = le_col(rows.iter().map(|r| r.revenue.to_le_bytes()));
    let asst = le_col(rows.iter().map(|r| r.assisted.to_le_bytes()));
    let cols = [
        Column { name: "campaign_id", kind: Kind::Str, parts: vec![&id], nvals: n },
        Column { name: "channel", kind: Kind::Str, parts: vec![&ch], nvals: n },
        Column { name: "day", kind: Kind::Date, parts: vec![&day], nvals: n },
        Column { name: "sessions", kind: Kind::I64, parts: vec![&se], nvals: n },
        Column { name: "purchasing_sessions", kind: Kind::I64, parts: vec![&pu], nvals: n },
        Column { name: "revenue_cents", kind: Kind::I64, parts: vec![&re], nvals: n },
        Column { name: "assisted_sessions", kind: Kind::I64, parts: vec![&asst], nvals: n },
    ];
    write_file(path, &cols, n);
}
