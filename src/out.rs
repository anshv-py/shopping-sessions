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

/// One column chunk: an optional dictionary page (PLAIN values, count) and the data page
/// body as pieces written back to back (no concatenation copy).
struct Column<'a> {
    name: &'static str,
    kind: Kind,
    dict: Option<(Vec<u8>, usize)>,
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
const ENC_PLAIN: i32 = 0;
const ENC_PLAIN_DICT: i32 = 2;
const ENC_RLE: i32 = 3;

fn page_header(body_len: usize, nvals: usize, enc: i32) -> Vec<u8> {
    let mut t = T::new();
    t.i32(1, 0); // DATA_PAGE
    t.i32(2, body_len as i32);
    t.i32(3, body_len as i32);
    t.begin(5);
    t.i32(1, nvals as i32);
    t.i32(2, enc);
    t.i32(3, ENC_RLE); // definition levels
    t.i32(4, ENC_RLE); // repetition levels
    t.end();
    t.b.push(0);
    t.b
}

fn dict_page_header(body_len: usize, ndict: usize) -> Vec<u8> {
    let mut t = T::new();
    t.i32(1, 2); // DICTIONARY_PAGE
    t.i32(2, body_len as i32);
    t.i32(3, body_len as i32);
    t.begin(7);
    t.i32(1, ndict as i32);
    t.i32(2, ENC_PLAIN_DICT);
    t.end();
    t.b.push(0);
    t.b
}

fn varint(out: &mut Vec<u8>, mut h: u64) {
    while h >= 0x80 {
        out.push((h as u8) | 0x80);
        h >>= 7;
    }
    out.push(h as u8);
}

/// Prefix of definition levels (bit width 1) stored as one bit-packed run whose
/// `packed_len` bytes follow: the 4-byte length, then the run header varint.
fn def_header(nvals: usize, packed_len: usize) -> Vec<u8> {
    let groups = nvals.div_ceil(8);
    assert_eq!(groups, packed_len);
    let mut run = Vec::with_capacity(10);
    varint(&mut run, ((groups as u64) << 1) | 1);
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&((run.len() + packed_len) as u32).to_le_bytes());
    out.extend_from_slice(&run);
    out
}

/// Bit width for indices into a dictionary of `ndict` entries (at least 1).
fn bit_width(ndict: usize) -> u32 {
    (usize::BITS - ndict.saturating_sub(1).leading_zeros()).max(1)
}

/// Dictionary indices as a data page body: bit width byte, then one bit-packed run of the
/// RLE / bit-packing hybrid (LSB first, padded to a multiple of 8 values).
fn index_body(idx: &[u32], ndict: usize, out: &mut Vec<u8>) {
    let bw = bit_width(ndict);
    out.push(bw as u8);
    let groups = idx.len().div_ceil(8);
    varint(out, ((groups as u64) << 1) | 1);
    out.reserve(groups * bw as usize);
    let (mut acc, mut nb) = (0u64, 0u32);
    for g in 0..groups * 8 {
        let v = idx.get(g).copied().unwrap_or(0) as u64;
        acc |= v << nb;
        nb += bw;
        while nb >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            nb -= 8;
        }
    }
    debug_assert_eq!(nb, 0);
}

/// Value -> dictionary index for u64 values (open addressing).
struct Dict64 {
    keys: Vec<u64>,
    slots: Vec<u32>, // index + 1, 0 = empty
    bits: u32,
}

impl Dict64 {
    fn new(n: usize) -> Self {
        let cap = (2 * n).next_power_of_two().max(16);
        Dict64 { keys: Vec::new(), slots: vec![0; cap], bits: cap.trailing_zeros() }
    }
    #[inline(always)]
    fn index(&mut self, v: u64) -> u32 {
        let mask = self.slots.len() - 1;
        let mut h = (v.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - self.bits)) as usize;
        loop {
            let s = self.slots[h];
            if s == 0 {
                self.keys.push(v);
                self.slots[h] = self.keys.len() as u32;
                return self.keys.len() as u32 - 1;
            }
            if self.keys[s as usize - 1] == v {
                return s - 1;
            }
            h = (h + 1) & mask;
        }
    }
}

fn write_file(path: &std::path::Path, cols: &[Column], nrows: usize) {
    let mut file = std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(path).expect("create parquet"));
    file.write_all(b"PAR1").expect("write parquet");
    let mut pos = 4usize;
    let mut chunks = Vec::new(); // (chunk start, dictionary page offset, data page offset, size)
    for c in cols {
        let off = pos;
        let mut dict_off = None;
        if let Some((body, ndict)) = &c.dict {
            let h = dict_page_header(body.len(), *ndict);
            file.write_all(&h).expect("write parquet");
            file.write_all(body).expect("write parquet");
            dict_off = Some(pos);
            pos += h.len() + body.len();
        }
        let data_off = pos;
        let body = c.len();
        let h = page_header(body, c.nvals, if c.dict.is_some() { ENC_PLAIN_DICT } else { ENC_PLAIN });
        file.write_all(&h).expect("write parquet");
        for p in &c.parts {
            file.write_all(p).expect("write parquet");
        }
        pos += h.len() + body;
        chunks.push((off, dict_off, data_off, pos - off));
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
    for (c, &(off, dict_off, data_off, size)) in cols.iter().zip(&chunks) {
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
        t.zz(if dict_off.is_some() { ENC_PLAIN_DICT } else { ENC_PLAIN } as i64);
        t.zz(ENC_RLE as i64);
        t.list(3, BIN, 1);
        t.varint(c.name.len() as u64);
        t.b.extend_from_slice(c.name.as_bytes());
        t.i32(4, 0); // UNCOMPRESSED
        t.i64(5, nrows as i64);
        t.i64(6, size as i64);
        t.i64(7, size as i64);
        t.i64(9, data_off as i64);
        if let Some(d) = dict_off {
            t.i64(11, d as i64);
        }
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
/// Each chain holds values of one width dividing BLOCK_BYTES, so none straddles blocks.
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
    fn put<const W: usize>(&mut self, b: [u8; W], pool: &mut Pool) {
        if self.len == BLOCK_BYTES {
            if !self.cur.is_null() {
                self.done.push(self.cur);
            }
            self.cur = pool.get();
            self.len = 0;
        }
        unsafe { std::ptr::copy_nonoverlapping(b.as_ptr(), self.cur.add(self.len), W) };
        self.len += W;
    }
    fn parts(&self) -> Vec<&[u8]> {
        let mut v: Vec<&[u8]> = self.done.iter().map(|&p| unsafe { std::slice::from_raw_parts(p, BLOCK_BYTES) }).collect();
        if !self.cur.is_null() {
            v.push(unsafe { std::slice::from_raw_parts(self.cur, self.len) });
        }
        v
    }
    fn each<const W: usize>(&self, mut f: impl FnMut([u8; W])) {
        for p in self.parts() {
            for c in p.chunks_exact(W) {
                f(c.try_into().unwrap());
            }
        }
    }
}

unsafe impl Send for Chain {}

const NO_TOP: u8 = u8::MAX;

/// user_daily rows as raw columns in pool blocks; encoded when the file is written.
pub struct UCols {
    pub n: usize,
    person: Chain,
    day: Chain,
    sessions: Chain,
    events: Chain,
    revenue: Chain,
    top: Chain, // category id, NO_TOP = null
}

impl UCols {
    pub fn new() -> Self {
        UCols {
            n: 0,
            person: Chain::new(),
            day: Chain::new(),
            sessions: Chain::new(),
            events: Chain::new(),
            revenue: Chain::new(),
            top: Chain::new(),
        }
    }
    /// `top`: category id (< 255) or None.
    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub fn push(&mut self, pool: &mut Pool, person: u64, day: i32, sessions: u32, events: u32, revenue: i64, top: Option<u8>) {
        self.person.put(person.to_le_bytes(), pool);
        self.day.put(day.to_le_bytes(), pool);
        self.sessions.put(sessions.to_le_bytes(), pool);
        self.events.put(events.to_le_bytes(), pool);
        self.revenue.put(revenue.to_le_bytes(), pool);
        self.top.put([top.unwrap_or(NO_TOP)], pool);
        self.n += 1;
    }
}

impl Default for UCols {
    fn default() -> Self {
        Self::new()
    }
}

/// Dictionary-encodes an integer column. `vals` yields each row's value as u64; `w` is the
/// PLAIN width of a dictionary entry (4 for INT32, 8 for INT64).
fn int_dict_column(name: &'static str, kind: Kind, n: usize, w: usize, vals: impl Fn(&mut dyn FnMut(u64)), idx: &mut Vec<u32>) -> (Column<'static>, Vec<u8>) {
    let mut d = Dict64::new(n);
    idx.clear();
    vals(&mut |v| idx.push(d.index(v)));
    let mut dict = Vec::with_capacity(d.keys.len() * w);
    for &k in &d.keys {
        dict.extend_from_slice(&k.to_le_bytes()[..w]);
    }
    let mut body = Vec::new();
    index_body(idx, d.keys.len(), &mut body);
    (Column { name, kind, dict: Some((dict, d.keys.len())), parts: Vec::new(), nvals: n }, body)
}

#[inline(never)]
pub fn write_users(path: &std::path::Path, c: &UCols, cat_names: &[Vec<u8>]) {
    let n = c.n;
    if n == 0 {
        let dh = def_header(0, 0);
        let e: &[u8] = &[];
        let plain = |name, kind| Column { name, kind, dict: None, parts: vec![e], nvals: 0 };
        let cols = [
            plain("user_id", Kind::Str),
            plain("day", Kind::Date),
            plain("sessions", Kind::I64),
            plain("events", Kind::I64),
            plain("revenue_cents", Kind::I64),
            Column { name: "top_category", kind: Kind::OptStr, dict: None, parts: vec![&dh], nvals: 0 },
        ];
        write_file(path, &cols, 0);
        return;
    }
    let mut idx: Vec<u32> = Vec::with_capacity(n);
    // user_id: a person's rows are consecutive, so the dictionary is the run of persons
    let (uid_col, uid_body) = {
        let mut dict = Vec::new();
        let mut nd = 0usize;
        let mut last = None;
        let mut tmp = Vec::with_capacity(64);
        c.person.each::<8>(|b| {
            let p = u64::from_le_bytes(b);
            if last != Some(p) {
                last = Some(p);
                tmp.clear();
                decode(p, &mut tmp);
                push_str(&mut dict, &tmp);
                nd += 1;
            }
            idx.push(nd as u32 - 1);
        });
        let mut body = Vec::new();
        index_body(&idx, nd, &mut body);
        (Column { name: "user_id", kind: Kind::Str, dict: Some((dict, nd)), parts: Vec::new(), nvals: n }, body)
    };
    let (day_col, day_body) = int_dict_column("day", Kind::Date, n, 4, |f| c.day.each::<4>(|b| f(u32::from_le_bytes(b) as u64)), &mut idx);
    let (ses_col, ses_body) = int_dict_column("sessions", Kind::I64, n, 8, |f| c.sessions.each::<4>(|b| f(u32::from_le_bytes(b) as u64)), &mut idx);
    let (ev_col, ev_body) = int_dict_column("events", Kind::I64, n, 8, |f| c.events.each::<4>(|b| f(u32::from_le_bytes(b) as u64)), &mut idx);
    let (rev_col, rev_body) = int_dict_column("revenue_cents", Kind::I64, n, 8, |f| c.revenue.each::<8>(|b| f(u64::from_le_bytes(b))), &mut idx);
    // top_category: definition levels, then indices of the non-null rows
    let (top_col, top_body) = {
        let mut defs = vec![0u8; n.div_ceil(8)];
        let mut map = [u32::MAX; 256];
        let mut dict = Vec::new();
        let mut nd = 0usize;
        idx.clear();
        let mut r = 0usize;
        c.top.each::<1>(|b| {
            let t = b[0];
            if t != NO_TOP {
                defs[r >> 3] |= 1 << (r & 7);
                if map[t as usize] == u32::MAX {
                    map[t as usize] = nd as u32;
                    push_str(&mut dict, &cat_names[t as usize]);
                    nd += 1;
                }
                idx.push(map[t as usize]);
            }
            r += 1;
        });
        let mut body = def_header(n, defs.len());
        body.extend_from_slice(&defs);
        if nd == 0 {
            // all null: no values, so a plain page with only definition levels
            (Column { name: "top_category", kind: Kind::OptStr, dict: None, parts: Vec::new(), nvals: n }, body)
        } else {
            index_body(&idx, nd, &mut body);
            (Column { name: "top_category", kind: Kind::OptStr, dict: Some((dict, nd)), parts: Vec::new(), nvals: n }, body)
        }
    };
    let cols = [
        with(uid_col, &uid_body),
        with(day_col, &day_body),
        with(ses_col, &ses_body),
        with(ev_col, &ev_body),
        with(rev_col, &rev_body),
        with(top_col, &top_body),
    ];
    write_file(path, &cols, n);
}

fn with<'a>(col: Column<'static>, body: &'a [u8]) -> Column<'a> {
    Column { name: col.name, kind: col.kind, dict: col.dict, parts: vec![body], nvals: col.nvals }
}

fn plain_col<'a>(name: &'static str, kind: Kind, data: &'a [u8], n: usize) -> Column<'a> {
    Column { name, kind, dict: None, parts: vec![data], nvals: n }
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
    let col = |name, kind, data| plain_col(name, kind, data, n);
    let cols = [
        col("campaign_id", Kind::Str, &id),
        col("channel", Kind::Str, &ch),
        col("day", Kind::Date, &day),
        col("sessions", Kind::I64, &se),
        col("purchasing_sessions", Kind::I64, &pu),
        col("revenue_cents", Kind::I64, &re),
        col("assisted_sessions", Kind::I64, &asst),
    ];
    write_file(path, &cols, n);
}
