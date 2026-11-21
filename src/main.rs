//! Shopping Sessions: a single-binary, multi-threaded implementation of rules.md.
//!
//! Pipeline:
//!   stage 1 (parallel chunks): parse links, events, clicks straight from mmapped CSV.
//!       links  -> pairs, union-find built by whichever thread finishes the last link chunk
//!       events -> compact records, radix-partitioned by event_id
//!       clicks -> validated, person-resolved, radix-partitioned by person
//!   stage 2 (parallel eid partitions): dedup on event_id (min ingest_ts), keep valid,
//!       resolve person, radix-partition by person
//!   stage 3 (parallel person partitions): group by person, sort, sessionize, attribute,
//!       aggregate; each thread writes its own user_daily parquet file.
//!
//! IDs are packed into order-preserving u64 keys (10 chars x 6 bits) when they fit the
//! alphabet [0-9A-Z_a-z]; anything else gets a hashed fallback key with the top bit set and
//! its bytes registered so ordering and names stay exact.

#[cfg(target_os = "linux")]
mod bump;
mod diag;
mod out;
mod parse;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: bump::Bump = bump::Bump;

use parse::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const GAP: i64 = 1_800_000;
const P1_BITS: u32 = 10;
const NP1: usize = 1 << P1_BITS;
const P2_BITS: u32 = 10;
const NP2: usize = 1 << P2_BITS;

/// Parsed event, partitioned by event_id. 28 bytes.
#[derive(Clone, Copy)]
#[repr(C, packed)]
struct ERec {
    eid: u64,
    uid: u64,
    ingest: u32, // seconds since epoch (saturated)
    ts: u32,     // relative to base; u32::MAX = invalid / late
    meta: u32,   // price (24 bits) | category << 24, 0 for non-purchase
}

/// Deduplicated surviving event, partitioned by user_id.
#[derive(Clone, Copy)]
#[repr(C)]
struct URec {
    uid: u64,
    tm: u64, // ts << 32 | meta
}

/// Valid click, partitioned by user_id.
#[derive(Clone, Copy)]
#[repr(C)]
struct C1 {
    uid: u64,
    ckey: u64,
    ts: u32,
    camp: u32,
}

/// Event of a person, partitioned by person.
#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    person: u64,
    tm: u64,
}

/// Click of a person, partitioned by person.
#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    person: u64,
    ckey: u64,
    ts: u32,
    camp: u32,
}

const P3_BITS: u32 = 10;
const NP3: usize = 1 << P3_BITS;

#[inline(always)]
fn part3(k: u64) -> usize {
    (k.wrapping_mul(0xBF58_476D_1CE4_E5B9) >> (64 - P3_BITS)) as usize
}

#[inline(always)]
fn part1(k: u64) -> usize {
    (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - P1_BITS)) as usize
}
#[inline(always)]
fn part2(k: u64) -> usize {
    (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> (64 - P2_BITS)) as usize
}
#[inline(always)]
fn thash(k: u64, bits: u32) -> usize {
    (k.wrapping_mul(0xA076_1D64_78BD_642F).rotate_left(23).wrapping_mul(0xE703_7ED1_A0B4_28DB) >> (64 - bits)) as usize
}

pub struct Lookup {
    prefix: &'static [u8],
    by_num: Vec<u16>,
    map: HashMap<Vec<u8>, u16>,
}

impl Lookup {
    fn new(prefix: &'static [u8], entries: &[(Vec<u8>, u16)]) -> Self {
        let mut by_num: Vec<u16> = Vec::new();
        let mut map = HashMap::new();
        for (k, v) in entries {
            if let Some(n) = fast_num(prefix, k) {
                if by_num.len() <= n {
                    by_num.resize(n + 1, u16::MAX);
                }
                by_num[n] = *v;
            } else {
                map.insert(k.clone(), *v);
            }
        }
        Lookup { prefix, by_num, map }
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> Option<u16> {
        if let Some(n) = fast_num(self.prefix, s) {
            return match self.by_num.get(n) {
                Some(&v) if v != u16::MAX => Some(v),
                _ => None,
            };
        }
        if self.map.is_empty() {
            return None;
        }
        self.map.get(s).copied()
    }
}

/// `prefix` then 1..=6 canonical decimal digits (no leading zero unless just "0").
#[inline(always)]
fn fast_num(prefix: &[u8], s: &[u8]) -> Option<usize> {
    let p = prefix.len();
    if s.len() <= p || s.len() > p + 6 || &s[..p] != prefix {
        return None;
    }
    let d = &s[p..];
    if d.len() > 1 && d[0] == b'0' {
        return None;
    }
    let mut n = 0usize;
    for &b in d {
        let x = b.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        n = n * 10 + x as usize;
    }
    Some(n)
}

const BLOCK_BYTES: usize = 8192;

/// Per-thread pool of free 8 KiB blocks. Blocks of consumed partitions are recycled into
/// it, so later stages mostly write into memory that is already faulted in.
pub struct Pool {
    free: Vec<*mut u8>,
}

impl Pool {
    fn new() -> Self {
        Pool { free: Vec::with_capacity(1 << 16) }
    }
    #[inline(always)]
    fn get(&mut self) -> *mut u8 {
        match self.free.pop() {
            Some(b) => b,
            None => unsafe { std::alloc::alloc(std::alloc::Layout::from_size_align_unchecked(BLOCK_BYTES, 64)) },
        }
    }
}

/// Per-thread radix scatter: each partition is a chain of fixed-size blocks.
pub struct Scatter<T: Copy> {
    cur: Vec<(*mut T, u32)>,
    blocks: Vec<Vec<*mut T>>,
}

unsafe impl<T: Copy> Send for Scatter<T> {}
unsafe impl<T: Copy> Sync for Scatter<T> {}

impl<T: Copy> Scatter<T> {
    const CAP: usize = BLOCK_BYTES / std::mem::size_of::<T>();
    fn new(np: usize) -> Self {
        Scatter { cur: vec![(std::ptr::null_mut(), 0); np], blocks: (0..np).map(|_| Vec::new()).collect() }
    }
    #[inline(always)]
    fn push(&mut self, p: usize, v: T, pool: &mut Pool) {
        let c = unsafe { self.cur.get_unchecked_mut(p) };
        if c.1 as usize == Self::CAP || c.0.is_null() {
            if !c.0.is_null() {
                unsafe { self.blocks.get_unchecked_mut(p) }.push(c.0);
            }
            *c = (pool.get() as *mut T, 0);
        }
        unsafe { c.0.add(c.1 as usize).write_unaligned(v) };
        c.1 += 1;
    }
    #[inline(always)]
    fn each(&self, p: usize, mut f: impl FnMut(&[T])) {
        for &b in &self.blocks[p] {
            f(unsafe { std::slice::from_raw_parts(b, Self::CAP) });
        }
        let c = self.cur[p];
        if !c.0.is_null() {
            f(unsafe { std::slice::from_raw_parts(c.0, c.1 as usize) });
        }
    }
    /// Hands partition p's blocks to `pool`. Partition p must never be read again.
    fn recycle(&self, p: usize, pool: &mut Pool) {
        for &b in &self.blocks[p] {
            pool.free.push(b as *mut u8);
        }
        let c = self.cur[p];
        if !c.0.is_null() {
            pool.free.push(c.0 as *mut u8);
        }
    }
    fn part_len(&self, p: usize) -> usize {
        self.blocks[p].len() * Self::CAP + if self.cur[p].0.is_null() { 0 } else { self.cur[p].1 as usize }
    }
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(p as *const i8);
    }
}

/// (key, person) for every linked ID, grouped by part3(key).
pub struct LinkParts {
    off: Vec<u32>,
    ent: Vec<(u64, u64)>,
}

/// Small per-partition lookup table, rebuilt for each uid partition (fits in L1/L2).
struct LocalMap {
    tab: Vec<(u64, u64)>,
    mask: usize,
    bits: u32,
}

impl LocalMap {
    fn new() -> Self {
        LocalMap { tab: Vec::new(), mask: 0, bits: 0 }
    }
    fn load(&mut self, ent: &[(u64, u64)]) {
        let cap = (2 * ent.len()).next_power_of_two().max(16);
        self.bits = cap.trailing_zeros();
        self.mask = cap - 1;
        self.tab.clear();
        self.tab.resize(cap, (0, 0));
        for &(k, v) in ent {
            let mut h = thash(k, self.bits);
            while self.tab[h].0 != 0 {
                h = (h + 1) & self.mask;
            }
            self.tab[h] = (k, v);
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        let mut h = thash(k, self.bits);
        loop {
            let e = unsafe { *self.tab.get_unchecked(h) };
            if e.0 == k {
                return e.1;
            }
            if e.0 == 0 {
                return k;
            }
            h = (h + 1) & self.mask;
        }
    }
}

#[inline(never)]
fn build_links(pairs: &[Vec<(u64, u64)>]) -> LinkParts {
    let m: usize = pairs.iter().map(|v| v.len()).sum();
    if m == 0 {
        return LinkParts { off: vec![0; NP3 + 1], ent: Vec::new() };
    }
    // At most 2m distinct keys; load factor <= 0.5. Slots hold (key, dense index) during
    // the build and (key, person) afterwards, so the build table is the lookup table.
    let tb = Instant::now();
    let cap = (4 * m).next_power_of_two().max(16);
    let bits = cap.trailing_zeros();
    let mask = cap - 1;
    let mut tab: Vec<(u64, u64)> = vec![(0, 0); cap];
    let mut keys: Vec<u64> = Vec::with_capacity(2 * m);
    let mut parent: Vec<u32> = Vec::with_capacity(2 * m);
    #[inline(always)]
    fn ins(tab: &mut [(u64, u64)], mask: usize, h0: usize, k: u64, keys: &mut Vec<u64>, parent: &mut Vec<u32>) -> u32 {
        let mut h = h0;
        loop {
            let e = unsafe { *tab.get_unchecked(h) };
            if e.0 == k {
                return e.1 as u32;
            }
            if e.0 == 0 {
                let idx = keys.len() as u32;
                unsafe { *tab.get_unchecked_mut(h) = (k, idx as u64) };
                keys.push(k);
                parent.push(idx);
                return idx;
            }
            h = (h + 1) & mask;
        }
    }
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let g = parent[parent[x as usize] as usize];
            parent[x as usize] = g;
            x = g;
        }
        x
    }
    const D: usize = 8;
    for v in pairs {
        let n = v.len();
        for i in 0..n.min(D) {
            prefetch(unsafe { tab.as_ptr().add(thash(v[i].0, bits)) });
            prefetch(unsafe { tab.as_ptr().add(thash(v[i].1, bits)) });
        }
        for i in 0..n {
            if i + D < n {
                prefetch(unsafe { tab.as_ptr().add(thash(v[i + D].0, bits)) });
                prefetch(unsafe { tab.as_ptr().add(thash(v[i + D].1, bits)) });
            }
            let (a, b) = v[i];
            let ia = ins(&mut tab, mask, thash(a, bits), a, &mut keys, &mut parent);
            let ib = ins(&mut tab, mask, thash(b, bits), b, &mut keys, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    let n = keys.len();
    let t_union = tb.elapsed().as_secs_f64();
    let mut minkey: Vec<u64> = vec![0; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        parent[i] = r;
        let cur = minkey[r as usize];
        if cur == 0 || key_lt(keys[i], cur) {
            minkey[r as usize] = keys[i];
        }
    }
    // Counting sort of (key, person) by part3(key).
    let mut off = vec![0u32; NP3 + 1];
    for &k in &keys {
        off[part3(k) + 1] += 1;
    }
    for i in 0..NP3 {
        off[i + 1] += off[i];
    }
    let mut fill = off.clone();
    let mut ent = vec![(0u64, 0u64); n];
    for i in 0..n {
        let k = keys[i];
        let q = part3(k);
        ent[fill[q] as usize] = (k, minkey[parent[i] as usize]);
        fill[q] += 1;
    }
    drop(tab);
    eprintln!("  links: {m} pairs, {n} keys, union {t_union:.3}s total {:.3}s", tb.elapsed().as_secs_f64());
    LinkParts { off, ent }
}

pub struct Ctx {
    base: i64, // window_start - 7 days; all relative times use this
    ws: i64,
    we: i64,
    products: Lookup,
    campaigns: Lookup,
    unknown_cat: u8,
}

struct Chunk {
    kind: u8, // 0 links, 1 events, 2 clicks
    file: usize,
    start: usize,
    end: usize,
}

struct S1Out {
    ev: Scatter<ERec>,
    cl: Scatter<C1>,
}

struct S2Out {
    uv: Scatter<URec>,
}

struct S2bOut {
    pv: Scatter<PRec>,
    cv: Scatter<CRec>,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn list_csv(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map_or(false, |x| x == "csv") && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn mmap(p: &std::path::Path) -> Option<memmap2::Mmap> {
    let f = std::fs::File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let m = unsafe { memmap2::MmapOptions::new().map(&f).ok()? };
    Some(m)
}

fn read_small_csv(path: &std::path::Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let buf = std::fs::read(path).unwrap_or_default();
    let mut out = Vec::new();
    let mut i = skip_line(&buf, 0);
    let mut f = [(0usize, 0usize); 2];
    while i < buf.len() {
        let (next, ok) = parse_line::<2>(&buf, i, &mut f);
        if ok {
            out.push((buf[f[0].0..f[0].1].to_vec(), buf[f[1].0..f[1].1].to_vec()));
        }
        i = next;
    }
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let at = text.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &text[at + key.len() + 2..];
    let q1 = rest.find('"').unwrap() + 1;
    let q2 = q1 + rest[q1..].find('"').unwrap();
    parse_ts(rest[q1..q2].as_bytes()).expect("manifest timestamp")
}

fn main() {
    let t0 = Instant::now();
    let data = std::path::PathBuf::from(env_or("DATA_DIR", "/data"));
    let outdir = std::path::PathBuf::from(env_or("OUTPUT_DIR", "/output"));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));
    if std::env::var("DIAG_CHILD").is_err() && diag::is_dev(&data) {
        diag::run(&data, &outdir);
    }
    let knob = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<usize>().ok());
    let chunk_size = knob("CHUNK_KB").map_or(256 << 10, |k| k << 10);
    let populate = knob("POPULATE").unwrap_or(0) == 1;
    let willneed = knob("WILLNEED").unwrap_or(0) == 1;

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    let base = ws - WEEK;

    // Products & categories.
    let prod_rows = read_small_csv(&data.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_idx: HashMap<Vec<u8>, u16> = HashMap::new();
    let mut prod_entries = Vec::with_capacity(prod_rows.len());
    for (p, c) in &prod_rows {
        let id = *cat_idx.entry(c.clone()).or_insert_with(|| {
            cat_names.push(c.clone());
            (cat_names.len() - 1) as u16
        });
        prod_entries.push((p.clone(), id));
    }
    let unknown_cat = *cat_idx.entry(b"unknown".to_vec()).or_insert_with(|| {
        cat_names.push(b"unknown".to_vec());
        (cat_names.len() - 1) as u16
    });
    assert!(cat_names.len() <= 255, "too many categories");
    let mut order: Vec<usize> = (0..cat_names.len()).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(&cat_names[b]));
    let mut cat_rank = vec![0u16; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c] = r as u16;
    }

    // Campaigns.
    let camp_rows = read_small_csv(&data.join("campaigns.csv"));
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut channels: Vec<Vec<u8>> = Vec::new();
    let mut camp_entries = Vec::new();
    {
        let mut seen: HashMap<Vec<u8>, u16> = HashMap::new();
        for (c, ch) in &camp_rows {
            if seen.contains_key(c) {
                continue;
            }
            let id = camp_ids.len() as u16;
            seen.insert(c.clone(), id);
            camp_ids.push(c.clone());
            channels.push(ch.clone());
            camp_entries.push((c.clone(), id));
        }
    }
    let ncamp = camp_ids.len();
    let ctx = Ctx {
        base,
        ws,
        we,
        products: Lookup::new(b"p", &prod_entries),
        campaigns: Lookup::new(b"c_", &camp_entries),
        unknown_cat: unknown_cat as u8,
    };

    // Files and chunks.
    let use_read = knob("READ").unwrap_or(0) == 1;
    let mut maps: [Vec<memmap2::Mmap>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    let mut files: [Vec<std::fs::File>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for (k, d) in ["links", "events", "clicks"].iter().enumerate() {
        for p in list_csv(&data.join(d)) {
            if let Some(m) = mmap(&p) {
                maps[k].push(m);
                files[k].push(std::fs::File::open(&p).expect("open"));
            }
        }
    }
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut ev_bytes = 0usize;
    let mut cl_bytes = 0usize;
    for kind in [0u8, 1, 2] {
        for (fi, m) in maps[kind as usize].iter().enumerate() {
            let len = m.len();
            if kind == 1 {
                ev_bytes += len;
            }
            if kind == 2 {
                cl_bytes += len;
            }
            let mut s = 0;
            while s < len {
                let e = (s + chunk_size).min(len);
                chunks.push(Chunk { kind, file: fi, start: s, end: e });
                s = e;
            }
        }
    }
    if let Ok(mode) = std::env::var("BENCH") {
        // Single-thread parse benchmark over the event chunks; min of REPS.
        let reps = knob("REPS").unwrap_or(7);
        let mut pool = Pool::new();
        let mut bm = Masks::default();
        let mut best = f64::MAX;
        for _ in 0..reps {
            let mut sc: Scatter<ERec> = Scatter::new(NP1);
            let t = Instant::now();
            let mut acc = 0usize;
            for c in chunks.iter().filter(|c| c.kind == 1) {
                let buf: &[u8] = &maps[1][c.file];
                if mode == "split" {
                    for_lines::<7>(buf, c.start, c.end, &mut bm, |f| acc += f[6].1);
                } else if mode == "keys" {
                    for_lines::<7>(buf, c.start, c.end, &mut bm, |f| {
                        acc += (pack_noreg(buf, f[0].0, f[0].1) ^ pack_reg(buf, f[1].0, f[1].1)) as usize
                    });
                } else if mode == "ts" {
                    for_lines::<7>(buf, c.start, c.end, &mut bm, |f| {
                        acc += ts_at(buf, f[2].0, f[2].1).unwrap_or(1) as usize
                            + f1_field(buf, f[3].0, f[3].1).unwrap_or(1) as usize
                    });
                } else if mode == "ing" {
                    for_lines::<7>(buf, c.start, c.end, &mut bm, |f| acc += f1_field(buf, f[3].0, f[3].1).unwrap_or(1) as usize);
                } else if mode == "tsonly" {
                    for_lines::<7>(buf, c.start, c.end, &mut bm, |f| acc += ts_at(buf, f[2].0, f[2].1).unwrap_or(1) as usize);
                } else if mode == "push" {
                    for_lines::<7>(buf, c.start, c.end, &mut bm, |f| {
                        let k = f[0].0 as u64 * 0x9E37_79B9_7F4A_7C15;
                        sc.push(part1(k), ERec { eid: k, uid: f[1].0 as u64, ingest: 0, ts: 0, meta: 0 }, &mut pool)
                    });
                } else {
                    events_chunk(buf, c.start, c.end, &ctx, &mut bm, &mut sc, &mut pool);
                }
            }
            best = best.min(t.elapsed().as_secs_f64());
            for p in 0..NP1 {
                sc.recycle(p, &mut pool);
            }
            std::hint::black_box(acc);
        }
        eprintln!("bench {mode}: best {:.4}s = {:.0} MB/s", best, ev_bytes as f64 / best / 1e6);
        std::process::exit(0);
    }
    let n_link_chunks = chunks.iter().filter(|c| c.kind == 0).count();
    // Big event chunks first so the tail is made of small ones.
    eprintln!(
        "[{:.3}] setup: {} chunks, events {} MB, clicks {} MB, threads {}",
        t0.elapsed().as_secs_f64(),
        chunks.len(),
        ev_bytes >> 20,
        cl_bytes >> 20,
        nthreads
    );

    #[cfg(target_os = "linux")]
    if willneed {
        // Kick off readahead of every input in the order the workers consume it.
        let regions: Vec<(usize, usize)> = chunks
            .iter()
            .map(|c| (maps[c.kind as usize][c.file].as_ptr() as usize + c.start, c.end - c.start))
            .collect();
        std::thread::spawn(move || {
            for (p, l) in regions {
                let a = p & !4095;
                unsafe { libc::madvise(a as *mut libc::c_void, l + (p - a), libc::MADV_WILLNEED) };
            }
        });
    }
    let _ = (populate, willneed);
    let link_ready_ms = std::sync::atomic::AtomicU64::new(0);
    let link_pairs: Mutex<Vec<Vec<(u64, u64)>>> = Mutex::new(Vec::new());
    let links_done = AtomicUsize::new(0);
    let link_parts: OnceLock<LinkParts> = OnceLock::new();
    if n_link_chunks == 0 {
        let _ = link_parts.set(LinkParts { off: vec![0; NP3 + 1], ent: Vec::new() });
    }
    let next = AtomicUsize::new(0);

    // ---------------- stage 1: parse ----------------
    let s1: Vec<S1Out> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                sc.spawn(|| {
                    let mut o = S1Out { ev: Scatter::new(NP1), cl: Scatter::new(NP3) };
                    let mut pool = Pool::new();
                    let mut masks = Masks::default();
                    let mut rbuf: Vec<u8> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(c) = chunks.get(i) else { break };
                        let whole: &[u8] = &maps[c.kind as usize][c.file];
                        let (buf, cs, ce): (&[u8], usize, usize) = if use_read {
                            let (a, b) = load_chunk(&files[c.kind as usize][c.file], whole.len(), c.start, c.end, &mut rbuf);
                            (&rbuf[..], a, b)
                        } else {
                            (whole, c.start, c.end)
                        };
                        let c = Chunk { kind: c.kind, file: c.file, start: cs, end: ce };
                        #[cfg(target_os = "linux")]
                        if populate {
                            let p = buf.as_ptr() as usize + c.start;
                            let a = p & !4095;
                            unsafe { libc::madvise(a as *mut libc::c_void, c.end - c.start + (p - a), 22) };
                        }
                        match c.kind {
                            0 => {
                                let v = links_chunk(buf, c.start, c.end, &mut masks);
                                link_pairs.lock().unwrap().push(v);
                                if links_done.fetch_add(1, Ordering::AcqRel) + 1 == n_link_chunks {
                                    let pairs = std::mem::take(&mut *link_pairs.lock().unwrap());
                                    let lp = build_links(&pairs);
                                    let _ = link_parts.set(lp);
                                    link_ready_ms.store(t0.elapsed().as_millis() as u64, Ordering::Relaxed);
                                }
                            }
                            1 => events_chunk(buf, c.start, c.end, &ctx, &mut masks, &mut o.ev, &mut pool),
                            _ => clicks_chunk(buf, c.start, c.end, &ctx, &mut masks, &mut o.cl, &mut pool),
                        }
                    }
                    o
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let link_parts = link_parts.into_inner().unwrap();
    eprintln!("[{:.3}] stage1 done (link map ready at {} ms)", t0.elapsed().as_secs_f64(), link_ready_ms.load(Ordering::Relaxed));

    // ---------------- stage 2: dedup, scatter by user_id ----------------
    let next = AtomicUsize::new(0);
    let s2: Vec<S2Out> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                sc.spawn(|| {
                    let mut o = S2Out { uv: Scatter::new(NP3) };
                    let mut pool = Pool::new();
                    let mut local: Vec<ERec> = Vec::new();
                    let mut table: Vec<u64> = Vec::new();
                    loop {
                        let p = next.fetch_add(1, Ordering::Relaxed);
                        if p >= NP1 {
                            break;
                        }
                        local.clear();
                        for t in &s1 {
                            t.ev.each(p, |sl| local.extend_from_slice(sl));
                            t.ev.recycle(p, &mut pool);
                        }
                        dedup_partition(&local, &mut table, &mut o.uv, &mut pool);
                    }
                    o
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    eprintln!("[{:.3}] stage2 done", t0.elapsed().as_secs_f64());

    // ---------------- stage 2b: resolve persons per user_id partition ----------------
    let next = AtomicUsize::new(0);
    let s2b: Vec<S2bOut> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                sc.spawn(|| {
                    let mut o = S2bOut { pv: Scatter::new(NP2), cv: Scatter::new(NP2) };
                    let mut pool = Pool::new();
                    let mut lm = LocalMap::new();
                    loop {
                        let p = next.fetch_add(1, Ordering::Relaxed);
                        if p >= NP3 {
                            break;
                        }
                        let ent = &link_parts.ent[link_parts.off[p] as usize..link_parts.off[p + 1] as usize];
                        lm.load(ent);
                        for t in &s2 {
                            t.uv.each(p, |sl| {
                                for r in sl {
                                    let person = lm.get(r.uid);
                                    o.pv.push(part2(person), PRec { person, tm: r.tm }, &mut pool);
                                }
                            });
                            t.uv.recycle(p, &mut pool);
                        }
                        for t in &s1 {
                            t.cl.each(p, |sl| {
                                for r in sl {
                                    let person = lm.get(r.uid);
                                    o.cv.push(part2(person), CRec { person, ckey: r.ckey, ts: r.ts, camp: r.camp }, &mut pool);
                                }
                            });
                            t.cl.recycle(p, &mut pool);
                        }
                    }
                    o
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    eprintln!("[{:.3}] stage2b done", t0.elapsed().as_secs_f64());

    // ---------------- stage 3: sessions ----------------
    let ud = outdir.join("user_daily");
    let cd = outdir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let day0 = base.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncs = ncamp + 1; // + organic
    let next = AtomicUsize::new(0);
    let wrote_any = AtomicUsize::new(0);
    let cat_names_ref = &cat_names;
    let cat_rank_ref = &cat_rank;
    let stats: Vec<Vec<[i64; 4]>> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|tid| {
                let s2b = &s2b;
                let next = &next;
                let ud = &ud;
                let wrote_any = &wrote_any;
                let ctx = &ctx;
                sc.spawn(move || {
                    let mut st = Stage3::new(ncamp, nd, cat_names_ref.len(), day0, ctx.base);
                    loop {
                        let p = next.fetch_add(1, Ordering::Relaxed);
                        if p >= NP2 {
                            break;
                        }
                        st.partition(p, s2b, cat_rank_ref);
                    }
                    let tw = Instant::now();
                    let compute = tw.duration_since(t0).as_secs_f64();
                    if !st.rows.is_empty() {
                        out::write_users(&ud.join(format!("part-{tid:03}.parquet")), &st.rows, cat_names_ref);
                    }
                    eprintln!("  t{tid} stage3 compute end {compute:.3} write {:.3}s rows {}", tw.elapsed().as_secs_f64(), st.rows.len());
                    if !st.rows.is_empty() {
                        wrote_any.fetch_add(1, Ordering::Relaxed);
                    }
                    st.camp
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    if wrote_any.load(Ordering::Relaxed) == 0 {
        out::write_users(&ud.join("part-empty.parquet"), &[], &cat_names);
    }
    eprintln!("[{:.3}] stage3 done", t0.elapsed().as_secs_f64());

    let mut total = vec![[0i64; 4]; ncs * nd];
    for s in &stats {
        for (a, b) in total.iter_mut().zip(s.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    let mut crows: Vec<out::CampRow> = Vec::new();
    for c in 0..ncs {
        for d in 0..nd {
            let v = total[c * nd + d];
            if v[0] > 0 || v[3] > 0 {
                crows.push(out::CampRow {
                    camp: c,
                    day: (day0 + d as i64) as i32,
                    sessions: v[0],
                    purchasing: v[1],
                    revenue: v[2],
                    assisted: v[3],
                });
            }
        }
    }
    out::write_campaigns(&cd.join("part-000.parquet"), &crows, &camp_ids, &channels);
    eprintln!("[{:.3}] done", t0.elapsed().as_secs_f64());
    #[cfg(target_os = "linux")]
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        eprintln!(
            "rusage user {:.3}s sys {:.3}s minflt {} majflt {} maxrss {} MB",
            ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 / 1e6,
            ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 / 1e6,
            ru.ru_minflt,
            ru.ru_majflt,
            ru.ru_maxrss >> 10
        );
    }
    // Skip destructors / munmap of gigabytes.
    std::process::exit(0);
}

const EXTRA: usize = 16 << 10;

#[inline(always)]
fn read_at(f: &std::fs::File, buf: &mut [u8], off: u64) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        f.read_exact_at(buf, off).expect("read");
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            let n = f.seek_read(&mut buf[done..], off + done as u64).expect("read");
            assert!(n > 0, "short read");
            done += n;
        }
    }
}

/// Reads file bytes [start-1, end + tail) into `buf`, extended until the line holding byte
/// end-1 is complete. Returns the chunk's [start, end) as indexes into `buf` (start is 0
/// only at the file start, so the header skip in `for_lines` still applies).
fn load_chunk(f: &std::fs::File, flen: usize, start: usize, end: usize, buf: &mut Vec<u8>) -> (usize, usize) {
    let r0 = start.saturating_sub(1);
    let mut want = (end + EXTRA).min(flen);
    loop {
        let n = want - r0;
        buf.clear();
        buf.reserve(n);
        unsafe { buf.set_len(n) };
        read_at(f, buf, r0 as u64);
        if want == flen || memchr::memchr(b'\n', &buf[end - 1 - r0..]).is_some() {
            break;
        }
        want = (want + 4 * EXTRA + (want - r0)).min(flen);
    }
    (start - r0, end - r0)
}

#[inline(never)]
fn links_chunk(buf: &[u8], start: usize, end: usize, m: &mut Masks) -> Vec<(u64, u64)> {
    let mut v = Vec::with_capacity((end - start) / 20 + 16);
    for_lines::<2>(buf, start, end, m, |f| {
        if f[0].1 > f[0].0 && f[1].1 > f[1].0 {
            v.push((pack_reg(buf, f[0].0, f[0].1), pack_reg(buf, f[1].0, f[1].1)));
        }
    });
    v
}

#[inline(never)]
fn events_chunk(buf: &[u8], start: usize, end: usize, ctx: &Ctx, m: &mut Masks, out: &mut Scatter<ERec>, pool: &mut Pool) {
    let (ws, we, base) = (ctx.ws, ctx.we, ctx.base);
    for_lines::<7>(buf, start, end, m, |f| {
        if f[0].1 == f[0].0 {
            return;
        }
        let ekey = pack_noreg(buf, f[0].0, f[0].1);
        let ingest = f1_field(buf, f[3].0, f[3].1).unwrap_or(i64::MAX / 4);
        let ingest_s = (ingest.div_euclid(1000)).clamp(0, u32::MAX as i64) as u32;
        let mut valid = true;
        let ukey = if f[1].1 == f[1].0 {
            valid = false;
            0
        } else {
            pack_reg(buf, f[1].0, f[1].1)
        };
        let ts = match ts_at(buf, f[2].0, f[2].1) {
            Some(t) if t >= ws && t < we => t,
            _ => {
                valid = false;
                0
            }
        };
        let ty = &buf[f[4].0..f[4].1];
        let prod = &buf[f[5].0..f[5].1];
        let mut meta = 0u32;
        if prod.is_empty() {
            valid = false;
        }
        match ty {
            b"view" | b"cart" => {}
            b"purchase" => {
                if valid {
                    match parse_price(&buf[f[6].0..f[6].1]) {
                        Some(p) => {
                            let cat = match ctx.products.get(prod) {
                                Some(c) => c as u8,
                                None => ctx.unknown_cat,
                            };
                            meta = p | (cat as u32) << 24;
                        }
                        None => valid = false,
                    }
                }
            }
            _ => valid = false,
        }
        if valid && ingest - ts > DAY {
            valid = false;
        }
        let tsr = if valid { (ts - base) as u32 } else { u32::MAX };
        let p = part1(ekey);
        out.push(p, ERec { eid: ekey, uid: ukey, ingest: ingest_s, ts: tsr, meta }, pool);
    });
}

#[inline(never)]
fn clicks_chunk(buf: &[u8], start: usize, end: usize, ctx: &Ctx, m: &mut Masks, out: &mut Scatter<C1>, pool: &mut Pool) {
    let lo = ctx.ws - WEEK;
    let hi = ctx.we;
    for_lines::<4>(buf, start, end, m, |f| {
        if f[0].1 == f[0].0 || f[1].1 == f[1].0 {
            return;
        }
        let ts = match ts_at(buf, f[3].0, f[3].1) {
            Some(t) if t >= lo && t < hi => t,
            _ => return,
        };
        let Some(camp) = ctx.campaigns.get(&buf[f[2].0..f[2].1]) else { return };
        let ckey = pack_reg(buf, f[0].0, f[0].1);
        let uid = pack_reg(buf, f[1].0, f[1].1);
        out.push(part3(uid), C1 { uid, ckey, ts: (ts - ctx.base) as u32, camp: camp as u32 }, pool);
    });
}

#[inline(never)]
fn dedup_partition(local: &[ERec], table: &mut Vec<u64>, out: &mut Scatter<URec>, pool: &mut Pool) {
    let n = local.len();
    if n == 0 {
        return;
    }
    // slot = tag (high 32 bits of a hash of eid) << 32 | index + 1; 0 = empty
    let cap = (2 * n).next_power_of_two().max(16);
    let bits = cap.trailing_zeros();
    let mask = cap - 1;
    table.clear();
    table.resize(cap, 0);
    for (i, r) in local.iter().enumerate() {
        let eid = r.eid;
        let hv = eid.wrapping_mul(0xA076_1D64_78BD_642F);
        let tag = (hv.rotate_left(29) >> 32) << 32;
        let mut h = thash(eid, bits);
        loop {
            let slot = unsafe { *table.get_unchecked(h) };
            if slot == 0 {
                unsafe { *table.get_unchecked_mut(h) = tag | (i as u64 + 1) };
                break;
            }
            if slot & !0xFFFF_FFFF == tag {
                let j = (slot as u32 - 1) as usize;
                let o = unsafe { local.get_unchecked(j) };
                if o.eid == eid {
                    if r.ingest < o.ingest {
                        unsafe { *table.get_unchecked_mut(h) = tag | (i as u64 + 1) };
                    }
                    break;
                }
            }
            h = (h + 1) & mask;
        }
    }
    for &slot in table.iter() {
        if slot == 0 {
            continue;
        }
        let r = unsafe { *local.get_unchecked((slot as u32 - 1) as usize) };
        if r.ts == u32::MAX {
            continue;
        }
        let uid = r.uid;
        out.push(part3(uid), URec { uid, tm: (r.ts as u64) << 32 | r.meta as u64 }, pool);
    }
}

#[derive(Clone, Copy)]
pub struct URow {
    pub person: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: i64,
    pub top: u16, // u16::MAX = null
}

#[derive(Clone, Copy)]
struct LClick {
    ts: u32,
    camp: u32,
    ckey: u64,
}

struct Stage3 {
    ncamp: usize,
    nd: usize,
    day0: i64,
    base: i64,
    camp: Vec<[i64; 4]>,
    rows: Vec<URow>,
    // scratch
    persons: Vec<u64>,
    table: Vec<u32>,
    eidx: Vec<u32>,
    cidx: Vec<u32>,
    ecount: Vec<u32>,
    ccount: Vec<u32>,
    evs: Vec<u64>,
    cls: Vec<LClick>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    catrev: Vec<i64>,
    touched: Vec<u8>,
}

impl Stage3 {
    fn new(ncamp: usize, nd: usize, ncat: usize, day0: i64, base: i64) -> Self {
        Stage3 {
            ncamp,
            nd,
            day0,
            base,
            camp: vec![[0; 4]; (ncamp + 1) * nd],
            rows: Vec::new(),
            persons: Vec::new(),
            table: Vec::new(),
            eidx: Vec::new(),
            cidx: Vec::new(),
            ecount: Vec::new(),
            ccount: Vec::new(),
            evs: Vec::new(),
            cls: Vec::new(),
            cnt: vec![0; ncamp + 1],
            pos: vec![0; ncamp + 1],
            active: Vec::new(),
            catrev: vec![0; ncat.max(1)],
            touched: Vec::new(),
        }
    }

#[inline(never)]
    fn partition(&mut self, p: usize, s2b: &[S2bOut], cat_rank: &[u16]) {
        let n_ev: usize = s2b.iter().map(|o| o.pv.part_len(p)).sum();
        if n_ev == 0 {
            return;
        }
        // Sized for ~4 events per person (load ~0.25 at the usual ~30); grown if needed.
        let mut cap = (n_ev / 2).next_power_of_two().max(256);
        let mut bits = cap.trailing_zeros();
        let mut mask = cap - 1;
        self.table.clear();
        self.table.resize(cap, u32::MAX);
        self.persons.clear();
        self.eidx.clear();
        self.ecount.clear();
        // group events by person
        for o in s2b {
            o.pv.each(p, |sl| for r in sl {
                let mut h = thash(r.person, bits);
                let idx = loop {
                    let s = unsafe { *self.table.get_unchecked(h) };
                    if s == u32::MAX {
                        let idx = self.persons.len() as u32;
                        unsafe { *self.table.get_unchecked_mut(h) = idx };
                        self.persons.push(r.person);
                        self.ecount.push(0);
                        if self.persons.len() * 2 > cap {
                            cap *= 2;
                            bits += 1;
                            mask = cap - 1;
                            self.table.clear();
                            self.table.resize(cap, u32::MAX);
                            for (j, &k) in self.persons.iter().enumerate() {
                                let mut g = thash(k, bits);
                                while self.table[g] != u32::MAX {
                                    g = (g + 1) & mask;
                                }
                                self.table[g] = j as u32;
                            }
                        }
                        break idx;
                    }
                    if unsafe { *self.persons.get_unchecked(s as usize) } == r.person {
                        break s;
                    }
                    h = (h + 1) & mask;
                };
                self.eidx.push(idx);
                unsafe { *self.ecount.get_unchecked_mut(idx as usize) += 1 };
            });
        }
        let np = self.persons.len();
        // exclusive prefix sums -> offsets (ecount becomes start offsets)
        let mut acc = 0u32;
        for c in self.ecount.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        let estart: Vec<u32> = self.ecount.clone();
        self.evs.clear();
        self.evs.resize(n_ev, 0);
        {
            let mut k = 0usize;
            for o in s2b {
                o.pv.each(p, |sl| for r in sl {
                    let idx = self.eidx[k] as usize;
                    k += 1;
                    let at = self.ecount[idx] as usize;
                    self.ecount[idx] += 1;
                    unsafe { *self.evs.get_unchecked_mut(at) = r.tm };
                });
            }
        }
        // clicks
        self.ccount.clear();
        self.ccount.resize(np + 1, 0);
        self.cidx.clear();
        let mut n_cl = 0usize;
        for o in s2b {
            o.cv.each(p, |sl| for r in sl {
                let mut h = thash(r.person, bits);
                let idx = loop {
                    let s = unsafe { *self.table.get_unchecked(h) };
                    if s == u32::MAX {
                        break u32::MAX;
                    }
                    if unsafe { *self.persons.get_unchecked(s as usize) } == r.person {
                        break s;
                    }
                    h = (h + 1) & mask;
                };
                self.cidx.push(idx);
                if idx != u32::MAX {
                    self.ccount[idx as usize] += 1;
                    n_cl += 1;
                }
            });
        }
        let mut acc = 0u32;
        for c in self.ccount.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        let cstart: Vec<u32> = self.ccount.clone();
        self.cls.clear();
        self.cls.resize(n_cl, LClick { ts: 0, camp: 0, ckey: 0 });
        {
            let mut k = 0usize;
            for o in s2b {
                o.cv.each(p, |sl| for r in sl {
                    let idx = self.cidx[k];
                    k += 1;
                    if idx == u32::MAX {
                        continue;
                    }
                    let at = self.ccount[idx as usize] as usize;
                    self.ccount[idx as usize] += 1;
                    self.cls[at] = LClick { ts: r.ts, camp: r.camp, ckey: r.ckey };
                });
            }
        }
        for pi in 0..np {
            let es = estart[pi] as usize;
            let ee = if pi + 1 < np { estart[pi + 1] as usize } else { n_ev };
            let cs = cstart[pi] as usize;
            let ce = cstart[pi + 1] as usize;
            let person = self.persons[pi];
            self.person(person, es, ee, cs, ce, cat_rank);
        }
    }

#[inline(never)]
    fn person(&mut self, person: u64, es: usize, ee: usize, cs: usize, ce: usize, cat_rank: &[u16]) {
        let mut evs = std::mem::take(&mut self.evs);
        let mut cls = std::mem::take(&mut self.cls);
        let ev = &mut evs[es..ee];
        ev.sort_unstable();
        let cl = &mut cls[cs..ce];
        if cl.len() > 1 {
            if cl.iter().any(|c| c.ckey & FB != 0) {
                cl.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(a.ckey, b.ckey)));
            } else {
                cl.sort_unstable_by_key(|c| ((c.ts as u128) << 64) | c.ckey as u128);
            }
        }
        let organic = self.ncamp as u32;
        let nd = self.nd;
        let n = ev.len();
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut i = 0usize;
        let mut cur_day = i64::MIN;
        let mut d_sess = 0u32;
        let mut d_ev = 0u32;
        let mut d_rev = 0i64;
        while i < n {
            let s = (ev[i] >> 32) as i64;
            let day = (self.base + s).div_euclid(DAY);
            if day != cur_day {
                if cur_day != i64::MIN {
                    self.flush_day(person, cur_day, d_sess, d_ev, d_rev, cat_rank);
                }
                cur_day = day;
                d_sess = 0;
                d_ev = 0;
                d_rev = 0;
            }
            let mut last = s;
            let mut j = i;
            let mut rev = 0i64;
            let mut purch = false;
            while j < n {
                let t = (ev[j] >> 32) as i64;
                if t - last > GAP {
                    break;
                }
                last = t;
                let meta = ev[j] as u32;
                let price = (meta & 0xFF_FFFF) as i64;
                if price > 0 {
                    rev += price;
                    purch = true;
                    let c = (meta >> 24) as usize;
                    if self.catrev[c] == 0 {
                        self.touched.push(c as u8);
                    }
                    self.catrev[c] += price;
                }
                j += 1;
            }
            let nev = (j - i) as u32;
            // attribution window [s - WEEK, s]
            while hi < cl.len() && (cl[hi].ts as i64) <= s {
                let c = cl[hi].camp as usize;
                if self.cnt[c] == 0 {
                    self.pos[c] = self.active.len() as u32;
                    self.active.push(c as u32);
                }
                self.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && (cl[lo].ts as i64) < s - WEEK {
                self.remove(cl[lo].camp as usize);
                lo += 1;
            }
            let credited = if lo < hi {
                let t = cl[hi - 1].ts;
                let mut k = hi - 1;
                while k > lo && cl[k - 1].ts == t {
                    k -= 1;
                }
                cl[k].camp
            } else {
                organic
            };
            let di = (day - self.day0) as usize;
            {
                let e = &mut self.camp[credited as usize * nd + di];
                e[0] += 1;
                e[1] += purch as i64;
                e[2] += rev;
            }
            for &a in &self.active {
                self.camp[a as usize * nd + di][3] += 1;
            }
            d_sess += 1;
            d_ev += nev;
            d_rev += rev;
            i = j;
        }
        if cur_day != i64::MIN {
            self.flush_day(person, cur_day, d_sess, d_ev, d_rev, cat_rank);
        }
        while lo < hi {
            self.remove(cl[lo].camp as usize);
            lo += 1;
        }
        self.evs = evs;
        self.cls = cls;
    }

    #[inline(always)]
    fn remove(&mut self, c: usize) {
        self.cnt[c] -= 1;
        if self.cnt[c] == 0 {
            let at = self.pos[c] as usize;
            let last = self.active.pop().unwrap();
            if at < self.active.len() {
                self.active[at] = last;
                self.pos[last as usize] = at as u32;
            }
        }
    }

#[inline(never)]
    fn flush_day(&mut self, person: u64, day: i64, sessions: u32, events: u32, revenue: i64, cat_rank: &[u16]) {
        let mut top = u16::MAX;
        let mut best = 0i64;
        for &c in &self.touched {
            let v = self.catrev[c as usize];
            if top == u16::MAX || v > best || (v == best && cat_rank[c as usize] < cat_rank[top as usize]) {
                top = c as u16;
                best = v;
            }
            self.catrev[c as usize] = 0;
        }
        self.touched.clear();
        self.rows.push(URow { person, day: day as i32, sessions, events, revenue, top });
    }
}
