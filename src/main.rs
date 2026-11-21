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
const CHUNK: usize = 1 << 20;

#[derive(Clone, Copy)]
#[repr(C)]
struct ERec {
    eid: u64,
    uid: u64,
    ingest: i64,
    ts: u32, // relative to base; u32::MAX = invalid / late
    meta: u32, // price (24 bits) | category << 24, 0 for non-purchase
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    person: u64,
    tm: u64, // ts << 32 | meta
}

#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    person: u64,
    ckey: u64,
    ts: u32,
    camp: u32,
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

const BLK: usize = 256;

/// Per-thread radix scatter: one contiguous arena per thread, each partition a chain of
/// BLK-record blocks inside it. Only the used prefix of the arena is ever touched.
pub struct Scatter<T: Copy> {
    arena: Vec<T>,
    cur: Vec<(u32, u32)>,
    blocks: Vec<Vec<u32>>,
}

impl<T: Copy> Scatter<T> {
    fn new(np: usize, reserve: usize) -> Self {
        Scatter { arena: Vec::with_capacity(reserve + np * BLK), cur: vec![(u32::MAX, 0); np], blocks: (0..np).map(|_| Vec::new()).collect() }
    }
    #[inline(always)]
    fn push(&mut self, p: usize, v: T) {
        let c = unsafe { self.cur.get_unchecked_mut(p) };
        if c.1 as usize == BLK || c.0 == u32::MAX {
            if c.0 != u32::MAX {
                unsafe { self.blocks.get_unchecked_mut(p) }.push(c.0);
            }
            let start = self.arena.len();
            self.arena.reserve(BLK);
            unsafe { self.arena.set_len(start + BLK) };
            *c = (start as u32, 0);
        }
        unsafe { *self.arena.as_mut_ptr().add(c.0 as usize + c.1 as usize) = v };
        c.1 += 1;
    }
    #[inline(always)]
    fn each(&self, p: usize, mut f: impl FnMut(&[T])) {
        for &b in &self.blocks[p] {
            f(&self.arena[b as usize..b as usize + BLK]);
        }
        let c = self.cur[p];
        if c.0 != u32::MAX {
            f(&self.arena[c.0 as usize..(c.0 + c.1) as usize]);
        }
    }
    fn len(&self) -> usize {
        let mut n = 0;
        for p in 0..self.cur.len() {
            n += self.blocks[p].len() * BLK;
            if self.cur[p].0 != u32::MAX {
                n += self.cur[p].1 as usize;
            }
        }
        n
    }
    fn part_len(&self, p: usize) -> usize {
        self.blocks[p].len() * BLK + if self.cur[p].0 != u32::MAX { self.cur[p].1 as usize } else { 0 }
    }
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(p as *const i8);
    }
}

pub struct LinkMap {
    tab: Vec<(u64, u64)>,
    bits: u32,
}

impl LinkMap {
    /// Resolves many keys with software prefetching; f(i, person).
    #[inline(always)]
    fn resolve(&self, keys: &[u64], mut f: impl FnMut(usize, u64)) {
        if self.tab.is_empty() {
            for (i, &k) in keys.iter().enumerate() {
                f(i, k);
            }
            return;
        }
        const D: usize = 12;
        let n = keys.len();
        for i in 0..n.min(D) {
            prefetch(unsafe { self.tab.as_ptr().add(thash(keys[i], self.bits)) });
        }
        for i in 0..n {
            if i + D < n {
                prefetch(unsafe { self.tab.as_ptr().add(thash(keys[i + D], self.bits)) });
            }
            f(i, self.get(keys[i]));
        }
    }

    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        if self.tab.is_empty() {
            return k;
        }
        let mask = (1usize << self.bits) - 1;
        let mut h = thash(k, self.bits);
        loop {
            let e = unsafe { *self.tab.get_unchecked(h) };
            if e.0 == k {
                return e.1;
            }
            if e.0 == 0 {
                return k;
            }
            h = (h + 1) & mask;
        }
    }
}

fn build_links(pairs: &[Vec<(u64, u64)>]) -> LinkMap {
    let m: usize = pairs.iter().map(|v| v.len()).sum();
    if m == 0 {
        return LinkMap { tab: Vec::new(), bits: 0 };
    }
    let cap = (4 * m).next_power_of_two().max(16);
    let bits = cap.trailing_zeros();
    let mask = cap - 1;
    let mut slot: Vec<u32> = vec![u32::MAX; cap];
    let mut keys: Vec<u64> = Vec::with_capacity(2 * m);
    let mut parent: Vec<u32> = Vec::with_capacity(2 * m);
    let mut ins = |k: u64, keys: &mut Vec<u64>, parent: &mut Vec<u32>| -> u32 {
        let mut h = thash(k, bits);
        loop {
            let s = slot[h];
            if s == u32::MAX {
                let idx = keys.len() as u32;
                slot[h] = idx;
                keys.push(k);
                parent.push(idx);
                return idx;
            }
            if keys[s as usize] == k {
                return s;
            }
            h = (h + 1) & mask;
        }
    };
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let g = parent[parent[x as usize] as usize];
            parent[x as usize] = g;
            x = g;
        }
        x
    }
    for v in pairs {
        for &(a, b) in v {
            let ia = ins(a, &mut keys, &mut parent);
            let ib = ins(b, &mut keys, &mut parent);
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
    let mut minkey: Vec<u64> = vec![0; n];
    let mut roots: Vec<u32> = vec![0; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        roots[i] = r;
        let cur = minkey[r as usize];
        if cur == 0 || key_lt(keys[i], cur) {
            minkey[r as usize] = keys[i];
        }
    }
    // Final lookup table sized for the distinct keys (load <= 0.5), rebuilt so it is as
    // small (cache-friendly) as possible for the 25M+ lookups that follow.
    let fcap = (2 * n).next_power_of_two().max(16);
    let fbits = fcap.trailing_zeros();
    let fmask = fcap - 1;
    let mut tab = vec![(0u64, 0u64); fcap];
    for i in 0..n {
        let k = keys[i];
        let mut h = thash(k, fbits);
        while tab[h].0 != 0 {
            h = (h + 1) & fmask;
        }
        tab[h] = (k, minkey[roots[i] as usize]);
    }
    let _ = bits;
    LinkMap { tab, bits: fbits }
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
    cl: Scatter<CRec>,
}

struct S2Out {
    pv: Scatter<PRec>,
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
    let mut maps: [Vec<memmap2::Mmap>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for (k, d) in ["links", "events", "clicks"].iter().enumerate() {
        for p in list_csv(&data.join(d)) {
            if let Some(m) = mmap(&p) {
                maps[k].push(m);
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
                let e = (s + CHUNK).min(len);
                chunks.push(Chunk { kind, file: fi, start: s, end: e });
                s = e;
            }
        }
    }
    if let Ok(mode) = std::env::var("BENCH") {
        let mut bv: Scatter<ERec> = Scatter::new(NP1, ev_bytes / 60);
        let mut bm = Masks::default();
        for rep in 0..3 {
            let t = Instant::now();
            let mut acc = 0u64;
            for c in chunks.iter().filter(|c| c.kind == 1) {
                let buf: &[u8] = &maps[1][c.file];
                match mode.as_str() {
                    "sum" => {
                        for &b in &buf[c.start..c.end] {
                            acc = acc.wrapping_add(b as u64);
                        }
                    }
                    "nl" => acc += memchr::memchr_iter(b'\n', &buf[c.start..c.end]).count() as u64,
                    "split" => {
                        let mut m = Masks::default();
                        for_lines::<7>(buf, c.start, c.end, &mut m, |f| acc += f[6].1 as u64);
                    }
                    _ => {
                        events_chunk(buf, c.start, c.end, &ctx, &mut bm, &mut bv);
                        acc += 1;
                    }
                }
            }
            eprintln!("bench {mode} rep {rep}: {:.3}s acc {acc}", t.elapsed().as_secs_f64());
        }
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

    let link_pairs: Mutex<Vec<Vec<(u64, u64)>>> = Mutex::new(Vec::new());
    let links_done = AtomicUsize::new(0);
    let link_map: OnceLock<LinkMap> = OnceLock::new();
    if n_link_chunks == 0 {
        let _ = link_map.set(LinkMap { tab: Vec::new(), bits: 0 });
    }
    let next = AtomicUsize::new(0);
    let est_ev = ev_bytes / 80 + 1024;
    let est_cl = cl_bytes / 40 + 1024;
    let cap1 = est_ev * 3 / (2 * nthreads);
    let capc = est_cl * 3 / (2 * nthreads);

    // ---------------- stage 1 ----------------
    let s1: Vec<S1Out> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                sc.spawn(|| {
                    let mut o = S1Out { ev: Scatter::new(NP1, cap1), cl: Scatter::new(NP2, capc) };
                    let mut cbuf: Vec<(u64, CRec)> = Vec::new();
                    let mut ckeys: Vec<u64> = Vec::new();
                    let mut masks = Masks::default();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(c) = chunks.get(i) else { break };
                        let buf: &[u8] = &maps[c.kind as usize][c.file];
                        match c.kind {
                            0 => {
                                let v = links_chunk(buf, c.start, c.end, &mut masks);
                                link_pairs.lock().unwrap().push(v);
                                if links_done.fetch_add(1, Ordering::AcqRel) + 1 == n_link_chunks {
                                    let pairs = std::mem::take(&mut *link_pairs.lock().unwrap());
                                    let lm = build_links(&pairs);
                                    let _ = link_map.set(lm);
                                }
                            }
                            1 => events_chunk(buf, c.start, c.end, &ctx, &mut masks, &mut o.ev),
                            _ => {
                                let lm = loop {
                                    if let Some(lm) = link_map.get() {
                                        break lm;
                                    }
                                    std::thread::yield_now();
                                };
                                clicks_chunk(buf, c.start, c.end, &ctx, lm, &mut masks, &mut cbuf, &mut ckeys, &mut o.cl);
                            }
                        }
                    }
                    o
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let link_map = link_map.into_inner().unwrap();
    eprintln!("[{:.3}] stage1 done", t0.elapsed().as_secs_f64());

    // ---------------- stage 2: dedup ----------------
    let next = AtomicUsize::new(0);
    let total_ev: usize = s1.iter().map(|o| o.ev.len()).sum();
    let cap2 = total_ev * 5 / (4 * nthreads);
    let s2: Vec<S2Out> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                sc.spawn(|| {
                    let mut o = S2Out { pv: Scatter::new(NP2, cap2) };
                    let mut local: Vec<ERec> = Vec::new();
                    let mut table: Vec<u32> = Vec::new();
                    let mut surv: Vec<(u64, u64)> = Vec::new();
                    let mut skeys: Vec<u64> = Vec::new();
                    loop {
                        let p = next.fetch_add(1, Ordering::Relaxed);
                        if p >= NP1 {
                            break;
                        }
                        local.clear();
                        for t in &s1 {
                            t.ev.each(p, |sl| local.extend_from_slice(sl));
                        }
                        dedup_partition(&local, &mut table, &link_map, &mut surv, &mut skeys, &mut o.pv);
                    }
                    o
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    eprintln!("[{:.3}] stage2 done", t0.elapsed().as_secs_f64());

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
                let s1 = &s1;
                let s2 = &s2;
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
                        st.partition(p, s1, s2, cat_rank_ref);
                    }
                    if !st.rows.is_empty() {
                        out::write_users(&ud.join(format!("part-{tid:03}.parquet")), &st.rows, cat_names_ref);
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
    // Skip destructors / munmap of gigabytes.
    std::process::exit(0);
}

fn links_chunk(buf: &[u8], start: usize, end: usize, m: &mut Masks) -> Vec<(u64, u64)> {
    let mut v = Vec::with_capacity((end - start) / 20 + 16);
    for_lines::<2>(buf, start, end, m, |f| {
        if f[0].1 > f[0].0 && f[1].1 > f[1].0 {
            v.push((pack_reg(buf, f[0].0, f[0].1), pack_reg(buf, f[1].0, f[1].1)));
        }
    });
    v
}

fn events_chunk(buf: &[u8], start: usize, end: usize, ctx: &Ctx, m: &mut Masks, out: &mut Scatter<ERec>) {
    let (ws, we, base) = (ctx.ws, ctx.we, ctx.base);
    for_lines::<7>(buf, start, end, m, |f| {
        if f[0].1 == f[0].0 {
            return;
        }
        let ekey = pack_noreg(buf, f[0].0, f[0].1);
        let ingest = f1_field(buf, f[3].0, f[3].1).unwrap_or(i64::MAX / 4);
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
        out.push(p, ERec { eid: ekey, uid: ukey, ingest, ts: tsr, meta });
    });
}

#[allow(clippy::too_many_arguments)]
fn clicks_chunk(
    buf: &[u8],
    start: usize,
    end: usize,
    ctx: &Ctx,
    lm: &LinkMap,
    m: &mut Masks,
    cbuf: &mut Vec<(u64, CRec)>,
    keys: &mut Vec<u64>,
    out: &mut Scatter<CRec>,
) {
    cbuf.clear();
    keys.clear();
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
        keys.push(pack_reg(buf, f[1].0, f[1].1));
        cbuf.push((0, CRec { person: 0, ckey, ts: (ts - ctx.base) as u32, camp: camp as u32 }));
    });
    lm.resolve(keys, |i, person| {
        let mut r = cbuf[i].1;
        r.person = person;
        out.push(part2(person), r);
    });
}

fn dedup_partition(
    local: &[ERec],
    table: &mut Vec<u32>,
    lm: &LinkMap,
    surv: &mut Vec<(u64, u64)>,
    keys: &mut Vec<u64>,
    out: &mut Scatter<PRec>,
) {
    let n = local.len();
    if n == 0 {
        return;
    }
    let cap = (2 * n).next_power_of_two().max(16);
    let bits = cap.trailing_zeros();
    let mask = cap - 1;
    table.clear();
    table.resize(cap, u32::MAX);
    for (i, r) in local.iter().enumerate() {
        let mut h = thash(r.eid, bits);
        loop {
            let s = unsafe { *table.get_unchecked(h) };
            if s == u32::MAX {
                unsafe { *table.get_unchecked_mut(h) = i as u32 };
                break;
            }
            let o = unsafe { local.get_unchecked(s as usize) };
            if o.eid == r.eid {
                if r.ingest < o.ingest {
                    unsafe { *table.get_unchecked_mut(h) = i as u32 };
                }
                break;
            }
            h = (h + 1) & mask;
        }
    }
    surv.clear();
    keys.clear();
    for &s in table.iter() {
        if s == u32::MAX {
            continue;
        }
        let r = unsafe { local.get_unchecked(s as usize) };
        if r.ts == u32::MAX {
            continue;
        }
        keys.push(r.uid);
        surv.push((r.uid, (r.ts as u64) << 32 | r.meta as u64));
    }
    lm.resolve(keys, |i, person| {
        out.push(part2(person), PRec { person, tm: surv[i].1 });
    });
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

    fn partition(&mut self, p: usize, s1: &[S1Out], s2: &[S2Out], cat_rank: &[u16]) {
        let n_ev: usize = s2.iter().map(|o| o.pv.part_len(p)).sum();
        if n_ev == 0 {
            return;
        }
        let cap = (2 * n_ev).next_power_of_two().max(16);
        let bits = cap.trailing_zeros();
        let mask = cap - 1;
        self.table.clear();
        self.table.resize(cap, u32::MAX);
        self.persons.clear();
        self.eidx.clear();
        self.ecount.clear();
        // group events by person
        for o in s2 {
            o.pv.each(p, |sl| for r in sl {
                let mut h = thash(r.person, bits);
                let idx = loop {
                    let s = unsafe { *self.table.get_unchecked(h) };
                    if s == u32::MAX {
                        let idx = self.persons.len() as u32;
                        unsafe { *self.table.get_unchecked_mut(h) = idx };
                        self.persons.push(r.person);
                        self.ecount.push(0);
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
            for o in s2 {
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
        for o in s1 {
            o.cl.each(p, |sl| for r in sl {
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
            for o in s1 {
                o.cl.each(p, |sl| for r in sl {
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

    fn person(&mut self, person: u64, es: usize, ee: usize, cs: usize, ce: usize, cat_rank: &[u16]) {
        let mut evs = std::mem::take(&mut self.evs);
        let mut cls = std::mem::take(&mut self.cls);
        let ev = &mut evs[es..ee];
        ev.sort_unstable();
        let cl = &mut cls[cs..ce];
        if cl.len() > 1 {
            cl.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(a.ckey, b.ckey)));
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
