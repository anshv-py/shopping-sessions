//! CSV dialect, timestamps, prices and ID keys.

use std::cmp::Ordering;
use std::sync::OnceLock;

/// Position just after the next '\n' at or after `i` (or the buffer end).
#[inline(always)]
pub fn skip_line(buf: &[u8], i: usize) -> usize {
    if i >= buf.len() {
        return buf.len();
    }
    match memchr::memchr(b'\n', &buf[i..]) {
        Some(p) => i + p + 1,
        None => buf.len(),
    }
}

/// First line start owned by a chunk beginning at `start`. The line at file offset 0 is
/// the header and is skipped.
#[inline(always)]
pub fn chunk_begin(buf: &[u8], start: usize) -> usize {
    if start == 0 {
        skip_line(buf, 0)
    } else if buf[start - 1] == b'\n' {
        start
    } else {
        skip_line(buf, start)
    }
}

/// Splits the line starting at `i` into exactly N fields (quotes stripped). Returns the
/// start of the next line and whether the line is well formed.
#[inline(always)]
pub fn parse_line<const N: usize>(buf: &[u8], mut i: usize, f: &mut [(usize, usize); N]) -> (usize, bool) {
    let n = buf.len();
    let mut k = 0usize;
    loop {
        let s;
        let e;
        let mut j;
        if i < n && unsafe { *buf.get_unchecked(i) } == b'"' {
            s = i + 1;
            j = s;
            while j < n {
                let b = unsafe { *buf.get_unchecked(j) };
                if b == b'"' || b == b',' || b == b'\n' {
                    break;
                }
                j += 1;
            }
            if j >= n || buf[j] != b'"' {
                return (skip_line(buf, j), false);
            }
            e = j;
            j += 1;
        } else {
            s = i;
            j = i;
            while j < n {
                let b = unsafe { *buf.get_unchecked(j) };
                if b == b'"' || b == b',' || b == b'\n' {
                    break;
                }
                j += 1;
            }
            if j < n && buf[j] == b'"' {
                return (skip_line(buf, j), false);
            }
            e = j;
        }
        if k >= N {
            return (skip_line(buf, j), false);
        }
        f[k] = (s, e);
        k += 1;
        if j >= n {
            return (n, k == N);
        }
        let b = buf[j];
        if b == b'\n' {
            return (j + 1, k == N);
        }
        if b == b',' {
            i = j + 1;
            continue;
        }
        return (skip_line(buf, j), false);
    }
}

// ---------------------------------------------------------------- SIMD line walker

/// Per-thread scratch for `for_lines`: per 64-byte word the quote, delimiter and newline
/// bits; the flattened delimiter positions; and three bit streams with one bit per
/// delimiter (is a newline / next byte is a quote / previous byte closes a quoted field).
#[derive(Default)]
pub struct Masks {
    quo: Vec<u64>,
    del: Vec<u64>,
    nl: Vec<u64>,
    pos: Vec<u32>,
    nlf: Vec<u64>,
    qf: Vec<u64>,
    cq: Vec<u32>,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn block3(p: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let m = |v: __m256i, c: u8| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c as i8))) as u32 as u64;
        (
            m(a, b',') | (m(b, b',') << 32),
            m(a, b'\n') | (m(b, b'\n') << 32),
            m(a, b'"') | (m(b, b'"') << 32),
        )
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
fn block3(p: &[u8; 64]) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for (i, &b) in p.iter().enumerate() {
        c |= ((b == b',') as u64) << i;
        n |= ((b == b'\n') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (c, n, q)
}

/// Bits of x at the set positions of m, packed to the low end.
#[inline(always)]
fn pext64(x: u64, m: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        std::arch::x86_64::_pext_u64(x, m)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let (mut r, mut m, mut k) = (0u64, m, 0);
        while m != 0 {
            let b = m.trailing_zeros();
            r |= ((x >> b) & 1) << k;
            k += 1;
            m &= m - 1;
        }
        r
    }
}

/// 64 bits of the bit stream `v` starting at bit `i` (v must have a readable word past).
#[inline(always)]
fn bits64(v: &[u64], i: usize) -> u64 {
    let w = i >> 6;
    let r = (i & 63) as u32;
    let lo = unsafe { *v.get_unchecked(w) } >> r;
    let hi = (unsafe { *v.get_unchecked(w + 1) } << 1) << (63 - r);
    lo | hi
}

/// Appends `x` (its bits past the count being appended are zero) at bit `at` of a stream
/// filled up to `at`: ORs into the current word and assigns the next, so streams need no
/// clearing (bits past the last delimiter are stale and never inspected).
#[inline(always)]
fn put_bits(v: &mut [u64], at: usize, x: u64) {
    let w = at >> 6;
    let sh = (at & 63) as u32;
    unsafe {
        *v.get_unchecked_mut(w) |= x << sh;
        *v.get_unchecked_mut(w + 1) = (x >> 1) >> (63 - sh);
    }
}

/// Index of the first set bit at or after bit `i` (one must exist).
#[cold]
#[inline(never)]
fn next_set(v: &[u64], i: usize) -> usize {
    let mut w = i >> 6;
    let mut bits = v[w] & (!0u64 << (i & 63));
    while bits == 0 {
        w += 1;
        bits = v[w];
    }
    w * 64 + bits.trailing_zeros() as usize
}

/// Calls `cb` with the N fields (quotes stripped) of every well-formed line that starts
/// in [start, end). Malformed lines (wrong field count, stray quotes, empty) are skipped.
///
/// Commas are always delimiters (a comma inside quotes makes the line malformed), so a
/// well-formed line is exactly N-1 commas then a newline. Delimiter positions are
/// flattened into an array, with bit streams marking per delimiter: newline; the next byte
/// is a quote (the following field opens with one); the previous byte is a quote that is
/// not a whole one-byte field (the preceding field closes with one). Quote rule: every
/// field is free of quotes or is `"` + quote-free text + `"`, i.e. the line's quote count
/// equals twice the number of fields that both open and close with a quote.
/// The fields of one line: field k is buf[s(k)..e(k)] (quotes stripped).
pub struct Fields {
    b0: usize,
    s: [u32; 8],
    e: [u32; 8],
}

impl Fields {
    #[inline(always)]
    pub fn s(&self, k: usize) -> usize {
        self.b0 + self.s[k] as usize
    }
    #[inline(always)]
    pub fn e(&self, k: usize) -> usize {
        self.b0 + self.e[k] as usize
    }
    #[inline(always)]
    pub fn get(&self, k: usize) -> (usize, usize) {
        (self.s(k), self.e(k))
    }
    #[inline(always)]
    pub fn is_empty(&self, k: usize) -> bool {
        self.s[k] == self.e[k]
    }
}

#[inline(always)]
pub fn for_lines<const N: usize>(
    buf: &[u8],
    start: usize,
    end: usize,
    m: &mut Masks,
    mut cb: impl FnMut(&Fields),
) {
    const { assert!(N >= 1 && N <= 8) };
    let len = buf.len();
    let b0 = chunk_begin(buf, start);
    if b0 >= end || b0 >= len {
        return;
    }
    let e1 = if end >= len { len } else { skip_line(buf, end - 1) };
    let n = e1 - b0;
    assert!(n < u32::MAX as usize - 64, "chunk too large");
    let words = n / 64 + 4;
    for v in [&mut m.quo, &mut m.del, &mut m.nl] {
        if v.len() < words {
            v.resize(words, 0);
        }
        // words past the data are read (lookahead, virtual newline): keep them zero
        v[n / 64..words].iter_mut().for_each(|x| *x = 0);
    }
    let sw = (n + 80) / 64 + 3; // stream words: at most n + 2 delimiters
    for v in [&mut m.nlf, &mut m.qf] {
        if v.len() < sw {
            v.resize(sw, 0);
        }
    }
    if m.cq.len() < words {
        m.cq.resize(words, 0);
    }
    m.pos.clear();
    m.pos.reserve(n + 80); // every byte a delimiter, plus the unconditional 16-entry writes
    let region = &buf[b0..e1];
    let full = n / 64;
    for w in 0..full {
        // w * 64 + 64 <= n and w < words: in bounds
        let blk: &[u8; 64] = unsafe { &*(region.as_ptr().add(w * 64) as *const [u8; 64]) };
        let (c, l, q) = block3(blk);
        unsafe {
            *m.quo.get_unchecked_mut(w) = q;
            *m.del.get_unchecked_mut(w) = c | l;
            *m.nl.get_unchecked_mut(w) = l;
        }
    }
    if n % 64 != 0 {
        let mut tmp = [0u8; 64];
        tmp[..n % 64].copy_from_slice(&region[full * 64..]);
        let (c, l, q) = block3(&tmp);
        m.quo[full] = q;
        m.del[full] = c | l;
        m.nl[full] = l;
    }
    if region[n - 1] != b'\n' {
        // the last line ends at the chunk end: a virtual newline there
        m.nl[n >> 6] |= 1u64 << (n & 63);
        m.del[n >> 6] |= 1u64 << (n & 63);
    }
    // entry 0: a virtual newline at position -1, so line starts are uniform
    let pos = m.pos.as_mut_ptr();
    unsafe { *pos = u32::MAX };
    m.nlf[0] = 1;
    m.qf[0] = 0;
    let mut np = 1usize;
    // plain slices: their pointers stay in registers across the stores below
    let (quo, del, nl): (&[u64], &[u64], &[u64]) = (&m.quo, &m.del, &m.nl);
    let (nlf, qf, cq): (&mut [u64], &mut [u64], &mut [u32]) = (&mut m.nlf, &mut m.qf, &mut m.cq);
    let mut dprev = 1u64 << 63; // the virtual delimiter at -1
    let mut qprev = 0u64;
    let mut qopen = quo[0] & 1; // the field after the previous delimiter opens with a quote
    let mut qtot = 0u32;
    for w in 0..=(n >> 6) {
        // w + 1 < words: in bounds
        let (d, q, qn, l) = unsafe { (*del.get_unchecked(w), *quo.get_unchecked(w), *quo.get_unchecked(w + 1), *nl.get_unchecked(w)) };
        let qb = (q << 1) | (qprev >> 63); // quote just before
        let d2 = (d << 2) | (dprev >> 62); // delimiter two before
        let qa = (q >> 1) | (qn << 63); // quote just after
        dprev = d;
        qprev = q;
        unsafe { *cq.get_unchecked_mut(w) = qtot };
        qtot += q.count_ones();
        if d == 0 {
            continue;
        }
        let cnt = d.count_ones() as usize;
        let base = (w * 64) as u32;
        let p = unsafe { pos.add(np) };
        let mut bits = d;
        for k in 0..8 {
            unsafe { *p.add(k) = base + bits.trailing_zeros() };
            bits &= bits.wrapping_sub(1);
        }
        if cnt > 8 {
            for k in 8..16 {
                unsafe { *p.add(k) = base + bits.trailing_zeros() };
                bits &= bits.wrapping_sub(1);
            }
            let mut k = 16;
            while bits != 0 {
                unsafe { *p.add(k) = base + bits.trailing_zeros() };
                bits &= bits - 1;
                k += 1;
            }
        }
        put_bits(nlf, np, pext64(l, d));
        // field ending at delimiter j is quoted: it opens (quote after delimiter j-1) and
        // closes (quote before j, not a one-byte field) with a quote
        let open = pext64(qa, d);
        let close = pext64(qb & !d2, d);
        put_bits(qf, np, ((open << 1) | qopen) & close);
        qopen = (open >> (cnt - 1)) & 1;
        np += cnt;
    }
    // 8 initialised entries past the end: the field loads below read up to 8 ahead
    unsafe {
        for k in 0..8 {
            *pos.add(np + k) = 0;
        }
        m.pos.set_len(np);
    }
    let pos: &[u32] = &m.pos;
    let posp = pos.as_ptr();
    let (nlf, qf, cq): (&[u64], &[u64], &[u32]) = (&m.nlf, &m.qf, &m.cq);
    let quo: &[u64] = &m.quo;
    // quotes before byte p
    let qbefore = |p: usize| -> u32 {
        let w = p >> 6;
        unsafe { *cq.get_unchecked(w) + (*quo.get_unchecked(w) & ((1u64 << (p & 63)) - 1)).count_ones() }
    };
    let want = 1u64 << (N - 1);
    let fmask = (1u64 << N) - 1;
    let mut f = Fields { b0, s: [0; 8], e: [0; 8] };
    let mut i = 1usize;
    let mut qs = 0u32; // quotes before the current line
    while i < np {
        let fl = bits64(nlf, i);
        if fl & fmask == want {
            // N-1 commas, then the newline; field k spans delimiters i-1+k .. i+k
            let le = unsafe { *pos.get_unchecked(i + N - 1) } as usize;
            let pm = bits64(qf, i) & fmask; // quoted fields
            // field k: start = pos[i-1+k] + 1 + d, end = pos[i+k] - d (d = 1 if quoted)
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            unsafe {
                use std::arch::x86_64::*;
                let p0 = _mm256_loadu_si256(posp.add(i - 1) as *const __m256i);
                let p1 = _mm256_loadu_si256(posp.add(i) as *const __m256i);
                let one = _mm256_set1_epi32(1);
                let d = _mm256_and_si256(_mm256_srlv_epi32(_mm256_set1_epi32(pm as i32), _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7)), one);
                _mm256_storeu_si256(f.s.as_mut_ptr() as *mut __m256i, _mm256_add_epi32(_mm256_add_epi32(p0, one), d));
                _mm256_storeu_si256(f.e.as_mut_ptr() as *mut __m256i, _mm256_sub_epi32(p1, d));
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
            for k in 0..N {
                let d = ((pm >> k) & 1) as u32;
                f.s[k] = unsafe { *posp.add(i - 1 + k) }.wrapping_add(1) + d;
                f.e[k] = unsafe { *posp.add(i + k) } - d;
            }
            // the line's quotes: those before its newline minus those before it (the newline
            // itself is no quote, so the next line starts with the same count)
            let qe = qbefore(le);
            let quotes = qe - qs;
            qs = qe;
            // single call site, so the callback inlines into the loop
            if quotes == 2 * pm.count_ones() {
                cb(&f);
            }
            i += N;
        } else {
            // malformed: skip to the delimiter that ends the line
            let k = if fl != 0 { i + fl.trailing_zeros() as usize } else { next_set(nlf, i) };
            qs = qbefore(unsafe { *pos.get_unchecked(k) } as usize);
            i = k + 1;
        }
    }
}

// ---------------------------------------------------------------- keys
//
// An ID of at most 9 bytes, all ASCII 0x01..=0x7F, packs exactly into 63 bits: 7 bits per
// byte, first byte most significant, zero padded. Numeric order == byte-wise order. Any
// other ID gets FB | 63-bit hash; IDs whose bytes matter later (names, ordering) are
// recorded in a registry (one per parsing thread, no locks), published read-only once
// parsing is done.

pub const FB: u64 = 1 << 63;

/// Bytes of hashed (FB) keys: open addressing over keys, values index `arena`
/// (4-byte length then the bytes).
#[derive(Default)]
pub struct Reg {
    keys: Vec<u64>, // 0 = empty slot (every stored key has FB set)
    offs: Vec<u32>,
    arena: Vec<u8>,
    n: usize,
}

impl Reg {
    #[inline(always)]
    fn slot(k: u64, mask: usize) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & mask
    }
    pub fn len(&self) -> usize {
        self.n
    }
    pub fn insert(&mut self, k: u64, v: &[u8]) {
        if 2 * (self.n + 1) > self.keys.len() {
            self.grow();
        }
        let mask = self.keys.len() - 1;
        let mut h = Self::slot(k, mask);
        loop {
            let t = unsafe { *self.keys.get_unchecked(h) };
            if t == k {
                return;
            }
            if t == 0 {
                break;
            }
            h = (h + 1) & mask;
        }
        self.keys[h] = k;
        self.offs[h] = self.arena.len() as u32;
        self.arena.extend_from_slice(&(v.len() as u32).to_le_bytes());
        self.arena.extend_from_slice(v);
        self.n += 1;
    }
    #[cold]
    fn grow(&mut self) {
        let cap = (self.keys.len() * 2).max(1024);
        let old_k = std::mem::replace(&mut self.keys, vec![0; cap]);
        let old_o = std::mem::replace(&mut self.offs, vec![0; cap]);
        let mask = cap - 1;
        for (i, &k) in old_k.iter().enumerate() {
            if k != 0 {
                let mut h = Self::slot(k, mask);
                while self.keys[h] != 0 {
                    h = (h + 1) & mask;
                }
                self.keys[h] = k;
                self.offs[h] = old_o[i];
            }
        }
    }
    pub fn get(&self, k: u64) -> Option<&[u8]> {
        if self.keys.is_empty() {
            return None;
        }
        let mask = self.keys.len() - 1;
        let mut h = Self::slot(k, mask);
        loop {
            let t = self.keys[h];
            if t == k {
                let o = self.offs[h] as usize;
                let l = u32::from_le_bytes(self.arena[o..o + 4].try_into().unwrap()) as usize;
                return Some(&self.arena[o + 4..o + 4 + l]);
            }
            if t == 0 {
                return None;
            }
            h = (h + 1) & mask;
        }
    }
    /// Every (key, bytes) entry.
    pub fn each(&self, mut f: impl FnMut(u64, &[u8])) {
        for (i, &k) in self.keys.iter().enumerate() {
            if k != 0 {
                let o = self.offs[i] as usize;
                let l = u32::from_le_bytes(self.arena[o..o + 4].try_into().unwrap()) as usize;
                f(k, &self.arena[o + 4..o + 4 + l]);
            }
        }
    }
}

/// Registries readable by `decode` / `key_cmp`: the link registry (set before link
/// naming), then the parsing threads' (set after parsing).
static LINK_REG: OnceLock<Reg> = OnceLock::new();
static THREAD_REGS: OnceLock<Vec<Reg>> = OnceLock::new();

/// Hashed keys: FB | 62-bit hash (bit 62 clear). Arena keys (click_ids): FB | AK | arena ref.
pub const AK: u64 = 1 << 62;

/// Append-only per-thread store of ID bytes, read through 32-bit references
/// (thread << 26 | 4-byte word offset + 1); an entry is a 4-byte length then the bytes.
pub struct Arena {
    tid: u32,
    buf: Vec<u8>,
}

const AR_TID: u32 = 26;
const AR_MAX_THREADS: usize = 64;

impl Arena {
    pub fn new(tid: usize) -> Self {
        assert!(tid < AR_MAX_THREADS);
        Arena { tid: tid as u32, buf: Vec::new() }
    }
    /// The reference of a new copy of v, or 0 when the arena is full.
    #[inline(always)]
    pub fn put(&mut self, v: &[u8]) -> u32 {
        let off = self.buf.len();
        let words = 1 + v.len().div_ceil(4);
        if off / 4 + words >= (1 << AR_TID) - 1 {
            return 0;
        }
        self.buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(v);
        self.buf.resize(off + 4 * words, 0);
        (self.tid << AR_TID) | (off / 4 + 1) as u32
    }
}

static ARENAS: OnceLock<Vec<Arena>> = OnceLock::new();

/// After parsing: the arenas of every parsing thread (any order).
pub fn publish_arenas(v: Vec<Arena>) {
    let mut by: Vec<Arena> = (0..AR_MAX_THREADS).map(|t| Arena { tid: t as u32, buf: Vec::new() }).collect();
    for a in v {
        let t = a.tid as usize;
        by[t] = a;
    }
    let _ = ARENAS.set(by);
}

fn arena_bytes(r: u32) -> &'static [u8] {
    let a = &ARENAS.get().expect("arenas published")[(r >> AR_TID) as usize];
    let o = 4 * ((r & ((1 << AR_TID) - 1)) as usize - 1);
    let l = u32::from_le_bytes(a.buf[o..o + 4].try_into().unwrap()) as usize;
    &a.buf[o + 4..o + 4 + l]
}

/// One table of hashed user_ids shared by all parsing threads (instead of a copy per
/// thread): lock-free insert by compare-and-swap of the key; the inserting thread copies
/// the bytes to its arena and stores the reference. Read (by `decode`) only after parsing.
/// When it gets too full, inserts go to the thread's own `Reg` instead.
pub struct GSet {
    keys: Vec<std::sync::atomic::AtomicU64>,
    refs: Vec<std::sync::atomic::AtomicU32>,
    count: std::sync::atomic::AtomicUsize,
    limit: usize,
}

static GSET: OnceLock<GSet> = OnceLock::new();

/// Creates the shared table with room for about `cap` keys (call before parsing).
pub fn init_gset(cap: usize) {
    let cap = cap.next_power_of_two().max(1 << 14);
    let keys = (0..cap).map(|_| std::sync::atomic::AtomicU64::new(0)).collect();
    let refs = (0..cap).map(|_| std::sync::atomic::AtomicU32::new(0)).collect();
    let _ = GSET.set(GSet { keys, refs, count: std::sync::atomic::AtomicUsize::new(0), limit: cap / 10 * 7 });
}

impl GSet {
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & (self.keys.len() - 1)
    }
    /// Records k's bytes once across all threads.
    #[inline(always)]
    fn insert(&self, k: u64, v: &[u8], ar: &mut Arena, reg: &mut Reg) {
        use std::sync::atomic::Ordering::Relaxed;
        let mask = self.keys.len() - 1;
        let mut h = self.slot(k);
        loop {
            let cur = unsafe { self.keys.get_unchecked(h) }.load(Relaxed);
            if cur == k {
                return;
            }
            if cur == 0 {
                if self.count.load(Relaxed) >= self.limit {
                    reg.insert(k, v);
                    return;
                }
                match self.keys[h].compare_exchange(0, k, Relaxed, Relaxed) {
                    Ok(_) => {
                        self.count.fetch_add(1, Relaxed);
                        let r = ar.put(v);
                        if r == 0 {
                            reg.insert(k, v);
                        } else {
                            self.refs[h].store(r, Relaxed);
                        }
                        return;
                    }
                    Err(now) if now == k => return,
                    Err(_) => {}
                }
            }
            h = (h + 1) & mask;
        }
    }
    fn get(&self, k: u64) -> Option<u32> {
        use std::sync::atomic::Ordering::Relaxed;
        let mask = self.keys.len() - 1;
        let mut h = self.slot(k);
        loop {
            let cur = self.keys[h].load(Relaxed);
            if cur == k {
                let r = self.refs[h].load(Relaxed);
                return if r == 0 { None } else { Some(r) };
            }
            if cur == 0 {
                return None;
            }
            h = (h + 1) & mask;
        }
    }
}

pub fn publish_link_reg(r: Reg) {
    let _ = LINK_REG.set(r);
}

pub fn publish_thread_regs(v: Vec<Reg>) {
    let _ = THREAD_REGS.set(v);
}

fn lookup(k: u64) -> &'static [u8] {
    if k & AK != 0 {
        return arena_bytes(k as u32);
    }
    if let Some(v) = LINK_REG.get().and_then(|r| r.get(k)) {
        return v;
    }
    if let Some(r) = GSET.get().and_then(|g| g.get(k)) {
        return arena_bytes(r);
    }
    if let Some(rs) = THREAD_REGS.get() {
        for r in rs {
            if let Some(v) = r.get(k) {
                return v;
            }
        }
    }
    panic!("fallback key registered")
}

const ONES: u64 = 0x0101_0101_0101_0101;
const HIGHS: u64 = 0x8080_8080_8080_8080;

#[inline(always)]
fn pext7(x: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        std::arch::x86_64::_pext_u64(x, 0x7F7F_7F7F_7F7F_7F7F)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut r = 0u64;
        for i in 0..8 {
            r |= ((x >> (8 * i)) & 0x7F) << (7 * i);
        }
        r
    }
}

#[inline(always)]
pub fn rd64(buf: &[u8], at: usize) -> u64 {
    debug_assert!(at + 8 <= buf.len());
    unsafe { (buf.as_ptr().add(at) as *const u64).read_unaligned() }
}

#[inline(always)]
pub fn rd32_at(buf: &[u8], at: usize) -> u32 {
    rd32(buf, at)
}

#[inline(always)]
fn rd32(buf: &[u8], at: usize) -> u32 {
    debug_assert!(at + 4 <= buf.len());
    unsafe { (buf.as_ptr().add(at) as *const u32).read_unaligned() }
}

/// Per ID length (0..=9): mask of the bytes kept from the first word, 0x01 filler for the
/// lanes past the end, and the same for byte 8 (kept only when the length is 9).
struct PackT {
    keep: u64,
    pad: u64,
    b8m: u64,
    b8p: u64,
}

const fn pack_t(len: usize) -> PackT {
    let n = if len > 8 { 8 } else { len };
    let keep = if n == 0 { 0 } else { u64::MAX >> (64 - 8 * n) };
    PackT { keep, pad: !keep & ONES, b8m: if len == 9 { 0xFF } else { 0 }, b8p: if len == 9 { 0 } else { 1 } }
}

static PACK_T: [PackT; 10] = [pack_t(0), pack_t(1), pack_t(2), pack_t(3), pack_t(4), pack_t(5), pack_t(6), pack_t(7), pack_t(8), pack_t(9)];

/// Exact key of buf[s..e], if it packs.
#[inline(always)]
fn pack_exact(buf: &[u8], s: usize, e: usize) -> Option<u64> {
    let len = e - s;
    if len > 9 {
        return None;
    }
    if len != 0 && s + 9 <= buf.len() {
        // branch-free: keep the first min(len, 8) bytes, byte 8 only when len == 9
        let t = &PACK_T[len];
        let lo = rd64(buf, s) & t.keep;
        let b8 = unsafe { *buf.get_unchecked(s + 8) } as u64 & t.b8m;
        // lanes past the end read as 0x01, so each lane is valid iff it is in 0x01..=0x7F:
        // x - 1 and x both keep bit 7 clear exactly then (no borrow leaves a valid lane)
        let v = lo | t.pad;
        let w = b8 | t.b8p;
        if (v.wrapping_sub(ONES) | v | w.wrapping_sub(1) | w) & HIGHS != 0 {
            return None;
        }
        Some((pext7(lo.swap_bytes()) << 7) | b8)
    } else {
        let mut k = 0u64;
        for i in 0..9 {
            let c = if s + i < e { buf[s + i] } else { 0 };
            if s + i < e && (c == 0 || c >= 0x80) {
                return None;
            }
            k = (k << 7) | c as u64;
        }
        Some(k)
    }
}

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = a as u128 * b as u128;
    (r as u64) ^ ((r >> 64) as u64)
}

/// 64-bit hash of buf[s..e], 8 bytes at a time (reads stay inside [s, e)).
#[inline(always)]
pub fn hash_at(buf: &[u8], s: usize, e: usize) -> u64 {
    const K: [u64; 4] = [0xa076_1d64_78bd_642f, 0xe703_7ed1_a0b4_28db, 0x8ebc_6af0_9c88_c6e3, 0x5899_65cc_7537_4cc3];
    let len = e - s;
    let mut h = (len as u64).wrapping_mul(K[0]) ^ K[1];
    if len >= 8 {
        let mut i = s;
        while i + 16 <= e {
            h = mum(rd64(buf, i) ^ K[2], rd64(buf, i + 8) ^ h);
            i += 16;
        }
        if i < e {
            // the last 1..=15 bytes, read as two (possibly overlapping) words ending at e
            let a = if e - i > 8 { rd64(buf, i) } else { 0 };
            h = mum(a ^ K[3], rd64(buf, e - 8) ^ h ^ K[0]);
        }
    } else {
        let mut x = 0u64;
        for i in s..e {
            x = (x << 8) | buf[i] as u64;
        }
        h = mum(x ^ K[3], h);
    }
    mum(h ^ K[1], (len as u64) ^ K[2])
}

pub fn hash_bytes(v: &[u8]) -> u64 {
    hash_at(v, 0, v.len())
}

/// Hashed key of an ID that does not pack exactly (bit 62 clear: see AK).
#[inline(always)]
pub fn hkey(buf: &[u8], s: usize, e: usize) -> u64 {
    FB | (hash_at(buf, s, e) >> 2)
}

/// Per-thread queue of hashed user_ids waiting to be recorded in the shared table: each
/// one's slot is prefetched when queued, and the inserts run 32 at a time, so the table's
/// cache misses overlap instead of stalling the parse. Holds offsets into the current
/// buffer: `flush` before the buffer changes.
pub struct UidQueue {
    k: [u64; 32],
    s: [u32; 32],
    e: [u32; 32],
    n: usize,
    pub ar: Arena,
    pub reg: Reg,
}

impl UidQueue {
    pub fn new(tid: usize) -> Self {
        UidQueue { k: [0; 32], s: [0; 32], e: [0; 32], n: 0, ar: Arena::new(tid), reg: Reg::default() }
    }
    #[inline(never)]
    pub fn flush(&mut self, buf: &[u8]) {
        for i in 0..self.n {
            let (k, s, e) = (self.k[i], self.s[i] as usize, self.e[i] as usize);
            match GSET.get() {
                Some(g) => g.insert(k, &buf[s..e], &mut self.ar, &mut self.reg),
                None => self.reg.insert(k, &buf[s..e]),
            }
        }
        self.n = 0;
    }
}

/// user_id key; a hashed one is queued for recording in the shared table (`UidQueue`).
#[inline(always)]
pub fn pack_uid(buf: &[u8], s: usize, e: usize, q: &mut UidQueue) -> u64 {
    match pack_exact(buf, s, e) {
        Some(k) => k,
        None => pack_uid_slow(buf, s, e, q),
    }
}

#[cold]
#[inline(never)]
fn pack_uid_slow(buf: &[u8], s: usize, e: usize, q: &mut UidQueue) -> u64 {
    let k = hkey(buf, s, e);
    if let Some(g) = GSET.get() {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(g.keys.as_ptr().add(g.slot(k)) as *const i8);
        }
    }
    if q.n == 32 {
        q.flush(buf);
    }
    let i = q.n;
    q.k[i] = k;
    q.s[i] = s as u32;
    q.e[i] = e as u32;
    q.n += 1;
    k
}

/// click_id key: click_ids are only ever ordered (on equal ts), never matched, so a
/// non-packing one is just a reference to a copy of its bytes.
#[inline(always)]
pub fn pack_cid(buf: &[u8], s: usize, e: usize, ar: &mut Arena, reg: &mut Reg) -> u64 {
    match pack_exact(buf, s, e) {
        Some(k) => k,
        None => pack_cid_slow(buf, s, e, ar, reg),
    }
}

#[cold]
#[inline(never)]
fn pack_cid_slow(buf: &[u8], s: usize, e: usize, ar: &mut Arena, reg: &mut Reg) -> u64 {
    let r = ar.put(&buf[s..e]);
    if r != 0 { FB | AK | r as u64 } else { pack_slow_reg(buf, s, e, reg) }
}

/// Key for an ID whose exact bytes matter later (names, ordering).
#[inline(always)]
pub fn pack_reg(buf: &[u8], s: usize, e: usize, reg: &mut Reg) -> u64 {
    match pack_exact(buf, s, e) {
        Some(k) => k,
        None => pack_slow_reg(buf, s, e, reg),
    }
}

#[cold]
#[inline(never)]
fn pack_slow_reg(buf: &[u8], s: usize, e: usize, reg: &mut Reg) -> u64 {
    let k = hkey(buf, s, e);
    reg.insert(k, &buf[s..e]);
    k
}

/// Key for an ID only compared for equality.
#[inline(always)]
pub fn pack_noreg(buf: &[u8], s: usize, e: usize) -> u64 {
    match pack_exact(buf, s, e) {
        Some(k) => k,
        None => hkey(buf, s, e),
    }
}

/// The bytes of key k (exact keys are unpacked into tmp).
#[inline]
pub fn key_bytes(k: u64, tmp: &mut [u8; 9]) -> &[u8] {
    if k & FB == 0 {
        let mut n = 0;
        while n < 9 {
            let c = (k >> (56 - 7 * n)) & 0x7F;
            if c == 0 {
                break;
            }
            tmp[n] = c as u8;
            n += 1;
        }
        &tmp[..n]
    } else {
        lookup(k)
    }
}

pub fn decode(k: u64, out: &mut Vec<u8>) {
    let mut t = [0u8; 9];
    out.extend_from_slice(key_bytes(k, &mut t));
}

#[inline(always)]
pub fn key_cmp(a: u64, b: u64) -> Ordering {
    if (a | b) & FB == 0 {
        return a.cmp(&b);
    }
    key_cmp_slow(a, b)
}

#[inline(never)]
fn key_cmp_slow(a: u64, b: u64) -> Ordering {
    if a == b {
        return Ordering::Equal;
    }
    let (mut x, mut y) = ([0u8; 9], [0u8; 9]);
    key_bytes(a, &mut x).cmp(key_bytes(b, &mut y))
}

#[inline(always)]
pub fn key_lt(a: u64, b: u64) -> bool {
    key_cmp(a, b) == Ordering::Less
}

// ---------------------------------------------------------------- timestamps

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some(a as i64 * 10 + b as i64)
    } else {
        None
    }
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}

const CAL_Y0: i64 = 1970;
const CAL_NY: usize = 200;
static CAL: OnceLock<Vec<i32>> = OnceLock::new();

fn calendar() -> &'static [i32] {
    CAL.get_or_init(|| {
        let mut v = vec![i32::MIN; CAL_NY * 13 * 32];
        for yi in 0..CAL_NY {
            let y = CAL_Y0 + yi as i64;
            for mo in 1..=12 {
                for d in 1..=days_in_month(y, mo) {
                    v[yi * 416 + mo as usize * 32 + d as usize] = days_from_civil(y, mo, d) as i32;
                }
            }
        }
        v
    })
}

/// `YYYY-MM-DDTHH:MM:SS` (first 19 bytes) to epoch ms.
#[inline(always)]
fn parse_dt(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if h > 23 || mi > 59 || se > 59 || mo > 12 || d > 31 {
        return None;
    }
    let yi = (y - CAL_Y0) as usize;
    let days = if yi < CAL_NY {
        let v = unsafe { *calendar().get_unchecked(yi * 416 + mo as usize * 32 + d as usize) };
        if v == i32::MIN {
            return None;
        }
        v as i64
    } else {
        if mo < 1 || d < 1 || d > days_in_month(y, mo) {
            return None;
        }
        days_from_civil(y, mo, d)
    };
    Some((days * 86_400 + h * 3600 + mi * 60 + se) * 1000)
}


#[inline(always)]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n == 20 && s[19] == b'Z' {
        return parse_dt(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        let t = parse_dt(s)?;
        return Some(if s[19] == b'+' { t - off } else { t + off });
    }
    let mut v: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

// ---------------------------------------------------------------- SWAR timestamps

const fn lane_mask(lanes: &[usize]) -> u64 {
    let mut m = 0u64;
    let mut i = 0;
    while i < lanes.len() {
        m |= 0xFFu64 << (8 * lanes[i]);
        i += 1;
    }
    m
}

/// 8 ASCII digits (first digit in the lowest byte) to their value.
#[inline(always)]
pub fn parse8(v: u64) -> u64 {
    let v = v.wrapping_sub(0x3030_3030_3030_3030);
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    ((v & 0x0000_00FF_0000_00FF)
        .wrapping_mul(100 + (1_000_000u64 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32
}

/// True when every lane selected by `m` holds an ASCII digit.
#[inline(always)]
pub fn digits_ok(v: u64, m: u64) -> bool {
    let hi = m & 0xF0F0_F0F0_F0F0_F0F0;
    let lo = m & 0x0F0F_0F0F_0F0F_0F0F;
    (v & hi) == (0x3030_3030_3030_3030 & m) && (((v & lo).wrapping_add(0x0606_0606_0606_0606 & m)) & hi) == 0
}

const MC: u64 = lane_mask(&[1, 2]);
const SCM: u64 = lane_mask(&[0, 3]);
const SC: u64 = 0x3A | (0x5Au64 << 24);

// Per-thread cache of the date part (`YYYY-MM-DD`) -> days since epoch. Timestamps in a
// dataset fall within a few weeks, so nearly every lookup hits.
/// Each parsing thread owns one; passed explicitly so lookups inline.
pub struct DayCache {
    a: [u64; 64],
    dd: [u32; 64],
    days: [i32; 64],
}

impl Default for DayCache {
    fn default() -> Self {
        DayCache { a: [0; 64], dd: [0; 64], days: [i32::MIN; 64] }
    }
}

const DMA: u64 = lane_mask(&[0, 1, 2, 3, 5, 6]);
const DSM: u64 = lane_mask(&[4, 7]);
const DS: u64 = (0x2Du64 << 32) | (0x2Du64 << 56);

#[cold]
fn date_slow(a: u64, dd: u64) -> Option<i64> {
    if (a & DSM) != DS || !digits_ok(a, DMA) || !digits_ok(dd, 0xFFFF) {
        return None;
    }
    let da = a & 0x0F0F_0F0F_0F0F_0F0F;
    let ta = da.wrapping_mul(10).wrapping_add(da >> 8);
    let y = ((ta & 0xFF) * 100 + ((ta >> 16) & 0xFF)) as i64;
    let mo = ((ta >> 40) & 0xFF) as i64;
    let d = ((dd & 0x0F) * 10 + ((dd >> 8) & 0x0F)) as i64;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
        return None;
    }
    Some(days_from_civil(y, mo, d))
}

/// Days since epoch of the date in `a` (bytes 0..8) and the low two bytes of `b`.
#[inline(always)]
fn date_days(a: u64, b: u64, c: &mut DayCache) -> Option<i64> {
    let dd = b & 0xFFFF;
    let idx = ((dd ^ (dd >> 6) ^ (a >> 40)) & 63) as usize;
    if c.a[idx] == a && c.dd[idx] == dd as u32 {
        let v = c.days[idx];
        return if v == i32::MIN { None } else { Some(v as i64) };
    }
    let v = date_slow(a, dd);
    c.a[idx] = a;
    c.dd[idx] = dd as u32;
    c.days[idx] = v.map_or(i32::MIN, |x| x as i32);
    v
}

const TBM: u64 = lane_mask(&[3, 4, 6, 7]);
const TSM: u64 = lane_mask(&[2, 5]);
const TS: u64 = (0x54u64 << 16) | (0x3Au64 << 40);

/// `THH:MM` in b's lanes 2..8 -> (hour, minute), validated.
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
fn hm(b: u64) -> Option<(i64, i64)> {
    if (b & TSM) != TS || !digits_ok(b, TBM) {
        return None;
    }
    let db = b & 0x0F0F_0F0F_0F0F_0F0F;
    let tb = db.wrapping_mul(10).wrapping_add(db >> 8);
    let h = ((tb >> 24) & 0xFF) as i64;
    let mi = ((tb >> 48) & 0xFF) as i64;
    if (h > 23) | (mi > 59) {
        return None;
    }
    Some((h, mi))
}

/// SSE time part of F1 / F2: the 16 bytes at s+4 are `-MM-DDTHH:MM:SS` plus one more byte
/// (`Z` for F1, the offset sign for F2). Checks the separators (lane 15 only when `z`) and
/// the month, day and time digits, and returns (h * 3600 + m * 60 + s, byte s+19).
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn hms_simd(buf: &[u8], s: usize, z: bool) -> Option<i64> {
    use std::arch::x86_64::*;
    unsafe {
        let v = _mm_loadu_si128(buf.as_ptr().add(s + 4) as *const __m128i);
        let t = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let dig = _mm_cmpeq_epi8(_mm_min_epu8(t, _mm_set1_epi8(9)), t);
        let tmpl = _mm_setr_epi8(
            b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, b':' as i8, 0, 0, b'Z' as i8,
        );
        let sep = _mm_cmpeq_epi8(v, tmpl);
        let sepm = _mm_setr_epi8(-128, 0, 0, -128, 0, 0, -128, 0, 0, -128, 0, 0, -128, 0, 0, -128);
        let ok = _mm_movemask_epi8(_mm_blendv_epi8(dig, sep, sepm)) as u32;
        let want = if z { 0xFFFF } else { 0x7FFF };
        // digit pairs (hour, minute, second) -> 10 * a + b in u16 lanes 0..3 (garbage when
        // the digit check fails, and then unused)
        let shuf = _mm_setr_epi8(7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1);
        let m = _mm_maddubs_epi16(_mm_shuffle_epi8(t, shuf), _mm_set1_epi16(0x010A));
        // range check in one compare: lanes over (23, 59, 59, 0, ...) are bad
        let over = _mm_movemask_epi8(_mm_cmpgt_epi16(m, _mm_setr_epi16(23, 59, 59, 0, 0, 0, 0, 0))) as u32;
        if (ok & want != want) | (over != 0) {
            return None;
        }
        // h * 3600 + mi * 60 in i32 lane 0, se in lane 1
        let hm = _mm_madd_epi16(m, _mm_setr_epi16(3600, 60, 1, 0, 0, 0, 0, 0));
        let x = _mm_cvtsi128_si64(hm) as u64;
        Some((x as u32 as i64) + (x >> 32) as i64)
    }
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ` at buf[s..s+20].
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn f1_at(buf: &[u8], s: usize, dc: &mut DayCache) -> Option<i64> {
    let t = hms_simd(buf, s, true)?;
    let days = date_days(rd64(buf, s), rd64(buf, s + 8), dc)?;
    Some((days * 86_400 + t) * 1000)
}

/// F2 `YYYY-MM-DDTHH:MM:SS+HH:MM` at buf[s..s+25].
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn f2_at(buf: &[u8], s: usize, dc: &mut DayCache) -> Option<i64> {
    let t = hms_simd(buf, s, false)?;
    let x = rd64(buf, s + 17); // S S sign H H : M M
    let sign = (x >> 16) & 0xFF;
    if ((x >> 40) & 0xFF) != 0x3A || !digits_ok(x, lane_mask(&[3, 4, 6, 7])) || (sign != 0x2B && sign != 0x2D) {
        return None;
    }
    let dx = x & 0x0F0F_0F0F_0F0F_0F0F;
    let tx = dx.wrapping_mul(10).wrapping_add(dx >> 8);
    let oh = ((tx >> 24) & 0xFF) as i64;
    let om = ((tx >> 48) & 0xFF) as i64;
    if (oh > 23) | (om > 59) {
        return None;
    }
    let days = date_days(rd64(buf, s), rd64(buf, s + 8), dc)?;
    let t = (days * 86_400 + t) * 1000;
    let off = (oh * 60 + om) * 60_000;
    Some(if sign == 0x2B { t - off } else { t + off })
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ` at buf[s..s+20].
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
fn f1_at(buf: &[u8], s: usize, dc: &mut DayCache) -> Option<i64> {
    let a = rd64(buf, s);
    let b = rd64(buf, s + 8);
    let c = rd32(buf, s + 16) as u64;
    if (c & SCM) != SC || !digits_ok(c, MC) {
        return None;
    }
    let se = (((c >> 8) & 0x0F) * 10 + ((c >> 16) & 0x0F)) as i64;
    let (h, mi) = hm(b)?;
    if se > 59 {
        return None;
    }
    let days = date_days(a, b, dc)?;
    Some((days * 86_400 + h * 3600 + mi * 60 + se) * 1000)
}

/// F2 `YYYY-MM-DDTHH:MM:SS+HH:MM` at buf[s..s+25].
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
fn f2_at(buf: &[u8], s: usize, dc: &mut DayCache) -> Option<i64> {
    let a = rd64(buf, s);
    let b = rd64(buf, s + 8);
    let x = rd64(buf, s + 17); // S S sign H H : M M
    if buf[s + 16] != b':' || ((x >> 40) & 0xFF) != 0x3A || !digits_ok(x, lane_mask(&[0, 1, 3, 4, 6, 7])) {
        return None;
    }
    let sign = (x >> 16) & 0xFF;
    if sign != 0x2B && sign != 0x2D {
        return None;
    }
    let dx = x & 0x0F0F_0F0F_0F0F_0F0F;
    let tx = dx.wrapping_mul(10).wrapping_add(dx >> 8);
    let se = (tx & 0xFF) as i64;
    let oh = ((tx >> 24) & 0xFF) as i64;
    let om = ((tx >> 48) & 0xFF) as i64;
    let (h, mi) = hm(b)?;
    if (se > 59) | (oh > 23) | (om > 59) {
        return None;
    }
    let days = date_days(a, b, dc)?;
    let t = (days * 86_400 + h * 3600 + mi * 60 + se) * 1000;
    let off = (oh * 60 + om) * 60_000;
    Some(if sign == 0x2B { t - off } else { t + off })
}

/// F3 digits at buf[s..e] (1..=16 of them, with 16 readable bytes from s).
#[inline(always)]
fn f3_at(buf: &[u8], s: usize, e: usize) -> Option<i64> {
    let len = e - s;
    let a = rd64(buf, s);
    if len <= 8 {
        let m = if len == 8 { !0 } else { (1u64 << (8 * len)) - 1 };
        if !digits_ok(a, m) {
            return None;
        }
        // move the digits to the top lanes and fill the low lanes with '0'
        let v = if len == 8 { a } else { ((a & m) << (8 * (8 - len))) | (0x3030_3030_3030_3030 >> (8 * len)) };
        Some(parse8(v) as i64)
    } else {
        let b = rd64(buf, s + 8);
        let k = len - 8; // digits in the high part, 1..=8
        let mb = if k == 8 { !0 } else { (1u64 << (8 * k)) - 1 };
        if !digits_ok(a, !0) || !digits_ok(b, mb) {
            return None;
        }
        let hi = if k == 8 { a } else { (a << (8 * (8 - k))) | (0x3030_3030_3030_3030 >> (8 * k)) };
        let lo = rd64(buf, e - 8);
        Some((parse8(hi) * 100_000_000 + parse8(lo)) as i64)
    }
}

/// Event/click `ts` of buf[s..e]; falls back to `parse_ts` off the fast paths.
#[inline(always)]
pub fn ts_at(buf: &[u8], s: usize, e: usize, dc: &mut DayCache) -> Option<i64> {
    let len = e - s;
    if len == 20 && unsafe { *buf.get_unchecked(s + 19) } == b'Z' {
        return f1_at(buf, s, dc);
    }
    if len >= 1 && len <= 16 && s + 16 <= buf.len() && unsafe { *buf.get_unchecked(s) }.wrapping_sub(b'0') < 10 {
        return f3_at(buf, s, e);
    }
    if len == 25 {
        return f2_at(buf, s, dc);
    }
    parse_ts(&buf[s..e])
}

/// ingest_ts (always F1).
#[inline(always)]
pub fn f1_field(buf: &[u8], s: usize, e: usize, dc: &mut DayCache) -> Option<i64> {
    if e - s == 20 && unsafe { *buf.get_unchecked(s + 19) } == b'Z' {
        f1_at(buf, s, dc)
    } else {
        None
    }
}

/// Purchase price: digits only, 1..=10_000_000.
#[inline(always)]
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u32 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u32;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn mutate(r: &mut Rng, s: &mut Vec<u8>) {
        let alphabet = b"0123456789-:TZtz+. e\r\x00\xc3";
        for _ in 0..r.below(3) {
            if s.is_empty() {
                break;
            }
            let i = r.below(s.len() as u64) as usize;
            match r.below(3) {
                0 => s[i] = alphabet[r.below(alphabet.len() as u64) as usize],
                1 => {
                    s.remove(i);
                }
                _ => s.insert(i, alphabet[r.below(alphabet.len() as u64) as usize]),
            }
        }
    }

    fn candidates(r: &mut Rng) -> Vec<u8> {
        let y = 1960 + r.below(250);
        let mo = r.below(14);
        let d = r.below(33);
        let h = r.below(26);
        let mi = r.below(62);
        let se = r.below(62);
        let mut s = match r.below(4) {
            0 => format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z").into_bytes(),
            1 => {
                let sign = if r.below(2) == 0 { '+' } else { '-' };
                format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}{sign}{:02}:{:02}", r.below(26), r.below(62)).into_bytes()
            }
            2 => {
                let len = 1 + r.below(22) as usize;
                let mut v: Vec<u8> = (0..len).map(|_| b'0' + r.below(10) as u8).collect();
                if r.below(3) == 0 {
                    let z = r.below(len as u64) as usize;
                    for b in v.iter_mut().take(z) {
                        *b = b'0';
                    }
                }
                v
            }
            _ => format!("{}", 1_700_000_000_000u64 + r.below(200_000_000_000)).into_bytes(),
        };
        if r.below(4) == 0 {
            mutate(r, &mut s);
        }
        s
    }

    #[test]
    fn swar_timestamps_match_reference() {
        let mut r = Rng(0x1234_5678_9abc_def1);
        let mut dc = DayCache::default();
        for _ in 0..3_000_000 {
            let s = candidates(&mut r);
            // embed with random tails so over-reads see junk
            let mut buf = s.clone();
            let tail = r.below(20) as usize;
            for _ in 0..tail {
                buf.push(b"0123456789Z,\n"[r.below(13) as usize]);
            }
            let fast = ts_at(&buf, 0, s.len(), &mut dc);
            let slow = parse_ts(&s);
            assert_eq!(fast, slow, "{:?}", String::from_utf8_lossy(&s));
            // and with plenty of readable bytes after the field
            let mut long = s.clone();
            for _ in 0..40 {
                long.push(b"0123456789Z:-+T,\n"[r.below(17) as usize]);
            }
            assert_eq!(ts_at(&long, 0, s.len(), &mut dc), slow, "long {:?}", String::from_utf8_lossy(&s));
            if s.len() == 20 {
                assert_eq!(f1_field(&buf, 0, 20, &mut dc), if s[19] == b'Z' { parse_ts(&s) } else { None });
            }
        }
    }

    /// Reference: every line starting in [start, end) parsed with the scalar parse_line.
    fn lines_ref<const N: usize>(buf: &[u8], start: usize, end: usize) -> Vec<Vec<(usize, usize)>> {
        let mut out = Vec::new();
        let mut i = chunk_begin(buf, start);
        let mut f = [(0usize, 0usize); N];
        while i < end && i < buf.len() {
            let (next, ok) = parse_line::<N>(buf, i, &mut f);
            if ok {
                out.push(f.to_vec());
            }
            i = next;
        }
        out
    }

    fn walker_matches<const N: usize>(r: &mut Rng, m: &mut Masks) {
        let alphabet: &[u8] = b"ab\",,,\n\"\r1";
        let lim = if r.below(4) == 0 { 3000 } else { 300 };
        let len = 1 + r.below(lim) as usize;
        let mut buf: Vec<u8> = Vec::with_capacity(len + 64);
        while buf.len() < len {
            match r.below(10) {
                // well-formed-ish line, with some quoted fields
                0..=4 => {
                    for k in 0..N {
                        let q = r.below(3) == 0;
                        if q {
                            buf.push(b'"');
                        }
                        let flen = if r.below(8) == 0 { 140 } else { 6 };
                        for _ in 0..r.below(flen) {
                            buf.push(b"xyz09"[r.below(5) as usize]);
                        }
                        if q && r.below(10) != 0 {
                            buf.push(b'"');
                        }
                        if k + 1 < N {
                            buf.push(b',');
                        }
                    }
                    buf.push(b'\n');
                }
                _ => buf.push(alphabet[r.below(alphabet.len() as u64) as usize]),
            }
        }
        // random chunk boundaries, as the pipeline uses them
        let cuts = 1 + r.below(4) as usize;
        let mut bounds: Vec<usize> = (0..cuts - 1).map(|_| r.below(buf.len() as u64) as usize).collect();
        bounds.push(0);
        bounds.push(buf.len());
        bounds.sort();
        for w in bounds.windows(2) {
            let (s, e) = (w[0], w[1]);
            if s == e {
                continue;
            }
            let mut got: Vec<Vec<(usize, usize)>> = Vec::new();
            for_lines::<N>(&buf, s, e, m, |f| got.push((0..N).map(|k| f.get(k)).collect()));
            let want = lines_ref::<N>(&buf, s, e);
            assert_eq!(got, want, "N={N} chunk {s}..{e} of {:?}", String::from_utf8_lossy(&buf));
        }
    }

    #[test]
    fn walker_matches_scalar_parser() {
        let mut r = Rng(0x9e37_79b9_7f4a_7c15);
        let mut m = Masks::default();
        for _ in 0..40_000 {
            walker_matches::<7>(&mut r, &mut m);
            walker_matches::<3>(&mut r, &mut m);
            walker_matches::<2>(&mut r, &mut m);
        }
    }

    #[test]
    fn keys_order_and_roundtrip() {
        let mut r = Rng(0xfeed_beef_1234_5678);
        let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyzABCZ_ \t\r\"\x01\x7f\xc3\xa9\x00\x80\xff\x7e";
        let hex = b"0123456789abcdef";
        let mut reg = Reg::default();
        let mut all: Vec<(Vec<u8>, u64)> = Vec::new();
        for i in 0..2_000_000 {
            let len = r.below(12) as usize;
            let mut s: Vec<u8> = (0..len).map(|_| alphabet[r.below(alphabet.len() as u64) as usize]).collect();
            if i % 4 == 0 {
                // v5-style long IDs: short base-36 part plus a 32-character hex suffix
                s.truncate(9);
                for _ in 0..32 {
                    s.push(hex[r.below(16) as usize]);
                }
            }
            let mut buf = s.clone();
            for _ in 0..r.below(12) {
                buf.push(alphabet[r.below(alphabet.len() as u64) as usize]);
            }
            let k = pack_reg(&buf, 0, s.len(), &mut reg);
            let exact = s.len() <= 9 && s.iter().all(|&c| (1..=0x7F).contains(&c));
            assert_eq!(k & FB == 0, exact, "{:?}", s);
            assert_eq!(pack_noreg(&buf, 0, s.len()), k);
            if s.is_empty() {
                assert_eq!(k, 0);
            }
            all.push((s, k));
        }
        publish_link_reg(reg);
        let mut d = Vec::new();
        for (i, (s, k)) in all.iter().enumerate() {
            d.clear();
            decode(*k, &mut d);
            assert_eq!(&d, s);
            if i > 0 {
                let (ps, pk) = &all[i - 1];
                assert_eq!(key_cmp(*pk, *k), ps.cmp(s), "{:?} {:?}", ps, s);
            }
        }
        // distinct IDs never share a key
        let mut m: std::collections::HashMap<u64, &Vec<u8>> = std::collections::HashMap::new();
        for (s, k) in &all {
            if let Some(o) = m.insert(*k, s) {
                assert_eq!(o, s, "key collision");
            }
        }
    }
}
