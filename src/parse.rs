//! CSV dialect, timestamps, prices and ID keys.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

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

/// Per-thread scratch for `for_lines`: comma, newline and quote bitmasks of one chunk.
#[derive(Default)]
pub struct Masks {
    quo: Vec<u64>,
    com: Vec<u64>,
    nl: Vec<u64>,
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

/// 128 bits of `v` starting at bit `a` (v must have 3 readable words past a >> 6).
#[inline(always)]
fn window(v: &[u64], a: usize) -> u128 {
    let w = a >> 6;
    let r = (a & 63) as u32;
    let x0 = unsafe { *v.get_unchecked(w) } as u128;
    let x1 = unsafe { *v.get_unchecked(w + 1) } as u128;
    let x2 = unsafe { *v.get_unchecked(w + 2) } as u128;
    let lo = x0 | (x1 << 64);
    if r == 0 {
        lo
    } else {
        (lo >> r) | (x2 << (128 - r))
    }
}

/// Calls `cb` with the N fields (quotes stripped) of every well-formed line that starts
/// in [start, end). Malformed lines (wrong field count, stray quotes, empty) are skipped.
#[inline(always)]
pub fn for_lines<const N: usize>(
    buf: &[u8],
    start: usize,
    end: usize,
    m: &mut Masks,
    mut cb: impl FnMut(&[(usize, usize); N]),
) {
    let len = buf.len();
    let b0 = chunk_begin(buf, start);
    if b0 >= end || b0 >= len {
        return;
    }
    let e1 = if end >= len { len } else { skip_line(buf, end - 1) };
    let n = e1 - b0;
    let words = n / 64 + 4;
    m.com.clear();
    m.com.resize(words, 0);
    m.nl.clear();
    m.nl.resize(words, 0);
    m.quo.clear();
    m.quo.resize(words, 0);
    let region = &buf[b0..e1];
    let full = n / 64;
    for w in 0..full {
        let blk: &[u8; 64] = region[w * 64..w * 64 + 64].try_into().unwrap();
        let (c, l, q) = block3(blk);
        m.com[w] = c;
        m.nl[w] = l;
        m.quo[w] = q;
    }
    if n % 64 != 0 {
        let mut tmp = [0u8; 64];
        tmp[..n % 64].copy_from_slice(&region[full * 64..]);
        let (c, l, q) = block3(&tmp);
        m.com[full] = c;
        m.nl[full] = l;
        m.quo[full] = q;
    }
    if region[n - 1] != b'\n' {
        m.nl[n >> 6] |= 1u64 << (n & 63);
    }
    let com = &m.com;
    let nl = &m.nl;
    let quo = &m.quo;
    let mut w = 0usize;
    let mut nbits = nl[0];
    let mut ls = 0usize;
    let mut f = [(0usize, 0usize); N];
    while ls < n {
        while nbits == 0 {
            w += 1;
            nbits = unsafe { *nl.get_unchecked(w) };
        }
        let le = w * 64 + nbits.trailing_zeros() as usize;
        nbits &= nbits - 1;
        let l = le - ls;
        if l < 128 {
            let lm: u128 = (1u128 << l) - 1;
            let mut cw = window(com, ls) & lm;
            if cw.count_ones() as usize == N - 1 {
                let mut prev = ls;
                for fk in f.iter_mut().take(N - 1) {
                    let p = ls + cw.trailing_zeros() as usize;
                    cw &= cw - 1;
                    *fk = (b0 + prev, b0 + p);
                    prev = p + 1;
                }
                f[N - 1] = (b0 + prev, b0 + le);
                let q = (window(quo, ls) & lm).count_ones();
                if q == 0 {
                    cb(&f);
                } else {
                    let mut shaped = 0u32;
                    for fk in f.iter_mut() {
                        let (a, b) = *fk;
                        let sh = (b - a >= 2)
                            && unsafe { *buf.get_unchecked(a) } == b'"'
                            && unsafe { *buf.get_unchecked(b - 1) } == b'"';
                        let d = sh as usize;
                        *fk = (a + d, b - d);
                        shaped += d as u32;
                    }
                    if q == 2 * shaped {
                        cb(&f);
                    }
                }
            }
        } else {
            let (_, ok) = parse_line::<N>(&buf[..b0 + le], b0 + ls, &mut f);
            if ok {
                cb(&f);
            }
        }
        ls = le + 1;
    }
}

// ---------------------------------------------------------------- keys
//
// An ID of at most 9 bytes, all ASCII 0x01..=0x7F, packs exactly into 63 bits: 7 bits per
// byte, first byte most significant, zero padded. Numeric order == byte-wise order. Any
// other ID gets FB | hash, with its bytes registered so names and ordering stay exact.

pub const FB: u64 = 1 << 63;

static FALLBACK: OnceLock<Mutex<HashMap<u64, Vec<u8>>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<u64, Vec<u8>>> {
    FALLBACK.get_or_init(|| Mutex::new(HashMap::new()))
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
fn rd32(buf: &[u8], at: usize) -> u32 {
    debug_assert!(at + 4 <= buf.len());
    unsafe { (buf.as_ptr().add(at) as *const u32).read_unaligned() }
}

/// Exact key of buf[s..e], if it packs.
#[inline(always)]
fn pack_exact(buf: &[u8], s: usize, e: usize) -> Option<u64> {
    let len = e - s;
    if len > 9 {
        return None;
    }
    if s + 9 <= buf.len() {
        let raw = rd64(buf, s);
        let lo = if len >= 8 { raw } else { raw & ((1u64 << (8 * len)) - 1) };
        let b8 = if len == 9 { buf[s + 8] as u64 } else { 0 };
        // lanes past the end get 0x01 so the zero-byte test only sees real bytes
        let pad = if len >= 8 { 0 } else { !((1u64 << (8 * len)) - 1) & ONES };
        let v = lo | pad;
        let has_zero = (v.wrapping_sub(ONES) & !v & HIGHS) != 0;
        let bad = has_zero || (lo & HIGHS) != 0 || b8 >= 0x80 || (len == 9 && b8 == 0);
        if bad {
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

pub fn hash_bytes(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in s {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h ^= s.len() as u64;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// Key for an ID whose exact bytes matter later (names, ordering).
#[inline(always)]
pub fn pack_reg(buf: &[u8], s: usize, e: usize) -> u64 {
    match pack_exact(buf, s, e) {
        Some(k) => k,
        None => pack_slow_reg(&buf[s..e]),
    }
}

#[cold]
#[inline(never)]
fn pack_slow_reg(v: &[u8]) -> u64 {
    let k = FB | (hash_bytes(v) >> 1);
    let mut m = registry().lock().unwrap();
    m.entry(k).or_insert_with(|| v.to_vec());
    k
}

/// Key for an ID only compared for equality.
#[inline(always)]
pub fn pack_noreg(buf: &[u8], s: usize, e: usize) -> u64 {
    match pack_exact(buf, s, e) {
        Some(k) => k,
        None => FB | (hash_bytes(&buf[s..e]) >> 1),
    }
}

pub fn decode(k: u64, out: &mut Vec<u8>) {
    if k & FB == 0 {
        for i in 0..9 {
            let c = (k >> (56 - 7 * i)) & 0x7F;
            if c == 0 {
                break;
            }
            out.push(c as u8);
        }
    } else {
        let m = registry().lock().unwrap();
        out.extend_from_slice(m.get(&k).expect("fallback key registered"));
    }
}

#[inline(always)]
pub fn key_cmp(a: u64, b: u64) -> Ordering {
    if (a | b) & FB == 0 {
        return a.cmp(&b);
    }
    let mut x = Vec::new();
    let mut y = Vec::new();
    decode(a, &mut x);
    decode(b, &mut y);
    x.cmp(&y)
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
fn parse8(v: u64) -> u64 {
    let v = v.wrapping_sub(0x3030_3030_3030_3030);
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    ((v & 0x0000_00FF_0000_00FF)
        .wrapping_mul(100 + (1_000_000u64 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32
}

/// True when every lane selected by `m` holds an ASCII digit.
#[inline(always)]
fn digits_ok(v: u64, m: u64) -> bool {
    let hi = m & 0xF0F0_F0F0_F0F0_F0F0;
    let lo = m & 0x0F0F_0F0F_0F0F_0F0F;
    (v & hi) == (0x3030_3030_3030_3030 & m) && (((v & lo).wrapping_add(0x0606_0606_0606_0606 & m)) & hi) == 0
}

const MA: u64 = lane_mask(&[0, 1, 2, 3, 5, 6]);
const MB: u64 = lane_mask(&[0, 1, 3, 4, 6, 7]);
const MC: u64 = lane_mask(&[1, 2]);
const SAM: u64 = lane_mask(&[4, 7]);
const SBM: u64 = lane_mask(&[2, 5]);
const SCM: u64 = lane_mask(&[0, 3]);
const SA: u64 = (0x2Du64 << 32) | (0x2Du64 << 56);
const SB: u64 = (0x54u64 << 16) | (0x3Au64 << 40);
const SC: u64 = 0x3A | (0x5Au64 << 24);

/// F1 `YYYY-MM-DDTHH:MM:SSZ` at buf[s..s+20].
#[inline(always)]
fn f1_at(buf: &[u8], s: usize) -> Option<i64> {
    let a = rd64(buf, s);
    let b = rd64(buf, s + 8);
    let c = rd32(buf, s + 16) as u64;
    let ok = ((a & SAM) == SA) & ((b & SBM) == SB) & ((c & SCM) == SC) & digits_ok(a, MA) & digits_ok(b, MB) & digits_ok(c, MC);
    if !ok {
        return None;
    }
    let da = a & 0x0F0F_0F0F_0F0F_0F0F;
    let ta = da.wrapping_mul(10).wrapping_add(da >> 8);
    let db = b & 0x0F0F_0F0F_0F0F_0F0F;
    let tb = db.wrapping_mul(10).wrapping_add(db >> 8);
    let y = ((ta & 0xFF) * 100 + ((ta >> 16) & 0xFF)) as i64;
    let mo = ((ta >> 40) & 0xFF) as i64;
    let d = (tb & 0xFF) as i64;
    let h = ((tb >> 24) & 0xFF) as i64;
    let mi = ((tb >> 48) & 0xFF) as i64;
    let se = (((c >> 8) & 0x0F) * 10 + ((c >> 16) & 0x0F)) as i64;
    if (h > 23) | (mi > 59) | (se > 59) | (mo > 12) | (d > 31) {
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
pub fn ts_at(buf: &[u8], s: usize, e: usize) -> Option<i64> {
    let len = e - s;
    if len == 20 && buf[s + 19] == b'Z' {
        return f1_at(buf, s);
    }
    if len >= 1 && len <= 16 && s + 16 <= buf.len() && buf[s].wrapping_sub(b'0') < 10 {
        return f3_at(buf, s, e);
    }
    parse_ts(&buf[s..e])
}

/// ingest_ts (always F1).
#[inline(always)]
pub fn f1_field(buf: &[u8], s: usize, e: usize) -> Option<i64> {
    if e - s == 20 && buf[s + 19] == b'Z' {
        f1_at(buf, s)
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
        for _ in 0..3_000_000 {
            let s = candidates(&mut r);
            // embed with random tails so over-reads see junk
            let mut buf = s.clone();
            let tail = r.below(20) as usize;
            for _ in 0..tail {
                buf.push(b"0123456789Z,\n"[r.below(13) as usize]);
            }
            let fast = ts_at(&buf, 0, s.len());
            let slow = parse_ts(&s);
            assert_eq!(fast, slow, "{:?}", String::from_utf8_lossy(&s));
            if s.len() == 20 {
                assert_eq!(f1_field(&buf, 0, 20), if s[19] == b'Z' { parse_ts(&s) } else { None });
            }
        }
    }

    #[test]
    fn keys_order_and_roundtrip() {
        let mut r = Rng(0xfeed_beef_1234_5678);
        let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyzABCZ_ \t\r\"\x01\x7f\xc3\xa9";
        let mut prev: Option<(Vec<u8>, u64)> = None;
        for _ in 0..2_000_000 {
            let len = r.below(12) as usize;
            let s: Vec<u8> = (0..len).map(|_| alphabet[r.below(alphabet.len() as u64) as usize]).collect();
            let mut buf = s.clone();
            for _ in 0..r.below(12) {
                buf.push(b'x');
            }
            let k = pack_reg(&buf, 0, s.len());
            let mut d = Vec::new();
            decode(k, &mut d);
            assert_eq!(d, s);
            if s.is_empty() {
                assert_eq!(k, 0);
            }
            if let Some((ps, pk)) = &prev {
                assert_eq!(key_cmp(*pk, k), ps.cmp(&s), "{:?} {:?}", ps, s);
            }
            prev = Some((s, k));
        }
    }
}
