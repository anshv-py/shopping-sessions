//! A never-freeing bump allocator over one huge anonymous mapping with transparent huge
//! pages. The process is short-lived and frees nothing that matters, so: no free lists,
//! no zeroing (fresh mmap memory is already zero), in-place growth of the newest block,
//! and ~512x fewer page faults than 4 KiB pages.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Bump;

const REGION: usize = 40 << 30;
const SLAB: usize = 8 << 20;
const BIG: usize = 1 << 20;

static BASE: AtomicUsize = AtomicUsize::new(0); // 0 = uninitialised, 1 = failed
static NEXT: AtomicUsize = AtomicUsize::new(0);
static END: AtomicUsize = AtomicUsize::new(0);
static POPULATE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

thread_local! {
    static CUR: Cell<usize> = const { Cell::new(0) };
    static LIM: Cell<usize> = const { Cell::new(0) };
}

#[cold]
fn init() -> bool {
    loop {
        match BASE.load(Ordering::Acquire) {
            0 => {}
            1 => return false,
            2 => {
                std::hint::spin_loop();
                continue;
            }
            _ => return true,
        }
        if BASE.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire).is_err() {
            continue;
        }
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                REGION,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if p == libc::MAP_FAILED {
            BASE.store(1, Ordering::Release);
            return false;
        }
        // getenv does not allocate (std::env would re-enter this allocator).
        if !unsafe { libc::getenv(c"NOPOP".as_ptr()) }.is_null() {
            POPULATE.store(false, Ordering::Relaxed);
        }
        if unsafe { libc::getenv(c"NOHUGE".as_ptr()) }.is_null() {
            unsafe {
                libc::madvise(p, REGION, libc::MADV_HUGEPAGE);
            }
        }
        let start = (p as usize + (2 << 20) - 1) & !((2 << 20) - 1);
        NEXT.store(start, Ordering::Relaxed);
        END.store(p as usize + REGION, Ordering::Relaxed);
        BASE.store(p as usize, Ordering::Release);
        return true;
    }
}

#[inline(always)]
fn ready() -> bool {
    let b = BASE.load(Ordering::Acquire);
    b > 2 || (b == 0 || b == 2) && init()
}

#[inline(always)]
fn global(size: usize, align: usize) -> *mut u8 {
    let align = align.max(64);
    loop {
        let cur = NEXT.load(Ordering::Relaxed);
        let start = (cur + align - 1) & !(align - 1);
        let end = start + size;
        if end > END.load(Ordering::Relaxed) {
            return std::ptr::null_mut();
        }
        if NEXT.compare_exchange_weak(cur, end, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            return start as *mut u8;
        }
    }
}

#[inline(always)]
fn local(size: usize, align: usize) -> *mut u8 {
    let cur = CUR.with(|c| c.get());
    let lim = LIM.with(|c| c.get());
    let start = (cur + align - 1) & !(align - 1);
    if cur != 0 && start + size <= lim {
        CUR.with(|c| c.set(start + size));
        return start as *mut u8;
    }
    let slab = global(SLAB, 2 << 20);
    if slab.is_null() {
        return slab;
    }
    // Fault the whole slab in with one call instead of one trap per page (slabs fill up;
    // MADV_POPULATE_WRITE = 23, Linux 5.14+; ignored where unsupported).
    if POPULATE.load(Ordering::Relaxed) {
        unsafe { libc::madvise(slab as *mut libc::c_void, SLAB, 23) };
    }
    let s = slab as usize;
    let start = (s + align - 1) & !(align - 1);
    CUR.with(|c| c.set(start + size));
    LIM.with(|c| c.set(s + SLAB));
    start as *mut u8
}

#[inline(always)]
fn in_region(p: *mut u8) -> bool {
    let b = BASE.load(Ordering::Relaxed);
    b > 2 && (p as usize) >= b && (p as usize) < END.load(Ordering::Relaxed)
}

unsafe impl GlobalAlloc for Bump {
    #[inline(always)]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if ready() {
            let p = if l.size() >= BIG { global(l.size(), l.align()) } else { local(l.size(), l.align()) };
            if !p.is_null() {
                return p;
            }
        }
        System.alloc(l)
    }

    #[inline(always)]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if ready() {
            let p = if l.size() >= BIG { global(l.size(), l.align()) } else { local(l.size(), l.align()) };
            if !p.is_null() {
                return p; // never handed out before: still zero
            }
        }
        System.alloc_zeroed(l)
    }

    #[inline(always)]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if !in_region(p) {
            System.dealloc(p, l)
        }
    }

    #[inline(always)]
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        if !in_region(p) {
            return System.realloc(p, l, new_size);
        }
        let old_end = p as usize + l.size();
        if new_size <= l.size() {
            return p;
        }
        // Newest block of this thread's slab: grow in place.
        let cur = CUR.with(|c| c.get());
        if cur == old_end && p as usize + new_size <= LIM.with(|c| c.get()) {
            CUR.with(|c| c.set(p as usize + new_size));
            return p;
        }
        // Newest block of the global region: grow in place.
        if old_end == NEXT.load(Ordering::Relaxed)
            && p as usize + new_size <= END.load(Ordering::Relaxed)
            && NEXT
                .compare_exchange(old_end, p as usize + new_size, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            return p;
        }
        let q = self.alloc(Layout::from_size_align_unchecked(new_size, l.align()));
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size());
        }
        q
    }
}

/// Bytes handed out from the region so far (slabs count in full).
pub fn used() -> usize {
    let b = BASE.load(Ordering::Relaxed);
    if b > 2 { NEXT.load(Ordering::Relaxed) - b } else { 0 }
}
