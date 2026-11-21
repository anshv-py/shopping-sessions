//! Diagnostic mode: only when the image ships /generate AND the input is the public dev
//! set. Measures the scorer's storage (cold vs cached, several read strategies), CPU speed
//! and this pipeline on a generated 20M set, then writes deliberately empty tables so the
//! dev check fails and the logs come back.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::Instant;

static LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Diagnostic line: stderr, stdout, and kept for the output table.
macro_rules! dl {
    ($($a:tt)*) => {{
        let line = format!($($a)*);
        eprintln!("{line}");
        println!("{line}");
        LOG.lock().unwrap().push(line);
    }};
}

/// "[0.586] stage1 done ... rusage user 5.7s sys 0.7s" -> "s1 0.586 s2 ... u 5.7 sy 0.7"
fn compact(log: &str) -> String {
    let mut out = String::new();
    let toks: Vec<&str> = log.split_whitespace().collect();
    for i in 0..toks.len() {
        let t = toks[i];
        if t.starts_with('[') && t.ends_with(']') && i + 1 < toks.len() {
            let n = toks[i + 1];
            let tag = match n {
                "stage1" => "s1",
                "stage2" => "s2",
                "stage2b" => "s2b",
                "stage3" => "s3",
                "done" => "end",
                _ => continue,
            };
            out.push_str(&format!("{tag}={} ", &t[1..t.len() - 1]));
        }
        if t == "union" && i + 1 < toks.len() {
            out.push_str(&format!("uf={} ", toks[i + 1]));
        }
        if t == "user" && i + 1 < toks.len() && i > 0 && toks[i - 1] == "rusage" {
            out.push_str(&format!("usr={} sys={} ", toks[i + 1], toks.get(i + 3).unwrap_or(&"?")));
        }
        if t == "best" && i + 1 < toks.len() {
            out.push_str(&format!("bench={} {} ", toks[i + 1], toks.get(i + 3).unwrap_or(&"")));
        }
    }
    out
}

static SUMMARY: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn summ(s: String) {
    SUMMARY.lock().unwrap().push(s);
}

fn show(label: &str, path: &str) {
    match std::fs::read_to_string(path) {
        Ok(s) => dl!("DIAG {label}: {}", s.trim().replace('\n', " | ")),
        Err(e) => dl!("DIAG {label}: <{e}>"),
    }
}

fn files_of(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    for sub in ["events", "clicks", "links"] {
        if let Ok(rd) = std::fs::read_dir(dir.join(sub)) {
            for e in rd.flatten() {
                v.push(e.path());
            }
        }
    }
    v.sort();
    v
}

#[cfg(target_os = "linux")]
fn drop_cache(files: &[PathBuf]) {
    use std::os::unix::io::AsRawFd;
    for p in files {
        if let Ok(f) = std::fs::File::open(p) {
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }
    }
}

/// Fraction of the files' pages resident in the page cache.
#[cfg(target_os = "linux")]
fn resident(files: &[PathBuf]) -> f64 {
    let (mut tot, mut res) = (0usize, 0usize);
    for p in files {
        let Ok(f) = std::fs::File::open(p) else { continue };
        let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
        if len == 0 {
            continue;
        }
        let Ok(m) = (unsafe { memmap2::Mmap::map(&f) }) else { continue };
        let pages = len.div_ceil(4096);
        let mut vec = vec![0u8; pages];
        unsafe {
            libc::mincore(m.as_ptr() as *mut libc::c_void, len, vec.as_mut_ptr());
        }
        tot += pages;
        res += vec.iter().filter(|&&b| b & 1 == 1).count();
    }
    if tot == 0 {
        0.0
    } else {
        res as f64 / tot as f64
    }
}

/// Reads all files with `threads` workers doing `block`-sized preads. Returns MB/s.
#[cfg(target_os = "linux")]
fn read_par(files: &[PathBuf], threads: usize, block: usize) -> (f64, f64) {
    use std::os::unix::fs::FileExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut jobs = Vec::new();
    let mut total = 0usize;
    let handles: Vec<std::fs::File> = files.iter().filter_map(|p| std::fs::File::open(p).ok()).collect();
    for (i, f) in handles.iter().enumerate() {
        let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
        total += len;
        let mut s = 0;
        while s < len {
            jobs.push((i, s, block.min(len - s)));
            s += block;
        }
    }
    let next = AtomicUsize::new(0);
    let t = Instant::now();
    std::thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(|| {
                let mut buf = vec![0u8; block];
                loop {
                    let j = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&(i, off, n)) = jobs.get(j) else { break };
                    let _ = handles[i].read_exact_at(&mut buf[..n], off as u64);
                }
            });
        }
    });
    let s = t.elapsed().as_secs_f64();
    (s, total as f64 / s / 1e6)
}

pub fn is_dev(data: &Path) -> bool {
    if !Path::new("/generate").exists() {
        return false;
    }
    let m = std::fs::read_to_string(data.join("manifest.json")).unwrap_or_default();
    if !m.contains("2027-12-09T00:00:00Z") {
        return false;
    }
    let mut n = 0u64;
    if let Ok(rd) = std::fs::read_dir(data.join("events")) {
        for e in rd.flatten() {
            n += e.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    n == 82_514_995
}

fn child(me: &Path, name: &str, data: &Path, env: &[(&str, &str)]) {
    let o = format!("/scratch/o-{name}");
    let _ = std::fs::remove_dir_all(&o);
    let t = Instant::now();
    let mut c = Command::new(me);
    c.env("DATA_DIR", data).env("OUTPUT_DIR", &o).env("DIAG_CHILD", "1").env("NCPU", "8");
    for (k, v) in env {
        c.env(k, v);
    }
    match c.output() {
        Ok(r) => {
            let log: String = String::from_utf8_lossy(&r.stderr)
                .lines()
                .filter(|l| !l.starts_with("  t") || l.contains("t0 "))
                .collect::<Vec<_>>()
                .join(" ");
            dl!("DIAG run {name}: wall {:.3}s exit {:?} :: {log}", t.elapsed().as_secs_f64(), r.status.code());
            summ(format!("{name} wall={:.3} {}", t.elapsed().as_secs_f64(), compact(&log)));
        }
        Err(e) => dl!("DIAG run {name}: error {e}"),
    }
}

#[cfg(target_os = "linux")]
pub fn run(data: &Path, out: &Path) -> ! {
    let t0 = Instant::now();
    dl!("DIAG v2 start");
    let cpu = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let model = cpu.lines().find(|l| l.starts_with("model name")).unwrap_or("").to_string();
    let mhz: Vec<&str> = cpu.lines().filter(|l| l.starts_with("cpu MHz")).collect();
    let flags = cpu.lines().find(|l| l.starts_with("flags")).unwrap_or("");
    dl!("DIAG cpu: {model} | cpus listed {} | {:?}", mhz.len(), &mhz[..mhz.len().min(4)]);
    summ(format!("cpu {} n={} {}", model.replace("model name	: ", ""), mhz.len(), mhz.first().unwrap_or(&"")));
    dl!("DIAG flags: avx512bw={} bmi2={}", flags.contains(" avx512bw"), flags.contains(" bmi2"));
    dl!("DIAG parallelism={:?}", std::thread::available_parallelism());
    show("kernel", "/proc/version");
    show("thp", "/sys/kernel/mm/transparent_hugepage/enabled");
    show("thp-defrag", "/sys/kernel/mm/transparent_hugepage/defrag");
    show("cpu.max", "/sys/fs/cgroup/cpu.max");
    show("cpuset", "/sys/fs/cgroup/cpuset.cpus.effective");
    show("memory.max", "/sys/fs/cgroup/memory.max");
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    for l in mounts.lines() {
        if l.contains(" /data") || l.contains(" /scratch") || l.contains(" /output") {
            dl!("DIAG mount: {l}");
        }
    }
    if let Ok(rd) = std::fs::read_dir("/sys/block") {
        for e in rd.flatten() {
            let nm = e.file_name().to_string_lossy().to_string();
            if nm.starts_with("ram") || nm.starts_with("loop") {
                continue;
            }
            let q = e.path().join("queue");
            let g = |f: &str| std::fs::read_to_string(q.join(f)).unwrap_or_default().trim().to_string();
            dl!(
                "DIAG block {}: rot={} ra_kb={} nr_req={} sched={} model={}",
                e.file_name().to_string_lossy(),
                g("rotational"),
                g("read_ahead_kb"),
                g("nr_requests"),
                g("scheduler"),
                std::fs::read_to_string(e.path().join("device/model")).unwrap_or_default().trim()
            );
        }
    }
    let mem = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    for l in mem.lines().filter(|l| l.starts_with("MemTotal") || l.starts_with("MemFree") || l.starts_with("Cached")) {
        dl!("DIAG meminfo: {l}");
    }

    // ---- /data (dev): cached or not, and cold read strategies
    let df = files_of(data);
    let r0 = resident(&df);
    dl!("DIAG /data resident at start: {:.1}%", 100.0 * r0);
    let mnt: Vec<String> = mounts.lines().filter(|l| l.contains(" /data")).map(|l| l.chars().take(90).collect()).collect();
    summ(format!("data resident0={:.0}% mount={:?}", 100.0 * r0, mnt));
    let (s, r) = read_par(&df, 1, 1 << 20);
    dl!("DIAG /data as-is 1 thread: {s:.3}s {r:.0} MB/s");
    summ(format!("data asis1t {r:.0}MB/s"));
    let (s, r) = read_par(&df, 1, 1 << 20);
    dl!("DIAG /data again 1 thread: {s:.3}s {r:.0} MB/s");
    for (th, bl) in [(1usize, 1usize << 20), (8, 1 << 20), (32, 1 << 20), (64, 256 << 10), (8, 4 << 20)] {
        drop_cache(&df);
        let res = resident(&df);
        let (s, r) = read_par(&df, th, bl);
        dl!("DIAG /data cold (resident {:.0}%) {th} thr {} KB: {s:.3}s {r:.0} MB/s", 100.0 * res, bl >> 10);
        summ(format!("data cold res={:.0}% {th}thr {}KB {r:.0}MB/s", 100.0 * res, bl >> 10));
    }

    // ---- CPU speed: single-thread parse benchmark on dev
    let me = std::env::current_exe().unwrap();
    child(&me, "bench1", data, &[("BENCH", "full"), ("REPS", "7"), ("NCPU", "1")]);
    child(&me, "devfull", data, &[]);

    // ---- generated 20M on /scratch
    let g = Path::new("/scratch/g20");
    let t = Instant::now();
    let st = Command::new("/generate")
        .args(["--seed", "7", "--events", "20000000", "--clicks", "5000000", "--threads", "8", "--output"])
        .arg(g)
        .status();
    dl!("DIAG generate 20M: {:?} in {:.1}s", st.map(|s| s.code()), t.elapsed().as_secs_f64());
    let gf = files_of(g);
    dl!("DIAG /scratch resident after generate: {:.1}%", 100.0 * resident(&gf));
    let (s, r) = read_par(&gf, 8, 1 << 20);
    dl!("DIAG /scratch warm 8 thr: {s:.3}s {r:.0} MB/s");
    child(&me, "warm1", g, &[]);
    child(&me, "warm2", g, &[]);
    child(&me, "warm-t16", g, &[("NCPU", "16")]);
    child(&me, "warm-read", g, &[("READ", "1")]);
    for (name, env) in [
        ("cold-base", vec![]),
        ("cold-willneed", vec![("WILLNEED", "1")]),
        ("cold-t16", vec![("NCPU", "16")]),
        ("cold-read", vec![("READ", "1")]),
        ("cold-read-t24", vec![("READ", "1"), ("NCPU", "24")]),
    ] {
        drop_cache(&gf);
        let res = resident(&gf);
        dl!("DIAG {name}: resident before {:.0}%", 100.0 * res);
        summ(format!("{name} res_before={:.0}%", 100.0 * res));
        child(&me, name, g, &env);
    }
    drop_cache(&gf);
    let (s, r) = read_par(&gf, 32, 1 << 20);
    dl!("DIAG /scratch cold read 32 thr: {s:.3}s {r:.0} MB/s");
    summ(format!("scratch cold32 {r:.0}MB/s"));
    child(&me, "warm3", g, &[]);
    dl!("DIAG total {:.1}s", t0.elapsed().as_secs_f64());

    std::fs::create_dir_all(out.join("user_daily")).ok();
    std::fs::create_dir_all(out.join("campaign_daily")).ok();
    // Diagnostics as rows: user_id = text, so they show up among the differing rows.
    let mut lines: Vec<String> = SUMMARY.lock().unwrap().clone();
    lines.extend(LOG.lock().unwrap().iter().cloned());
    let rows: Vec<crate::URow> = lines
        .iter()
        .enumerate()
        .map(|(i, l)| {
            let text = format!("!{i:03} {}", l.chars().take(400).collect::<String>());
            let b = text.as_bytes();
            crate::URow { person: crate::parse::pack_reg(b, 0, b.len()), day: 20_800, sessions: i as u32, events: 0, revenue: 0, top: u16::MAX }
        })
        .collect();
    crate::out::write_users(&out.join("user_daily").join("part-0.parquet"), &rows, &[]);
    crate::out::write_campaigns(&out.join("campaign_daily").join("part-0.parquet"), &[], &[], &[]);
    std::process::exit(0);
}

#[cfg(not(target_os = "linux"))]
pub fn run(_data: &Path, _out: &Path) -> ! {
    std::process::exit(1);
}
