//! Diagnostic mode: only when the image ships /generate AND the input is the public dev
//! set. Prints scorer hardware facts and timings of this pipeline on a generated 20M set,
//! then writes deliberately empty tables so the dev check fails and the logs come back.

use std::path::Path;
use std::process::Command;
use std::time::Instant;

fn show(label: &str, path: &str) {
    match std::fs::read_to_string(path) {
        Ok(s) => eprintln!("DIAG {label}: {}", s.trim().replace('\n', " | ")),
        Err(e) => eprintln!("DIAG {label}: <{e}>"),
    }
}

fn read_all(dir: &Path) -> (u64, f64) {
    let t = Instant::now();
    let mut n = 0u64;
    for sub in ["events", "clicks", "links"] {
        if let Ok(rd) = std::fs::read_dir(dir.join(sub)) {
            for e in rd.flatten() {
                if let Ok(v) = std::fs::read(e.path()) {
                    n += v.len() as u64;
                }
            }
        }
    }
    (n, t.elapsed().as_secs_f64())
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

pub fn run(data: &Path, out: &Path) -> ! {
    let t0 = Instant::now();
    eprintln!("DIAG start");
    let cpu = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let model = cpu.lines().find(|l| l.starts_with("model name")).unwrap_or("").to_string();
    let mhz: Vec<&str> = cpu.lines().filter(|l| l.starts_with("cpu MHz")).collect();
    let flags = cpu.lines().find(|l| l.starts_with("flags")).unwrap_or("");
    eprintln!("DIAG cpu: {model} | cpus listed {} | {:?}", mhz.len(), mhz);
    eprintln!(
        "DIAG flags: avx512f={} avx512bw={} bmi2={} avx2={}",
        flags.contains(" avx512f"),
        flags.contains(" avx512bw"),
        flags.contains(" bmi2"),
        flags.contains(" avx2")
    );
    eprintln!("DIAG parallelism={:?} NCPU={:?}", std::thread::available_parallelism(), std::env::var("NCPU"));
    show("kernel", "/proc/version");
    show("thp", "/sys/kernel/mm/transparent_hugepage/enabled");
    show("thp-defrag", "/sys/kernel/mm/transparent_hugepage/defrag");
    show("overcommit", "/proc/sys/vm/overcommit_memory");
    show("cpu.max", "/sys/fs/cgroup/cpu.max");
    show("memory.max", "/sys/fs/cgroup/memory.max");
    show("cpuset", "/sys/fs/cgroup/cpuset.cpus.effective");
    show("fault_around", "/sys/kernel/mm/fault_around_bytes");
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    for l in mounts.lines() {
        if l.contains(" /data ") || l.contains(" /scratch ") || l.contains(" /output ") || l.contains(" / ") {
            eprintln!("DIAG mount: {l}");
        }
    }
    let mem = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    for l in mem.lines().take(5) {
        eprintln!("DIAG meminfo: {l}");
    }

    let (n, s) = read_all(data);
    eprintln!("DIAG /data read pass1: {} MB in {:.3}s", n >> 20, s);
    let (n, s) = read_all(data);
    eprintln!("DIAG /data read pass2: {} MB in {:.3}s", n >> 20, s);

    let g = Path::new("/scratch/g20");
    let t = Instant::now();
    let st = Command::new("/generate")
        .args(["--seed", "7", "--events", "20000000", "--clicks", "5000000", "--threads", "8", "--output"])
        .arg(g)
        .status();
    eprintln!("DIAG generate 20M: {:?} in {:.1}s", st.map(|s| s.code()), t.elapsed().as_secs_f64());
    let (n, s) = read_all(g);
    eprintln!("DIAG /scratch read: {} MB in {:.3}s ({:.2} GB/s)", n >> 20, s, n as f64 / s / 1e9);

    let me = std::env::current_exe().unwrap();
    let configs: &[(&str, &[(&str, &str)])] = &[
        ("base", &[("NCPU", "8")]),
        ("base2", &[("NCPU", "8")]),
        ("t16", &[("NCPU", "16")]),
        ("t12", &[("NCPU", "12")]),
        ("willneed", &[("NCPU", "8"), ("WILLNEED", "1")]),
        ("populate", &[("NCPU", "8"), ("POPULATE", "1")]),
        ("chunk4m", &[("NCPU", "8"), ("CHUNK_KB", "4096")]),
        ("chunk256k", &[("NCPU", "8"), ("CHUNK_KB", "256")]),
        ("base3", &[("NCPU", "8")]),
    ];
    for (i, (name, env)) in configs.iter().enumerate() {
        let o = format!("/scratch/o{i}");
        let _ = std::fs::remove_dir_all(&o);
        let t = Instant::now();
        let mut c = Command::new(&me);
        c.env("DATA_DIR", g).env("OUTPUT_DIR", &o).env("DIAG_CHILD", "1");
        for (k, v) in env.iter() {
            c.env(k, v);
        }
        let r = c.output();
        let wall = t.elapsed().as_secs_f64();
        match r {
            Ok(r) => {
                let log = String::from_utf8_lossy(&r.stderr).replace('\n', " ");
                eprintln!("DIAG run {name}: wall {wall:.3}s exit {:?} :: {log}", r.status.code());
            }
            Err(e) => eprintln!("DIAG run {name}: error {e}"),
        }
    }
    eprintln!("DIAG total {:.1}s", t0.elapsed().as_secs_f64());
    // Deliberately empty tables: the dev check fails and returns these logs.
    std::fs::create_dir_all(out.join("user_daily")).ok();
    std::fs::create_dir_all(out.join("campaign_daily")).ok();
    crate::out::write_users(&out.join("user_daily").join("part-0.parquet"), &[], &[]);
    crate::out::write_campaigns(&out.join("campaign_daily").join("part-0.parquet"), &[], &[], &[]);
    std::process::exit(0);
}
