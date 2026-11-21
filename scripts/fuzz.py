#!/usr/bin/env python3
"""Differential fuzzing: mutate fixture inputs, run the binary and reference.py, compare.

Usage: fuzz.py BINARY ITERATIONS
"""
import os
import random
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = os.path.join(HERE, "..", "fixtures")

JUNK = ['"', ",", "\r", " ", "", "x" * 150, '""', "Z", "T", "0", "-", "+", ":", "u1", "U1", "c_1", "p1", "purchase", "view"]


def mutate_line(r, line):
    ops = r.randint(1, 3)
    for _ in range(ops):
        k = r.randrange(5)
        if k == 0 and line:
            i = r.randrange(len(line) + 1)
            line = line[:i] + r.choice(JUNK) + line[i:]
        elif k == 1 and line:
            i = r.randrange(len(line))
            j = min(len(line), i + r.randint(1, 4))
            line = line[:i] + line[j:]
        elif k == 2:
            f = line.split(",")
            i = r.randrange(len(f))
            f[i] = '"' + f[i] + '"'
            line = ",".join(f)
        elif k == 3:
            f = line.split(",")
            i = r.randrange(len(f))
            f[i] = f[i] + "y" * r.randint(100, 300)
            line = ",".join(f)
        else:
            line = line  # keep
    return line


def retag(line, tag):
    """Appends `tag` to the first field (inside its quotes, if quoted)."""
    i = line.find(",")
    if i < 0:
        return line
    head = line[:i]
    if len(head) >= 2 and head[0] == '"' and head[-1] == '"':
        head = head[:-1] + tag + '"'
    elif head != "":
        head = head + tag
    return head + line[i:]


def build_case(r, out):
    cases = [d for d in os.listdir(FIX) if os.path.isdir(os.path.join(FIX, d))]
    picks = r.sample(cases, 3)
    base = os.path.join(FIX, picks[0], "input")
    shutil.copytree(base, out)
    for sub in ("events", "clicks", "links"):
        rows = []
        for ci, c in enumerate(picks):
            d = os.path.join(FIX, c, "input", sub)
            for f in sorted(os.listdir(d)):
                with open(os.path.join(d, f), "rb") as fh:
                    got = fh.read().decode("utf-8", "surrogateescape").split("\n")[1:]
                    if sub != "links":
                        got = [retag(x, f"_{ci}") for x in got]
                    rows += got
        rows = [x for x in rows if x != ""] + ([""] if r.random() < 0.3 else [])
        out_rows = []
        for x in rows:
            if r.random() < 0.25:
                if sub != "links":
                    x = retag(x, f"_m{r.randrange(10**9)}")
                x = mutate_line(r, x)
            out_rows.append(x)
            if r.random() < 0.1:
                out_rows.append(x)  # duplicate line
        r.shuffle(out_rows)
        d = os.path.join(out, sub)
        for f in os.listdir(d):
            os.remove(os.path.join(d, f))
        header = {"events": "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
                  "clicks": "click_id,user_id,campaign_id,ts", "links": "user_id_a,user_id_b"}[sub]
        cut = r.randint(0, len(out_rows))
        for i, part in enumerate((out_rows[:cut], out_rows[cut:])):
            text = header + "\n" + "\n".join(part) + ("\n" if part and r.random() < 0.8 else "")
            with open(os.path.join(d, f"part-{i:05d}.csv"), "wb") as fh:
                fh.write(text.encode("utf-8", "surrogateescape"))


def main():
    binary, iters = sys.argv[1], int(sys.argv[2])
    r = random.Random(int(sys.argv[3]) if len(sys.argv) > 3 else 1)
    fails = 0
    for it in range(iters):
        tmp = tempfile.mkdtemp()
        data = os.path.join(tmp, "data")
        build_case(r, data)
        o1, o2 = os.path.join(tmp, "ours"), os.path.join(tmp, "ref")
        env = dict(os.environ, DATA_DIR=data, OUTPUT_DIR=o1, CHUNK_KB=str(r.choice([1, 2, 1024])))
        p = subprocess.run([binary], env=env, capture_output=True, text=True)
        subprocess.run([sys.executable, os.path.join(HERE, "reference.py"), data, o2], check=True)
        c = subprocess.run([sys.executable, os.path.join(HERE, "compare.py"), o1, o2], capture_output=True, text=True)
        if p.returncode != 0 or "MATCH" not in c.stdout.splitlines()[-1]:
            fails += 1
            keep = os.path.join(HERE, "..", f"fuzzfail-{it}")
            shutil.copytree(data, keep, dirs_exist_ok=True)
            print(f"FAIL iteration {it} (kept {keep})\n{p.stderr[-500:]}\n{c.stdout[-1500:]}")
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"{iters - fails}/{iters} fuzz cases matched")


if __name__ == "__main__":
    main()
