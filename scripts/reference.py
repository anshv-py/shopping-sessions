#!/usr/bin/env python3
"""Slow, independent reference implementation of rules.md, for cross-checking on any seed.

Usage: reference.py DATA_DIR OUT_DIR   (writes user_daily.csv and campaign_daily.csv)
"""
import calendar
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000


def split(line, n):
    """Fields of a well-formed line (quotes stripped) or None."""
    if line == "":
        return None
    parts = line.split(",")
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if '"' in p:
            if len(p) >= 2 and p[0] == '"' and p[-1] == '"' and '"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p)
    return out


def lines(path):
    with open(path, "rb") as f:
        data = f.read().decode("utf-8", errors="surrogateescape")
    ls = data.split("\n")
    if data.endswith("\n"):
        ls = ls[:-1]
    return ls[1:]  # header


def csvs(folder):
    if not os.path.isdir(folder):
        return []
    return sorted(os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv"))


def isdigits(s):
    return s != "" and all("0" <= c <= "9" for c in s)


def civil_ms(s):
    # YYYY-MM-DDTHH:MM:SS
    if len(s) != 19 or s[4] != "-" or s[7] != "-" or s[10] != "T" or s[13] != ":" or s[16] != ":":
        return None
    parts = [s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19]]
    if not all(isdigits(p) for p in parts):
        return None
    y, mo, d, h, mi, se = map(int, parts)
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
        return None
    if y < 1:
        return None
    if not (1 <= d <= calendar.monthrange(y, mo)[1]):
        return None
    dt = datetime(y, mo, d, h, mi, se, tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1000


def parse_ts(s):
    if s == "":
        return None
    if isdigits(s):
        v = int(s)
        return v if v < 2**63 else None
    if len(s) == 20 and s[19] == "Z":
        return civil_ms(s[:19])
    if len(s) == 25 and s[19] in "+-" and s[22] == ":":
        oh, om = s[20:22], s[23:25]
        if not (isdigits(oh) and isdigits(om)):
            return None
        oh, om = int(oh), int(om)
        if oh > 23 or om > 59:
            return None
        t = civil_ms(s[:19])
        if t is None:
            return None
        off = (oh * 60 + om) * 60_000
        return t - off if s[19] == "+" else t + off
    return None


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws = parse_ts(man["window_start"])
    we = parse_ts(man["window_end"])

    products = {}
    for l in lines(os.path.join(data, "products.csv")):
        f = split(l, 2)
        if f:
            products[f[0]] = f[1]
    channels = {}
    for l in lines(os.path.join(data, "campaigns.csv")):
        f = split(l, 2)
        if f and f[0] not in channels:
            channels[f[0]] = f[1]

    parent = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for path in csvs(os.path.join(data, "links")):
        for l in lines(path):
            f = split(l, 2)
            if not f or f[0] == "" or f[1] == "":
                continue
            a, b = find(f[0]), find(f[1])
            if a != b:
                parent[a] = b
    name = {}
    for x in list(parent):
        r = find(x)
        if r not in name or x.encode("utf-8", "surrogateescape") < name[r].encode("utf-8", "surrogateescape"):
            name[r] = x

    def person(u):
        return name[find(u)] if u in parent else u

    best = {}
    for path in csvs(os.path.join(data, "events")):
        for l in lines(path):
            f = split(l, 7)
            if not f or f[0] == "":
                continue
            ing = parse_ts(f[3])
            if ing is None:
                ing = 2**62
            cur = best.get(f[0])
            if cur is None or ing < cur[0]:
                best[f[0]] = (ing, f)
    events = defaultdict(list)
    for eid, (ing, f) in best.items():
        _, uid, ts, _, ty, prod, price = f
        if uid == "" or prod == "":
            continue
        t = parse_ts(ts)
        if t is None or not (ws <= t < we):
            continue
        if ty not in ("view", "cart", "purchase"):
            continue
        p = 0
        if ty == "purchase":
            if not isdigits(price):
                continue
            p = int(price)
            if not (1 <= p <= 10_000_000):
                continue
        if ing - t > DAY:
            continue
        events[person(uid)].append((t, p, products.get(prod, "unknown")))

    clicks = defaultdict(list)
    seen = set()
    for path in csvs(os.path.join(data, "clicks")):
        for l in lines(path):
            f = split(l, 4)
            if not f or f[0] == "":
                continue
            if f[0] in seen:
                continue
            seen.add(f[0])
            cid, uid, camp, ts = f
            t = parse_ts(ts)
            if uid == "" or t is None or not (ws - WEEK <= t < we) or camp not in channels:
                continue
            clicks[person(uid)].append((t, cid.encode("utf-8", "surrogateescape"), camp))

    ud = {}
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for who, evs in events.items():
        evs.sort(key=lambda e: e[0])
        cl = clicks.get(who, [])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= GAP:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            q = [c for c in cl if start - WEEK <= c[0] <= start]
            if q:
                top = max(c[0] for c in q)
                credited = min(c[1] for c in q if c[0] == top)
                camp = [c[2] for c in q if c[0] == top and c[1] == credited][0]
            else:
                camp = "organic"
            rev = sum(e[1] for e in s)
            purch = any(e[1] > 0 for e in s)
            row = cd[(camp, day)]
            row[0] += 1
            row[1] += 1 if purch else 0
            row[2] += rev
            for a in {c[2] for c in q}:
                cd[(a, day)][3] += 1
            u = ud.setdefault((who, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += len(s)
            u[2] += rev
            for e in s:
                if e[1] > 0:
                    u[3][e[2]] += e[1]

    os.makedirs(out, exist_ok=True)

    def date(d):
        return datetime.fromtimestamp(d * 86400, tz=timezone.utc).strftime("%Y-%m-%d")

    with open(os.path.join(out, "user_daily.csv"), "w", encoding="utf-8", errors="surrogateescape", newline="") as fo:
        fo.write("user_id,day,sessions,events,revenue_cents,top_category\n")
        for (who, day), (n, ne, rev, cats) in ud.items():
            top = ""
            if cats:
                top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0][0]
            fo.write(f"{who},{date(day)},{n},{ne},{rev},{top}\n")
    with open(os.path.join(out, "campaign_daily.csv"), "w", encoding="utf-8", newline="") as fo:
        fo.write("campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
        for (camp, day), (n, pu, rev, ass) in cd.items():
            ch = "organic" if camp == "organic" else channels[camp]
            fo.write(f"{camp},{ch},{date(day)},{n},{pu},{rev},{ass}\n")


if __name__ == "__main__":
    main()
