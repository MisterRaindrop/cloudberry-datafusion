#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""TPC-H with and without vec_executor (docs/benchmarks.md).

usage: bench.py run --db DB --out FILE [--rounds N] [--warmup N]
                    [--configs C,...] [--queries 1-22] [--timeout S]
                    [--set GUC=VALUE ...] [--label TEXT]
       bench.py summary FILE [--baseline FILE] [--threshold F]

run: each query runs in a psql of its own, timed from the client
(connection and output included), under each configuration:

  native-off  the database without the extension, Postgres planner
  native-on   the database without the extension, GPORCA
  vec-off     vec_executor with vec.mode = on, Postgres planner
  vec-on      vec_executor with vec.mode = on, GPORCA

"Without the extension" resets the database's session_preload_libraries,
so that no backend, on the coordinator or the segments, loads it; the
setting the database had is put back at the end, also after an error or
Ctrl-C.  The warm-up rounds are not counted; the order of the
configurations rotates from round to round.  Every run's result rows are
hashed: a query whose rows differ between configurations or rounds is
reported, and its outputs are kept next to FILE.  Each run is a line of
FILE (JSON), after a first line describing the setup, so an interrupted
run keeps what it measured.

summary: per query, the median of each configuration's timed runs; the
totals, speedups, geometric means and smallest speedups; a Markdown
table as in docs/benchmarks.md.  With --baseline, each configuration's
total and each query against an earlier FILE; exits 1 if a total is
slower than the baseline's by more than --threshold (default 0.10).
"""
import argparse, datetime, hashlib, json, math, os, re, signal, socket
import statistics, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIGS = ["native-off", "native-on", "vec-off", "vec-on"]


def psql(db, sql, opts=None, timeout=None):
    env = dict(os.environ)
    if opts:
        env["PGOPTIONS"] = " ".join(f"-c {k}={v}" for k, v in opts.items())
    return subprocess.run(["psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", sql],
                          capture_output=True, text=True, env=env, timeout=timeout)


def scalar(db, sql):
    r = psql(db, sql)
    if r.returncode != 0:
        sys.exit(f"bench.py: {sql}: {r.stderr.strip()}")
    return r.stdout.strip()


def queries(spec):
    out = []
    for part in spec.split(","):
        a, _, b = part.partition("-")
        out += list(range(int(a), int(b or a) + 1))
    return out


def query_text(q):
    with open(os.path.join(HERE, "queries", f"q{q:02d}.sql")) as f:
        return f.read()


class Preload:
    """The database's session_preload_libraries, switched per configuration
    and put back at the end."""

    def __init__(self, db):
        self.db = db
        self.saved = scalar("postgres", f"""
            SELECT coalesce((SELECT option_value FROM pg_db_role_setting s
                             JOIN pg_database d ON d.oid = s.setdatabase,
                             pg_options_to_table(s.setconfig)
                             WHERE d.datname = '{db}' AND s.setrole = 0
                               AND option_name = 'session_preload_libraries'), '')""")
        self.current = None

    def set(self, value):
        if value == self.current:
            return
        if value:
            scalar("postgres", f"ALTER DATABASE \"{self.db}\" SET session_preload_libraries = '{value}'")
        else:
            scalar("postgres", f"ALTER DATABASE \"{self.db}\" RESET session_preload_libraries")
        self.current = value

    def restore(self):
        self.current = None
        self.set(self.saved)


def run(args):
    qs = queries(args.queries)
    configs = args.configs.split(",")
    for c in configs:
        if c not in CONFIGS:
            sys.exit(f"bench.py: unknown configuration {c}; one of {', '.join(CONFIGS)}")
    extra = dict(s.split("=", 1) for s in args.set)
    texts = {q: query_text(q) for q in qs}
    preload = Preload(args.db)
    outdir = os.path.splitext(args.out)[0] + ".outputs"

    def stop(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)

    meta = {
        "meta": True, "label": args.label, "db": args.db, "configs": configs, "queries": qs,
        "rounds": args.rounds, "warmup": args.warmup, "set": extra,
        "host": socket.gethostname(), "cpus": os.cpu_count(),
        "started": datetime.datetime.now().isoformat(timespec="seconds"),
        "version": scalar(args.db, "SELECT version()"),
        "segments": scalar(args.db, "SELECT count(*) FROM gp_segment_configuration "
                                    "WHERE role = 'p' AND content >= 0"),
        "extension": scalar(args.db, "SELECT coalesce((SELECT extversion FROM pg_extension "
                                     "WHERE extname = 'vec_executor'), 'not installed')"),
        "lineitem_rows": scalar(args.db, "SELECT count(*) FROM lineitem"),
        "preload_before": preload.saved,
    }
    hashes = {}
    bad = set()
    failures = 0
    try:
        with open(args.out, "w") as out:
            out.write(json.dumps(meta) + "\n")
            for rnd in range(args.warmup + args.rounds):
                order = configs[rnd % len(configs):] + configs[:rnd % len(configs)]
                for cfg in order:
                    vec = cfg.startswith("vec")
                    preload.set("vec_executor" if vec else "")
                    opts = {"optimizer": cfg.endswith("-on") and "on" or "off",
                            "statement_timeout": f"{args.timeout}s", **extra}
                    if vec:
                        opts["vec.mode"] = "on"
                    for q in qs:
                        t = time.time()
                        try:
                            r = psql(args.db, texts[q], opts, timeout=args.timeout + 60)
                            err = r.stderr.strip() if r.returncode != 0 else ""
                            rows = r.stdout
                        except subprocess.TimeoutExpired:
                            err, rows = "client timeout", ""
                        el = time.time() - t
                        md5 = hashlib.md5(rows.encode()).hexdigest()
                        rec = {"round": rnd, "timed": rnd >= args.warmup, "cfg": cfg, "q": q,
                               "t": round(el, 3), "md5": md5, "err": err[:500],
                               "at": datetime.datetime.now().isoformat(timespec="seconds")}
                        out.write(json.dumps(rec) + "\n")
                        out.flush()
                        if err:
                            failures += 1
                            print(f"round {rnd} {cfg} Q{q}: {err.splitlines()[0]}", flush=True)
                            continue
                        first = hashes.setdefault(q, (md5, cfg, rnd, rows))
                        if md5 != first[0] and q not in bad:
                            bad.add(q)
                            os.makedirs(outdir, exist_ok=True)
                            for c, rn, text in ((first[1], first[2], first[3]), (cfg, rnd, rows)):
                                with open(os.path.join(outdir, f"q{q:02d}-{c}-r{rn}.txt"), "w") as f:
                                    f.write(text)
                            print(f"round {rnd} {cfg} Q{q}: rows differ from {first[1]} round {first[2]} "
                                  f"(both kept in {outdir})", flush=True)
                    print(f"round {rnd}{' (warm-up)' if rnd < args.warmup else ''} {cfg} done "
                          f"{datetime.datetime.now():%H:%M:%S}", flush=True)
    except KeyboardInterrupt:
        print("bench.py: interrupted; the runs so far are in", args.out, flush=True)
    finally:
        preload.restore()
    print(f"bench.py: {failures} failed runs; queries whose rows differ: "
          f"{', '.join(f'Q{q}' for q in sorted(bad)) or 'none'}")
    return 1 if failures or bad else 0


def load(path):
    meta, runs = None, {}
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            if r.get("meta"):
                meta = r
            elif r["timed"] and not r["err"]:
                runs.setdefault((r["cfg"], r["q"]), []).append(r["t"])
    return meta, runs


def medians(runs):
    return {k: statistics.median(v) for k, v in runs.items()}


def summary(args):
    meta, runs = load(args.file)
    med = medians(runs)
    configs = [c for c in CONFIGS if any(k[0] == c for k in med)]
    qs = sorted({k[1] for k in med})
    print(f"# {args.file}: {meta.get('label') or ''} {meta['started']}, {meta['segments']} segments, "
          f"lineitem {meta['lineitem_rows']} rows, {meta['rounds']} timed rounds")
    print(f"# {meta['version'][:100]}")
    total = {c: sum(med.get((c, q), float("nan")) for q in qs) for c in configs}
    pairs = [(p, f"native-{p}", f"vec-{p}") for p in ("off", "on")
             if f"native-{p}" in configs and f"vec-{p}" in configs]
    names = {"off": "Postgres planner", "on": "GPORCA"}
    if pairs:
        print("\n| Planner | Cloudberry | vec_executor | Speedup | Geometric mean | Smallest |")
        print("|---|---:|---:|---:|---:|---:|")
        for p, n, v in pairs:
            sp = [med[(n, q)] / med[(v, q)] for q in qs if (n, q) in med and (v, q) in med]
            geo = math.exp(sum(map(math.log, sp)) / len(sp))
            print(f"| {names[p]} | {total[n]:.1f} s | {total[v]:.1f} s | **{total[n] / total[v]:.1f}x** "
                  f"| {geo:.1f}x | {min(sp):.1f}x |")
    print("\n| Query | " + " | ".join(configs) +
          "".join(f" | Speedup ({names[p]})" for p, _, _ in pairs) + " |")
    print("|---|" + "---:|" * (len(configs) + len(pairs)))
    for q in qs:
        cells = [f"{med[(c, q)]:.2f}" if (c, q) in med else "-" for c in configs]
        cells += [f"{med[(n, q)] / med[(v, q)]:.1f}x" if (n, q) in med and (v, q) in med else "-"
                  for _, n, v in pairs]
        print(f"| Q{q} | " + " | ".join(cells) + " |")
    cells = [f"**{total[c]:.2f}**" for c in configs] + \
        [f"**{total[n] / total[v]:.1f}x**" for _, n, v in pairs]
    print("| **Total** | " + " | ".join(cells) + " |")
    noisy = []
    for (c, q), ts in sorted(runs.items()):
        if len(ts) > 1 and (max(ts) - min(ts)) / statistics.median(ts) > 0.10:
            noisy.append(f"{c} Q{q} {(max(ts) - min(ts)) / statistics.median(ts):.0%}")
    if noisy:
        print(f"\nVaried by more than 10% of the median: {', '.join(noisy)}")
    if not args.baseline:
        return 0
    bmeta, bruns = load(args.baseline)
    bmed = medians(bruns)
    print(f"\n## Against {args.baseline} ({bmeta.get('label') or ''} {bmeta['started']})\n")
    print("| Configuration | Baseline | Now | Change |")
    print("|---|---:|---:|---:|")
    worse = False
    for c in configs:
        common = [q for q in qs if (c, q) in med and (c, q) in bmed]
        if not common:
            continue
        b = sum(bmed[(c, q)] for q in common)
        n = sum(med[(c, q)] for q in common)
        change = n / b - 1
        flag = " (slower)" if change > args.threshold else ""
        worse |= change > args.threshold
        print(f"| {c} | {b:.1f} s | {n:.1f} s | {change:+.1%}{flag} |")
    slow = [f"{c} Q{q} {med[(c, q)] / bmed[(c, q)] - 1:+.0%}"
            for c in configs for q in qs
            if (c, q) in med and (c, q) in bmed and med[(c, q)] > 0.5
            and med[(c, q)] / bmed[(c, q)] - 1 > args.threshold]
    if slow:
        print(f"\nQueries slower by more than {args.threshold:.0%} (of those over 0.5 s): {', '.join(slow)}")
    return 1 if worse else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--db", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--rounds", type=int, default=5)
    r.add_argument("--warmup", type=int, default=1)
    r.add_argument("--configs", default=",".join(CONFIGS))
    r.add_argument("--queries", default="1-22")
    r.add_argument("--timeout", type=int, default=1800, help="seconds per query")
    r.add_argument("--set", action="append", default=[], metavar="GUC=VALUE")
    r.add_argument("--label", default="")
    s = sub.add_parser("summary")
    s.add_argument("file")
    s.add_argument("--baseline")
    s.add_argument("--threshold", type=float, default=0.10)
    args = ap.parse_args()
    sys.exit(run(args) if args.cmd == "run" else summary(args))


if __name__ == "__main__":
    main()
