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
"""
usage: stress.py --db DB --queries DIR [--sessions N] [--minutes M]
                 [--timeouts P] [--kills P] [--seed N]

Concurrency and cancel test.  Each query file in DIR (one statement per
file) is first run once with vec.mode off: that result is the reference.
Then N sessions run the queries in random order with vec.mode on, each
session one long-lived psql, for M minutes.  A fraction P of the queries
(--timeouts) get a random statement_timeout shorter than the query, and a
killer terminates a random running session now and then (--kills: the
chance per query); a terminated session starts a new psql.  Every
completed query must return the reference rows (compared as sorted lines);
a cancelled or terminated one must fail with the matching error.

At the end, with every session gone, the engine must have left nothing
behind: no workfiles in gp_toolkit, no files in pgsql_tmp (of the data
directories on this host) and no backend still busy.  Exit 1 on any wrong result, unexpected error
or leftover.

DB must load vec_executor on every segment (session_preload_libraries).
"""
import argparse, glob, os, queue, random, re, subprocess, sys, threading, time

END = "__stress_end__"


def load(path):
    s = open(path).read()
    s = re.sub(r"^--.*$", "", s, flags=re.M)
    s = re.sub(r";\s*limit -1;", ";", s)
    s = re.sub(r";\s*limit (\d+);", r"\nlimit \1;", s)
    if "drop view" in s:  # TPC-H Q15: its view must exist in DB already
        s = s[s.index("select\n\ts_suppkey"):s.index("drop view")]
    return s.strip().rstrip(";")


def norm(out):
    out = re.sub(r"\s*\(seg\d+ slice\d+ [^)]*\)", "", out)
    return "\n".join(sorted(l for l in out.splitlines() if l.strip()))


class Session:
    """One long-lived psql; run() sends a statement and reads to a marker."""

    def __init__(self, db):
        self.p = subprocess.Popen(
            ["psql", "-X", "-At", "-q", "-v", "ON_ERROR_STOP=0", "-d", db],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1)
        self.dead = False
        self.pid = int(self.run("select pg_backend_pid()").strip())

    DEAD = ("terminating connection due to administrator command",
            "server closed the connection", "<psql exited>")

    def run(self, sql):
        """The output; sets self.dead when the backend or psql is gone."""
        try:
            self.p.stdin.write(sql + ";\n\\echo " + END + "\n")
            self.p.stdin.flush()
        except (BrokenPipeError, OSError):
            self.dead = True
            return "<psql exited>"
        lines = []
        while True:
            l = self.p.stdout.readline()
            if l == "":
                lines.append("<psql exited>")
                break
            if l.rstrip("\n") == END:
                break
            lines.append(l)
        out = "".join(lines)
        if any(d in out for d in self.DEAD):
            self.dead = True
        return out

    def close(self):
        try:
            self.p.stdin.close()
            self.p.wait(10)
        except Exception:
            self.p.kill()


def psql1(db, sql):
    r = subprocess.run(["psql", "-X", "-At", "-d", db, "-c", sql], capture_output=True, text=True)
    return r.stdout + r.stderr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--queries", required=True)
    ap.add_argument("--sessions", type=int, default=8)
    ap.add_argument("--minutes", type=float, default=30)
    ap.add_argument("--timeouts", type=float, default=0.1)
    ap.add_argument("--kills", type=float, default=0.03)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    rnd = random.Random(a.seed)

    files = sorted(glob.glob(os.path.join(a.queries, "*.sql")),
                   key=lambda f: (len(f), f))
    qs = {os.path.basename(f)[:-4]: load(f) for f in files}
    pre = "SET extra_float_digits = 0; SET statement_timeout = 0"

    ref, dur = {}, {}
    base = Session(a.db)
    base.run(pre + "; SET vec.mode = off")
    for name, q in qs.items():
        t = time.time()
        ref[name] = norm(base.run(q))
        dur[name] = time.time() - t
        if "ERROR" in ref[name]:
            sys.exit(f"reference {name} failed: {ref[name][:300]}")
        print(f"reference {name}: {dur[name]:.1f}s", flush=True)
    base.close()

    stop = time.time() + a.minutes * 60
    lock = threading.Lock()
    stats = dict(ok=0, timeout=0, killed=0, bad=0)
    bad = []
    running = {}  # session index -> (pid, query start)
    kill_q = queue.Queue()

    def worker(i):
        r = random.Random(a.seed * 1000 + i)
        s, killed = None, False
        while time.time() < stop:
            if s is not None and s.dead:
                s.close()
                s = None
            if s is None:
                s = Session(a.db)
                s.run(pre + "; SET vec.mode = on")
                if s.dead:
                    continue
            name = r.choice(list(qs))
            to = None
            if r.random() < a.timeouts:
                to = max(50, int(dur[name] * 1000 * r.uniform(0.05, 0.9)))
            if to:
                s.run(f"SET statement_timeout = {to}")
                if s.dead:
                    continue
            with lock:
                running[i] = s.pid
            if r.random() < a.kills:
                kill_q.put((i, s.pid, r.uniform(0.05, 0.9) * dur[name]))
            out = s.run(qs[name])
            with lock:
                running.pop(i, None)
            got = norm(out)
            if "crash of another server process" in out or "terminated by signal" in out:
                kind = "bad"  # a crash ended the session, not the killer
            elif s.dead:
                kind = "killed"
            elif to and "canceling statement due to statement timeout" in out:
                kind = "timeout"
            elif got == ref[name]:
                kind = "ok"
            else:
                kind = "bad"
            if to and not s.dead:
                s.run("SET statement_timeout = 0")
            with lock:
                stats[kind] += 1
                if kind == "bad":
                    bad.append((name, to, out[:2000]))
                    print(f"BAD session {i} {name} timeout={to}: {out[:300]!r}", flush=True)
        if s is not None:
            s.close()

    def killer():
        while time.time() < stop + 5:
            try:
                i, pid, after = kill_q.get(timeout=1)
            except queue.Empty:
                continue
            time.sleep(after)
            with lock:
                if running.get(i) != pid:
                    continue
            psql1(a.db, f"select pg_terminate_backend({pid})")

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(a.sessions)]
    kt = threading.Thread(target=killer, daemon=True)
    kt.start()
    for t in ts:
        t.start()
    last = time.time()
    while any(t.is_alive() for t in ts):
        time.sleep(5)
        if time.time() - last > 60:
            last = time.time()
            with lock:
                print(f"{time.strftime('%H:%M:%S')} {stats}", flush=True)
    for t in ts:
        t.join()

    # leftovers, once every session is gone
    time.sleep(3)
    left = []
    wf = psql1(a.db, "select count(*) from gp_toolkit.gp_workfile_entries").strip()
    if not wf.isdigit():
        print(f"workfiles not checked (no gp_toolkit in {a.db}?): {wf[:100]}")
    elif wf != "0":
        left.append(f"workfile entries: {wf}")
    # pgsql_tmp of each data directory on this host (the engine's spill
    # files live there too); none may be left
    dirs = psql1(a.db, "select datadir from gp_segment_configuration where role = 'p'").split()
    for d in dirs:
        for t in glob.glob(os.path.join(d, "base", "pgsql_tmp")) + glob.glob(os.path.join(d, "pg_tblspc", "*", "*", "pgsql_tmp")):
            n = sum(len(fs) for _, _, fs in os.walk(t))
            if n:
                left.append(f"{n} files in {t}")
    act = psql1(a.db, "select count(*) from pg_stat_activity where state <> 'idle' "
                      "and pid <> pg_backend_pid() and backend_type = 'client backend'").strip()
    if act != "0":
        left.append(f"busy backends: {act}")
    print(f"done {stats}", flush=True)
    for l in left:
        print("LEFTOVER", l)
    for name, to, out in bad:
        print(f"--- {name} timeout={to}\n{out}")
    sys.exit(1 if bad or left else 0)


if __name__ == "__main__":
    main()
