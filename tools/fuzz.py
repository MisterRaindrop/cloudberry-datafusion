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
usage: fuzz.py [--db DB] [--seed N] [--queries N] [--setup] [--out FILE]

Differential testing: random queries over random tables, each run with
vec.mode off and on under both planners; the results must be the same
rows.  --setup (re)creates the tables (fz_*: heap, AO column and PAX, of
every type the engine reads, with NULLs, duplicates, empty strings,
negatives and extremes) in DB, which must have vec_executor in its
session_preload_libraries.  Queries avoid what the README lists as known
differences: no expression that can fail (a division divides by NULLIF(x,
0)), text sorted under the C
collation, LIMIT only under an ORDER BY of every output column.  Rows
compare as sets; a value printed as a float8 prints may differ in the last
digits (12 significant digits must agree), and -0 equals 0.

Each query is printed with its verdict: same, DIFF (rows differ), VECERR
(only vec failed), PGERR (only PostgreSQL failed), both failing (BOTHERR,
same error or not), and whether a slice ran in the engine.  Mismatches go
to --out with both results.  Exit 1 if any query differs or fails under
vec only.
"""
import argparse, random, re, subprocess, sys

TABLES = [("fz_heap", "heap"), ("fz_aoco", "ao_column"), ("fz_pax", "pax")]
COLS = {  # name: (type, generator kind)
    "id": "int4", "k": "int4", "g": "int2", "b": "int8", "n": "numeric(15,2)",
    "w": "numeric(30,6)", "f": "float8", "t": "text", "v": "varchar(12)", "c": "char(4)",
    "d": "date", "ts": "timestamp", "flag": "bool",
}
NUMS = ["k", "g", "b", "n", "w", "f"]
INTS = ["k", "g", "b"]
STRS = ["t", "v", "c"]
KEYS = ["k", "g", "t", "d", "flag", "c"]


def psql(db, sql, timeout=300):
    try:
        r = subprocess.run(["psql", "-X", "-At", "-q", "-F", "|", "-v", "ON_ERROR_STOP=1",
                            "-d", db, "-f", "-"], input=sql, capture_output=True, text=True,
                           timeout=timeout)
    except subprocess.TimeoutExpired:
        return None, "TIMEOUT"
    err = r.stderr.strip()
    if r.returncode != 0 or "ERROR" in err:
        m = re.search(r"ERROR:\s+(.*)", err)
        return None, re.sub(r"\s*\(seg\d+ slice\d+ [^)]*\)", "", m.group(1) if m else err)
    return r.stdout, None


def setup(db, rng):
    rows = []
    words = ["", "a", "B", "abc", "Abc", "z", "中文", "é", "x y", "k1", "k10", "k2"]
    for i in range(1, 20001):
        def maybe(v, p=0.08):
            return "NULL" if rng.random() < p else v
        k = rng.choice([rng.randint(-5, 50), rng.randint(0, 2000)])
        n = round(rng.uniform(-99999, 99999), 2)
        w = round(rng.uniform(-1e9, 1e9), 6)
        f = rng.choice([rng.uniform(-1e6, 1e6), 0.0, -0.0, 1e-9, 12345.678])
        t = rng.choice(words) + (str(rng.randint(0, 99)) if rng.random() < 0.5 else "")
        rows.append("(%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (
            i, maybe(str(k)), maybe(str(rng.randint(-3, 9))), maybe(str(rng.randint(-10**12, 10**12))),
            maybe(repr(n)), maybe(repr(w)), maybe(repr(f)),
            maybe("'" + t.replace("'", "''") + "'"), maybe("'" + rng.choice(words[:6]) + "'"),
            maybe("'" + rng.choice(["a", "ab", "abcd", "", "中"]) + "'"),
            maybe("date '2000-01-01' + %d" % rng.randint(-4000, 9000)),
            maybe("timestamp '2001-02-03 04:05:06' + interval '%d minutes'" % rng.randint(-10**6, 10**6)),
            maybe(rng.choice(["true", "false"]))))
    cols = ", ".join(f"{c} {t}" for c, t in COLS.items())
    sql = []
    for name, am in TABLES:
        sql.append(f"DROP TABLE IF EXISTS {name};")
        sql.append(f"CREATE TABLE {name} ({cols}) USING {am} DISTRIBUTED BY (id);")
    sql.append("INSERT INTO fz_heap VALUES " + ",".join(rows) + ";")
    for name, _ in TABLES[1:]:
        sql.append(f"INSERT INTO {name} SELECT * FROM fz_heap WHERE id % 3 <> 0;")
        sql.append(f"INSERT INTO {name} SELECT * FROM fz_heap WHERE id % 3 = 0;")
    sql.append("DROP TABLE IF EXISTS fz_dim; CREATE TABLE fz_dim (k int4, label text, weight numeric(8,3)) DISTRIBUTED BY (k);")
    sql.append("INSERT INTO fz_dim SELECT i, 'L' || (i % 7), i * 1.125 FROM generate_series(-5, 300) i;")
    sql.append("DELETE FROM fz_pax WHERE id % 97 = 5; DELETE FROM fz_heap WHERE id % 89 = 1;")
    sql.append("ANALYZE fz_heap; ANALYZE fz_aoco; ANALYZE fz_pax; ANALYZE fz_dim;")
    out, err = psql(db, "\n".join(sql), timeout=900)
    if err:
        sys.exit(f"setup failed: {err}")


class Gen:
    def __init__(self, rng):
        self.r = rng

    def pick(self, xs):
        return self.r.choice(xs)

    def num_expr(self, a, depth=0):
        r = self.r.random()
        col = f"{a}.{self.pick(NUMS)}"
        if depth > 1 or r < 0.45:
            return col
        if r < 0.6:
            return f"({self.num_expr(a, depth + 1)} {self.pick(['+', '-', '*'])} {self.num_expr(a, depth + 1)})"
        if r < 0.7:
            return f"({col} / NULLIF({a}.{self.pick(INTS)}, 0))"
        if r < 0.8:
            return f"COALESCE({col}, {self.r.randint(-5, 5)})"
        if r < 0.9:
            return f"abs({col})"
        return f"CASE WHEN {self.pred(a, depth + 1)} THEN {col} ELSE {self.r.randint(0, 9)} END"

    def str_expr(self, a):
        col = f"{a}.{self.pick(STRS)}"
        return self.pick([col, col, f"upper({col})", f"lower({col})", f"substr({col}, 2, 3)",
                          f"({col} || 'x')", f"COALESCE({col}, '?')", f"{col}::text"])

    def pred(self, a, depth=0):
        r = self.r.random()
        if depth < 2 and r < 0.15:
            return f"({self.pred(a, depth + 1)} {self.pick(['AND', 'OR'])} {self.pred(a, depth + 1)})"
        if r < 0.45:
            return f"{self.num_expr(a, depth + 1)} {self.pick(['<', '<=', '=', '<>', '>', '>='])} {self.r.randint(-50, 2000)}"
        if r < 0.55:
            return f"{a}.{self.pick(NUMS + STRS + ['d', 'flag'])} IS {self.pick(['', 'NOT '])}NULL"
        if r < 0.65:
            return f"{a}.t {self.pick(['LIKE', 'NOT LIKE'])} '{self.pick(['a%', '%1', '_b%', '%', 'k1_'])}'"
        if r < 0.75:
            return f"{a}.d {self.pick(['<', '>='])} date '{self.r.randint(1995, 2020)}-0{self.r.randint(1, 9)}-15'"
        if r < 0.85:
            return f"{a}.k IN ({', '.join(str(self.r.randint(-5, 60)) for _ in range(4))})"
        if r < 0.92:
            return f"{a}.flag"
        return f"{self.str_expr(a)} {self.pick(['=', '<>'])} '{self.pick(['a', 'abc', '', 'k1'])}'"

    def agg(self, a):
        col = self.num_expr(a)
        return self.pick([f"count(*)", f"count({a}.{self.pick(list(COLS))})", f"sum({col})",
                          f"avg({a}.{self.pick(['k', 'n', 'b', 'w'])})", f"min({col})", f"max({col})",
                          f"count(DISTINCT {a}.{self.pick(['k', 'g', 't'])})",
                          f"min({a}.{self.pick(STRS + ['d', 'ts'])})", f"max({a}.{self.pick(['t', 'd', 'ts'])})"])

    def from_clause(self):
        t1 = self.pick([n for n, _ in TABLES])
        r = self.r.random()
        if r < 0.45:
            return f"{t1} a", ["a"]
        t2 = self.pick([n for n, _ in TABLES] + ["fz_dim"])
        key = "k"
        if r < 0.7:
            kind = self.pick(["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"])
            if t2 == "fz_dim":
                return f"{t1} a {kind} fz_dim b ON a.k = b.k", ["a"]
            key = self.pick(["k", "g", "t", "d"])
            return f"{t1} a {kind} {t2} b ON a.{key} = b.{key}", ["a", "b"]
        if r < 0.85:
            sub = f"SELECT 1 FROM {t2} b WHERE b.k = a.k" + ("" if t2 == "fz_dim" else f" AND {self.pred('b')}")
            return f"{t1} a WHERE {self.pick(['', 'NOT '])}EXISTS ({sub})", ["a"]
        return (f"{t1} a WHERE a.k {self.pick(['IN', 'NOT IN'])} (SELECT b.k FROM {t2} b WHERE b.k IS NOT NULL"
                + ("" if t2 == "fz_dim" else f" AND {self.pred('b')}") + ")"), ["a"]

    def query(self):
        frm, al = self.from_clause()
        where_kw = " AND " if " WHERE " in frm else " WHERE "
        where = where_kw + self.pred(al[0]) if self.r.random() < 0.6 else ""
        a = al[0]
        r = self.r.random()
        if r < 0.35:
            keys = self.r.sample(KEYS, self.r.randint(1, 2))
            sel = [f"{a}.{k}" for k in keys] + [self.agg(a) for _ in range(self.r.randint(1, 3))]
            having = f" HAVING count(*) > {self.r.randint(0, 3)}" if self.r.random() < 0.3 else ""
            q = f"SELECT {', '.join(sel)} FROM {frm}{where} GROUP BY {', '.join(f'{a}.{k}' for k in keys)}{having}"
            ncols = len(sel)
        elif r < 0.5:
            sel = [self.agg(a) for _ in range(self.r.randint(1, 4))]
            q = f"SELECT {', '.join(sel)} FROM {frm}{where}"
            ncols = len(sel)
        elif r < 0.62:
            part = self.pick(["k", "g", "flag", "t"])
            fn = self.pick([f"row_number() OVER (PARTITION BY {a}.{part} ORDER BY {a}.id)",
                            f"rank() OVER (PARTITION BY {a}.{part} ORDER BY {a}.b, {a}.id)",
                            f"sum({a}.n) OVER (PARTITION BY {a}.{part})",
                            f"count(*) OVER (PARTITION BY {a}.{part})",
                            f"max({a}.k) OVER (PARTITION BY {a}.{part} ORDER BY {a}.id)"])
            q = f"SELECT {a}.id, {a}.{part}, {fn} AS wf FROM {frm}{where}"
            ncols = 3
        elif r < 0.72:
            t1, t2 = self.pick(TABLES)[0], self.pick(TABLES)[0]
            cols = self.pick(["k, g", "t", "d, flag", "k, t", "c"])
            ncols = cols.count(",") + 1
            q = (f"SELECT {cols} FROM {t1} WHERE {self.pred(t1)} {self.pick(['UNION ALL', 'UNION', 'INTERSECT', 'EXCEPT', 'INTERSECT ALL', 'EXCEPT ALL'])} "
                 f"SELECT {cols} FROM {t2} WHERE {self.pred(t2)}")
        elif r < 0.8:
            q = f"SELECT DISTINCT {a}.{self.pick(KEYS)}, {a}.{self.pick(KEYS)} FROM {frm}{where}"
            ncols = 2
        else:
            sel = [f"{a}.id"] + [self.pick([self.num_expr(a), self.str_expr(a), f"{a}.d", f"{a}.ts",
                                            f"extract(year from {a}.d)", f"({a}.d + {a}.g)"])
                                 for _ in range(self.r.randint(1, 3))]
            q = f"SELECT {', '.join(sel)} FROM {frm}{where}"
            ncols = len(sel)
        if self.r.random() < 0.25:
            # an ORDER BY of every output column makes LIMIT deterministic
            q = f"SELECT * FROM ({q}) s ORDER BY {', '.join(str(i) for i in range(1, ncols + 1))} LIMIT {self.r.randint(1, 50)}"
        return q


def floatish(v):
    """A value printed as a float8 prints, not as a numeric of a scale: an
    exponent, more than 6 decimals, or 15 or more digits."""
    m = re.fullmatch(r"-?(\d+)(?:\.(\d+))?(e[-+]?\d+)?", v)
    return bool(m) and (m.group(3) is not None or len(m.group(2) or "") > 6
                        or (m.group(2) is None and len(m.group(1)) >= 15))


def cells_equal(a, b):
    """Known differences (README): float sums in the last digits, and -0."""
    if a == b or {a, b} == {"-0", "0"}:
        return True
    if not (floatish(a) or floatish(b)):
        return False
    try:
        x, y = float(a), float(b)
    except ValueError:
        return False
    return abs(x - y) <= 1e-12 * max(abs(x), abs(y))


def same_rows(a, b):
    ra, rb = sorted(a.splitlines()), sorted(b.splitlines())
    if len(ra) != len(rb):
        return False
    for x, y in zip(ra, rb):
        cx, cy = x.split("|"), y.split("|")
        if len(cx) != len(cy) or not all(cells_equal(p, q) for p, q in zip(cx, cy)):
            return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="fuzzdb")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--queries", type=int, default=200)
    ap.add_argument("--setup", action="store_true")
    ap.add_argument("--out", default="fuzz-mismatch.txt")
    args = ap.parse_args()
    rng = random.Random(args.seed)
    if args.setup:
        setup(args.db, random.Random(12345))
    gen = Gen(rng)
    counts = {}
    bad = 0
    with open(args.out, "w") as out:
        for i in range(args.queries):
            q = gen.query()
            for opt in ("off", "on"):
                pre = f"SET optimizer = {opt}; SET extra_float_digits = 0; SET statement_timeout = '120s'; "
                a, ea = psql(args.db, pre + "SET vec.mode = off; " + q)
                b, eb = psql(args.db, pre + "SET vec.mode = on; " + q)
                plan, _ = psql(args.db, pre + "SET vec.mode = on; EXPLAIN " + q)
                used = plan is not None and re.search(r"Vectorized: slice \d+ eligible", plan) is not None
                if ea and eb:
                    v = "BOTHERR"
                elif eb:
                    v = "VECERR"
                elif ea:
                    v = "PGERR"
                else:
                    v = "same" if same_rows(a, b) else "DIFF"
                counts[v] = counts.get(v, 0) + 1
                counts["vec-used"] = counts.get("vec-used", 0) + used
                print(f"{i} {opt} {v}{' vec' if used else ''}: {q[:150]}", flush=True)
                if v in ("DIFF", "VECERR", "PGERR"):
                    bad += v != "PGERR"
                    out.write(f"== {i} optimizer={opt} {v}\n{q};\n-- off: {ea or a}\n-- on: {eb or b}\n\n")
                    out.flush()
    print("SUMMARY", " ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
