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
"""Check an engine's rows against PostgreSQL's (level 0, docs/engines.md).

usage: engine_check.py [--engine NAME] [--baseline NAME] [--out DIR] test ...

Runs each regression test (sql/<test>.sql) twice through psql, once with
vec.engine = NAME and once with the baseline engine, by default null,
which runs nothing, so that every query stays on PostgreSQL.  Then it
compares the two outputs without what is meant to differ: EXPLAIN's plans
and the results of the vec_debug_* test functions.  What is left must be
the same rows and the same errors.

The expected files of the regression tests are DataFusion's, plans and
all; this check needs none, so it works for any engine.  The tests that
are about loading engines (vec_executor, engines) are left out.  Run it
against a cluster where `make install` put the extension and the engines,
after `make installcheck` has created contrib_regression (or give
PGDATABASE).  Exits 1 if any test differs; the differences go to
DIR/<test>.diff (results-engine/ by default).
"""
import argparse, difflib, os, re, subprocess, sys

SKIP = {"vec_executor", "engines"}
MASKS = [
    (re.compile(r"\(seg\d+ slice\d+ [\d.]+:\d+ pid=\d+\)"), "(segN)"),
    (re.compile(r"\(vx_\w+\.c:\d+\)"), "(vx_X.c:N)"),
    (re.compile(r"gp_execution_segment\(\) = \d+"), "gp_execution_segment() = N"),
]


def run(test, engine, db):
    env = dict(os.environ, PGOPTIONS=f"-c vec.engine={engine}", PGDATABASE=db)
    r = subprocess.run(["psql", "-X", "-a", "-q", "-v", "ON_ERROR_STOP=0",
                        "-f", f"sql/{test}.sql"], env=env, text=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return r.stdout.splitlines()


def comparable(lines, source):
    """The output without plans and test-function results.

    Lines the SQL file holds are psql's echo of the statements; the lines
    after a statement up to the next echo are its output, dropped when the
    statement is an EXPLAIN or calls a vec_debug_* function."""
    src = {l.strip() for l in source if l.strip()}
    out, stmt, skip = [], "", False
    rows = None                 # the rows of a result table being read
    for line in lines:
        if line.strip() in src:
            if not stmt or stmt.rstrip().endswith(";"):
                stmt = ""
            stmt += " " + line.strip()
            skip = bool(re.search(r"\bEXPLAIN\b|\bvec_debug_\w+|\bvec_version\b", stmt, re.I))
            out.append(line)
            continue
        if skip:
            continue
        for pat, rep in MASKS:
            line = pat.sub(rep, line)
        # Without ORDER BY the order of a result's rows is not defined:
        # compare them sorted, as pg_regress's gpdiff does.
        if rows is not None:
            if re.match(r"^\(\d+ rows?\)$", line):
                out.extend(rows if re.search(r"\bORDER BY\b", stmt, re.I) else sorted(rows))
                rows = None
                out.append(line)
            else:
                rows.append(line)
            continue
        out.append(line)
        if re.match(r"^-+(\+-+)*$", line):
            rows = []
    if rows is not None:
        out.extend(rows)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", default="datafusion")
    ap.add_argument("--baseline", default="null")
    ap.add_argument("--out", default="results-engine")
    ap.add_argument("tests", nargs="+")
    a = ap.parse_args()
    db = os.environ.get("PGDATABASE", "contrib_regression")
    os.makedirs(a.out, exist_ok=True)
    failed = 0
    for test in a.tests:
        if test in SKIP:
            continue
        source = open(f"sql/{test}.sql").read().splitlines()
        base = comparable(run(test, a.baseline, db), source)
        got = comparable(run(test, a.engine, db), source)
        diff = list(difflib.unified_diff(base, got, f"{test} ({a.baseline})",
                                         f"{test} ({a.engine})", lineterm="", n=2))
        path = os.path.join(a.out, f"{test}.diff")
        if diff:
            failed += 1
            open(path, "w").write("\n".join(diff) + "\n")
            changed = sum(1 for l in diff if l[:1] in "+-" and l[:3] not in ("+++", "---"))
            print(f"not ok  {test}: {changed} lines differ ({path})")
        else:
            if os.path.exists(path):
                os.remove(path)
            print(f"ok      {test}")
    print(f"{failed} of {len([t for t in a.tests if t not in SKIP])} tests differ "
          f"between {a.engine} and {a.baseline}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
