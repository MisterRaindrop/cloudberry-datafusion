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
"""Check plans against the catalog of docs/ir.md.

usage: ir_keys.py [log ...]   (standard input without files)

Reads text holding "datafusion plan: {...}" lines, as the extension logs
each plan at DEBUG1 (SET client_min_messages = debug1 shows the segments'
ones too), and reports every member name, and every value of a member the
catalog lists values for, that the catalog lacks.  Exits 1 if there is
any; also lists the catalog entries no plan used.
"""
import collections, json, os, re, sys

DOC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs", "ir.md")


def catalog():
    text = open(DOC).read()
    block = re.search(r"```catalog\n(.*?)```", text, re.S).group(1)
    members, values = set(), collections.defaultdict(set)
    for line in block.splitlines():
        words = line.split()
        if not words:
            continue
        if words[0] == "member":
            members.update(words[1:])
        elif words[0] == "value":
            values[words[1]].update(words[2:])
        else:
            sys.exit(f"bad catalog line: {line}")
    return members, values


def plans(stream):
    decoder = json.JSONDecoder()
    for line in stream:
        i = line.find("datafusion plan: {")
        if i >= 0:
            yield decoder.raw_decode(line, i + len("datafusion plan: "))[0]


def walk(v, valued, seen_members, seen_values):
    if isinstance(v, dict):
        for k, x in v.items():
            seen_members[k] += 1
            if k in valued and isinstance(x, str):
                seen_values[(k, re.sub(r"^numeric:\d+$", "numeric:S", x))] += 1
            walk(x, valued, seen_members, seen_values)
    elif isinstance(v, list):
        for x in v:
            walk(x, valued, seen_members, seen_values)


def main():
    members, values = catalog()
    seen_members, seen_values = collections.Counter(), collections.Counter()
    n = 0
    files = [open(f) for f in sys.argv[1:]] or [sys.stdin]
    for f in files:
        for p in plans(f):
            n += 1
            walk(p, values, seen_members, seen_values)
    bad = sorted(k for k in seen_members if k not in members)
    bad_values = sorted(kv for kv in seen_values if kv[1] not in values[kv[0]])
    print(f"{n} plans, {len(seen_members)} members, {len(seen_values)} member values")
    for k in bad:
        print(f"member not in the catalog: {k} ({seen_members[k]} times)")
    for k, x in bad_values:
        print(f"value not in the catalog: {k} {x} ({seen_values[(k, x)]} times)")
    unused = sorted(members - set(seen_members))
    unused_values = sorted(f"{k} {x}" for k in values for x in values[k] if (k, x) not in seen_values)
    if unused:
        print("members no plan used:", " ".join(unused))
    if unused_values:
        print("values no plan used:", ", ".join(unused_values))
    sys.exit(1 if bad or bad_values or n == 0 else 0)


main()
