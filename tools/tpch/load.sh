#!/bin/bash
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
# Create a TPC-H database for tools/tpch/bench.py (docs/benchmarks.md).
#
# usage: load.sh SF DB [WORKDIR]
#
# Builds TPC-H's dbgen (tpch-kit, at a fixed commit; its license does not
# let this repository carry it) in WORKDIR, creates database DB with the
# PAX tables of schema.sql and the view of Q15, generates scale factor SF
# in chunks, COPYs each chunk and removes its files, then analyzes the
# tables and makes the database load the extension.  Run it as the
# cluster's owner with the cluster's environment (psql reaches the
# coordinator); WORKDIR (default /tmp/tpch-work) needs room for a few
# chunks, about SF * 1 GB / CHUNKS each.  DB must not exist.
#
# Environment: CHUNKS (default: SF, at least 1), JOBS (chunks generated and
# loaded at once, default 4), TPCH_KIT (a tpch-kit checkout to use instead
# of cloning one).
#
set -euo pipefail

SF=${1:?usage: load.sh SF DB [WORKDIR]}
DB=${2:?usage: load.sh SF DB [WORKDIR]}
WORK=${3:-/tmp/tpch-work}
HERE=$(cd "$(dirname "$0")" && pwd)
KIT_COMMIT=852ad0a5ee31ebefeed884cea4188781dd9613a3
CHUNKS=${CHUNKS:-$(awk -v s="$SF" 'BEGIN { c = int(s); print (c < 1 ? 1 : c) }')}
JOBS=${JOBS:-4}

mkdir -p "$WORK"
KIT=${TPCH_KIT:-$WORK/tpch-kit}
if [ ! -x "$KIT/dbgen/dbgen" ]; then
	if [ ! -d "$KIT" ]; then
		git clone -q https://github.com/gregrahn/tpch-kit.git "$KIT"
		git -C "$KIT" checkout -q "$KIT_COMMIT"
	fi
	make -s -C "$KIT/dbgen" MACHINE=LINUX DATABASE=POSTGRESQL >/dev/null
fi
DBGEN=$KIT/dbgen

if [ -n "$(psql -X -At -d postgres -c "SELECT 1 FROM pg_database WHERE datname = '$DB'")" ]; then
	echo "load.sh: database $DB exists; drop it first or choose another name" >&2
	exit 1
fi
psql -X -q -d postgres -c "CREATE DATABASE \"$DB\""
psql -X -q -d "$DB" -v ON_ERROR_STOP=1 -f "$HERE/schema.sql"

# One chunk: generate it in a directory of its own, COPY each table's file
# (dbgen ends every line with '|'), remove the files.
load_chunk() {
	local i=$1 dir="$WORK/chunk$1"
	mkdir -p "$dir"
	(cd "$dir" && DSS_CONFIG="$DBGEN" "$DBGEN/dbgen" -q -f -s "$SF" -C "$CHUNKS" -S "$i" -b "$DBGEN/dists.dss")
	for f in "$dir"/*.tbl*; do
		[ -e "$f" ] || continue
		local t
		t=$(basename "$f" | sed 's/\.tbl.*//')
		# every chunk holds all of nation and region
		if [ "$i" -gt 1 ] && { [ "$t" = nation ] || [ "$t" = region ]; }; then
			rm -f "$f"
			continue
		fi
		sed 's/|$//' "$f" | psql -X -q -d "$DB" -v ON_ERROR_STOP=1 \
			-c "\\copy $t FROM STDIN WITH (DELIMITER '|')"
		rm -f "$f"
	done
	rmdir "$dir"
}

start=$(date +%s)
running=0
for i in $(seq 1 "$CHUNKS"); do
	load_chunk "$i" &
	running=$((running + 1))
	if [ "$running" -ge "$JOBS" ]; then
		wait -n
		running=$((running - 1))
	fi
done
# wait -n reports each job's status, so set -e stops at a failed chunk
while [ "$running" -gt 0 ]; do
	wait -n
	running=$((running - 1))
done

psql -X -q -d "$DB" -v ON_ERROR_STOP=1 -f "$HERE/view15.sql"
psql -X -q -d "$DB" -c "ANALYZE"
psql -X -q -d "$DB" -c "CREATE EXTENSION IF NOT EXISTS vec_executor"
psql -X -q -d postgres -c "ALTER DATABASE \"$DB\" SET session_preload_libraries = 'vec_executor'"
echo "load.sh: $DB at scale factor $SF in $(( $(date +%s) - start )) s"
psql -X -d "$DB" -c "SELECT count(*) AS lineitem_rows FROM lineitem"
