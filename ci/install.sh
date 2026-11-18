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
# Install a vec_executor binary package (ci/ci.sh package) into the
# Cloudberry installation that pg_config (on PATH, or PG_CONFIG) names.
#
# usage: install.sh [--hosts FILE]
#
# Copies lib/*.so to pg_config --pkglibdir and extension/* to
# pg_config --sharedir/extension on this host; with --hosts, also to the
# same directories on every host listed in FILE (one per line, as for
# gpssh), through scp.  Every host of the cluster needs the files.  Then,
# in a database: CREATE EXTENSION vec_executor, and add vec_executor to
# shared_preload_libraries or the database's session_preload_libraries
# (docs/operations.md).
#
# The package is built against one Cloudberry commit (BUILD-INFO); the
# extension is meant for a Cloudberry built from that commit.  The direct
# PAX reader (vec_pax.so) checks that the installed pax.so is the one it
# was built with and stays unused otherwise.
#
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PG_CONFIG=${PG_CONFIG:-pg_config}
hosts=

while [ $# -gt 0 ]; do
	case "$1" in
	--hosts)
		hosts=${2:?--hosts needs a file}
		shift 2
		;;
	*)
		echo "usage: install.sh [--hosts FILE]" >&2
		exit 2
		;;
	esac
done

libdir=$("$PG_CONFIG" --pkglibdir)
extdir=$("$PG_CONFIG" --sharedir)/extension
echo "Installing into $libdir and $extdir"
sed -n 1,2p "$HERE/BUILD-INFO"
echo "This Cloudberry: $("$PG_CONFIG" --version)"

install -m 755 "$HERE"/lib/*.so "$libdir/"
install -m 644 "$HERE"/extension/* "$extdir/"

if [ -n "$hosts" ]; then
	while IFS= read -r h; do
		h=${h%%#*}
		h=$(echo "$h" | xargs)
		[ -n "$h" ] || continue
		[ "$h" != "$(hostname)" ] || continue
		echo "Copying to $h"
		scp -q "$HERE"/lib/*.so "$h:$libdir/"
		scp -q "$HERE"/extension/* "$h:$extdir/"
	done <"$hosts"
fi
echo "Done.  Next: CREATE EXTENSION vec_executor; see docs/operations.md."
