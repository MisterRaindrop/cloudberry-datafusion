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
# Checks that need neither Cloudberry nor a build (the CI's lint job):
#
#   ci/lint.sh license   every source file carries the Apache license
#                        header; not the documents, the expected outputs,
#                        the PAX patches (PAX's files) or the TPC-H queries
#                        (dbgen's qgen wrote them)
#   ci/lint.sh fmt       cargo fmt --check of the engine
#   ci/lint.sh ext       the extension's SQL scripts: a script a release
#                        (tag v*) shipped is unchanged, and the control
#                        file's default_version is reachable: its install
#                        script, or one of an older version and update
#                        scripts up to it (README, Versions)
#   ci/lint.sh deps      cargo deny (engines/datafusion/deny.toml): the
#                        licenses and sources of the dependencies;
#                        "deps advisories" checks RustSec's advisories
#                        instead, which needs the network
#   ci/lint.sh           license, ext and fmt
#
set -euo pipefail

EXT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$EXT"

license() {
	local missing=0 f files
	files=$(git ls-files) # set -e: no list, no check
	while IFS= read -r f; do
		case "$f" in
		*.md | LICENSE | .gitignore | */Cargo.lock | expected/* | benchmarks/* | \
			patches/* | tools/tpch/queries/*) continue ;;
		esac
		if ! head -20 "$f" | grep -q "Licensed to the Apache Software Foundation"; then
			echo "no license header: $f"
			missing=1
		fi
	done <<<"$files"
	[ "$missing" = 0 ] && echo "license headers: ok"
	return "$missing"
}

ext() {
	local bad=0 tag f v
	for tag in $(git tag -l 'v*'); do
		for f in $(git ls-tree --name-only "$tag" | grep -E '^vec_executor--.*\.sql$'); do
			if [ ! -f "$f" ]; then
				echo "$f: shipped in $tag, removed since"
				bad=1
			elif ! git show "$tag:$f" | cmp -s - "$f"; then
				echo "$f: shipped in $tag, changed since; add an update script instead"
				bad=1
			fi
		done
	done
	v=$(sed -n "s/^default_version *= *'\(.*\)'/\1/p" vec_executor.control)
	# breadth-first over update scripts, from the versions that have an
	# install script
	local reach=" " frontier="" next from to
	for f in vec_executor--*.sql; do
		f=${f#vec_executor--}
		f=${f%.sql}
		case "$f" in *--*) ;; *) reach="$reach$f " frontier="$frontier $f" ;; esac
	done
	while [ -n "$frontier" ]; do
		next=""
		for from in $frontier; do
			for f in vec_executor--"$from"--*.sql; do
				[ -e "$f" ] || continue
				to=${f#vec_executor--"$from"--}
				to=${to%.sql}
				case "$reach" in *" $to "*) ;; *) reach="$reach$to " next="$next $to" ;; esac
			done
		done
		frontier=$next
	done
	case "$reach" in
	*" $v "*) ;;
	*)
		echo "default_version $v: no install script and no update path to it"
		bad=1
		;;
	esac
	[ "$bad" = 0 ] && echo "extension scripts: ok (default_version $v)"
	return "$bad"
}

deps() {
	local which=("$@")
	[ ${#which[@]} -gt 0 ] || which=(licenses bans sources)
	(cd engines/datafusion && cargo deny check "${which[@]}")
}

fmt() {
	(cd engines/datafusion && cargo fmt --check) && echo "cargo fmt: ok"
}

case "${1:-all}" in
license) license ;;
fmt) fmt ;;
ext) ext ;;
deps)
	shift
	deps "$@"
	;;
all)
	rc=0
	license || rc=1
	ext || rc=1
	fmt || rc=1
	exit $rc
	;;
*)
	echo "usage: ci/lint.sh [license|ext|fmt|deps [advisories]]" >&2
	exit 2
	;;
esac
