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
#   ci/lint.sh           both
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

fmt() {
	(cd engines/datafusion && cargo fmt --check) && echo "cargo fmt: ok"
}

case "${1:-all}" in
license) license ;;
fmt) fmt ;;
all)
	rc=0
	license || rc=1
	fmt || rc=1
	exit $rc
	;;
*)
	echo "usage: ci/lint.sh [license|fmt]" >&2
	exit 2
	;;
esac
