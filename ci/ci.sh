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
# The steps of the CI (.github/workflows/ci.yml), runnable by hand in
# Apache Cloudberry's build container (apache/incubator-cloudberry:
# cbdb-build-rocky9-latest, hostname cdw), as gpadmin:
#
#   ci/ci.sh cloudberry CBDB   build and install the Cloudberry tree CBDB
#                              in /usr/local/cloudberry-db, as Cloudberry's
#                              own CI does; ENABLE_DEBUG=true adds
#                              assertions (--enable-cassert) and debug
#                              information
#   ci/ci.sh cluster CBDB      start a demo cluster (3 primaries, 3 mirrors)
#                              from CBDB/gpAux/gpdemo
#   ci/ci.sh rust              install rustup; rust-toolchain.toml picks
#                              the toolchain
#   ci/ci.sh build CBDB        build and install the extension, the engines
#                              and the direct PAX reader; a compiler warning
#                              in the C code fails the build
#   ci/ci.sh clippy            cargo clippy of the engine, warnings fail
#   ci/ci.sh test CBDB         the regression tests, the engine check and
#                              the Rust tests
#   ci/ci.sh asan              the engine's unit tests under AddressSanitizer
#                              (a nightly toolchain, installed by the step)
#   ci/ci.sh package CBDB OUT  a binary package of what 'build' installed:
#                              OUT/vec_executor-VERSION-cloudberry-COMMIT-
#                              OS-ARCH.tar.gz (libraries, extension files,
#                              install.sh, BUILD-INFO) and its .sha256
#   ci/ci.sh soak CBDB         TPC-H at scale factor 1 (tools/tpch), then
#                              tools/stress.py and tools/fuzz.py for a few
#                              minutes each (SOAK_MINUTES, default 5)
#
# Only 'cloudberry' needs the whole tree; the other steps need its
# gpAux/gpdemo and contrib/pax_storage/src/cpp, which the workflow caches
# with the installation.

set -euo pipefail

EXT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
GPHOME_DIR=/usr/local/cloudberry-db

env_cloudberry() {
	# cloudberry-env.sh reads unset variables
	set +u
	source "$GPHOME_DIR/cloudberry-env.sh"
	set -u
	export PATH="$HOME/.cargo/bin:$PATH"
}

env_cluster() {
	env_cloudberry
	set +u
	source "$CBDB/gpAux/gpdemo/gpdemo-env.sh"
	set -u
}

step=${1:?usage: ci.sh cloudberry|cluster|rust|build|clippy|test|asan|package|soak [CBDB [OUT]]}
CBDB=${2:-}
[ -z "$CBDB" ] || CBDB=$(cd "$CBDB" && pwd)

case "$step" in
cloudberry)
	S=$CBDB/devops/build/automation/cloudberry/scripts
	mkdir -p "$CBDB/build-logs"	# the scripts log there, but do not create it
	cd "$CBDB"					# and run ./configure and make from there
	SRC_DIR=$CBDB BUILD_DESTINATION=$GPHOME_DIR ENABLE_DEBUG=${ENABLE_DEBUG:-false} \
		"$S/configure-cloudberry.sh"
	SRC_DIR=$CBDB BUILD_DESTINATION=$GPHOME_DIR "$S/build-cloudberry.sh"
	;;
cluster)
	env_cloudberry
	ssh -o StrictHostKeyChecking=no "$(hostname)" true
	# C.UTF-8, as the development container's segments have it: C ordering
	# (the engine treats it as C, CU1) and upper()/lower() of any UTF-8;
	# gpinitsystem takes the environment's locale
	LC_ALL=C.UTF-8 LANG=C.UTF-8 make create-demo-cluster --directory "$CBDB/gpAux/gpdemo"
	env_cluster
	# the development container's setting, which the memory tests assume
	gpconfig -c gp_vmem_protect_limit -v 8192
	gpstop -ar
	psql -X -d postgres -c "SELECT version()"
	;;
rust)
	if ! command -v rustup >/dev/null && [ ! -x "$HOME/.cargo/bin/rustup" ]; then
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
			sh -s -- -y --profile minimal --default-toolchain none
	fi
	export PATH="$HOME/.cargo/bin:$PATH"
	# with no argument: the toolchain rust-toolchain.toml names
	cd "$EXT/engines/datafusion" && rustup toolchain install
	rustup component add rustfmt clippy
	;;
build)
	env_cloudberry
	cd "$EXT"
	make VX_PAX_SRC="$CBDB/contrib/pax_storage/src/cpp" COPT=-Werror
	make VX_PAX_SRC="$CBDB/contrib/pax_storage/src/cpp" install
	;;
clippy)
	export PATH="$HOME/.cargo/bin:$PATH"
	cd "$EXT/engines/datafusion"
	cargo clippy --release --workspace --all-targets -- -D warnings
	;;
test)
	env_cluster
	cd "$EXT"
	rc=0
	make installcheck VX_PAX_SRC="$CBDB/contrib/pax_storage/src/cpp" || rc=1
	[ ! -f regression.diffs ] || cat regression.diffs
	make installcheck-engine VX_PAX_SRC="$CBDB/contrib/pax_storage/src/cpp" || rc=1
	# release, as the engine is built: a debug build of the workspace takes
	# another 10 GB
	make df-rust-test DF_CARGO_FLAGS=--release || rc=1
	exit $rc
	;;
asan)
	export PATH="$HOME/.cargo/bin:$PATH"
	rustup toolchain install nightly --profile minimal
	cd "$EXT/engines/datafusion"
	# a target of its own: the sanitizer applies to what is built for it,
	# not to the build scripts
	RUSTFLAGS="-Zsanitizer=address" RUSTDOCFLAGS="-Zsanitizer=address" \
	ASAN_OPTIONS=detect_leaks=1 CARGO_TARGET_DIR="$EXT/engines/datafusion/target-asan" \
		cargo +nightly test --workspace --target "$(rustc -vV | sed -n 's/^host: //p')"
	;;
package)
	env_cloudberry
	OUT=${3:?usage: ci.sh package CBDB OUT}
	mkdir -p "$OUT"
	OUT=$(cd "$OUT" && pwd)
	cd "$EXT"
	version=$(git describe --tags --always --dirty 2>/dev/null || echo unknown)
	version=${version#v}
	cbdb_commit=$(git -C "$CBDB" rev-parse HEAD 2>/dev/null || echo "${CLOUDBERRY_COMMIT:-unknown}")
	os=$(. /etc/os-release && echo "$ID$VERSION_ID" | cut -d. -f1)
	name=vec_executor-$version-cloudberry-${cbdb_commit:0:10}-$os-$(uname -m)
	stage=$(mktemp -d)
	make VX_PAX_SRC="$CBDB/contrib/pax_storage/src/cpp" install DESTDIR="$stage/root" >/dev/null
	pkg=$stage/$name
	mkdir -p "$pkg/lib" "$pkg/extension"
	cp "$stage/root$(pg_config --pkglibdir)"/{vec_executor,vec_engine_datafusion,vec_engine_null}.so "$pkg/lib/"
	if [ -f "$stage/root$(pg_config --pkglibdir)/vec_pax.so" ]; then
		cp "$stage/root$(pg_config --pkglibdir)/vec_pax.so" "$pkg/lib/"
	fi
	# debug information off (480 MB of the engine's), symbols kept
	strip --strip-debug "$pkg"/lib/*.so
	cp "$stage/root$(pg_config --sharedir)"/extension/vec_executor* "$pkg/extension/"
	cp "$EXT/ci/install.sh" "$EXT/LICENSE" "$pkg/"
	cat >"$pkg/BUILD-INFO" <<-INFO
		vec_executor $version ($(git rev-parse HEAD 2>/dev/null || echo unknown))
		Built against Apache Cloudberry $cbdb_commit
		  $(pg_config --version)
		$(rustc --version)
		$(gcc --version | head -1)
		$(. /etc/os-release && echo "$PRETTY_NAME") $(uname -m)
		Built $(date -u +%Y-%m-%dT%H:%M:%SZ)
	INFO
	tar -C "$stage" -czf "$OUT/$name.tar.gz" "$name"
	(cd "$OUT" && sha256sum "$name.tar.gz" >"$name.tar.gz.sha256")
	rm -rf "$stage"
	ls -l "$OUT/$name.tar.gz"
	cat "$OUT/$name.tar.gz.sha256"
	;;
soak)
	env_cluster
	cd "$EXT"
	minutes=${SOAK_MINUTES:-5}
	rc=0
	tools/tpch/load.sh 1 tpch1 /tmp/tpch-work
	python3 tools/stress.py --db tpch1 --queries tools/tpch/queries --sessions 4 \
		--minutes "$minutes" || rc=1
	psql -X -q -d postgres -c "CREATE DATABASE fuzz"
	psql -X -q -d postgres -c "ALTER DATABASE fuzz SET session_preload_libraries = 'vec_executor'"
	# each seed's queries, then the next seed until the time is up; what
	# differs goes to fuzz-SEED.txt, which the workflow keeps
	end=$(($(date +%s) + 60 * minutes))
	seed=1
	setup=--setup
	while [ "$(date +%s)" -lt "$end" ]; do
		if ! python3 tools/fuzz.py --db fuzz $setup --queries 100 --seed "$seed" \
			--out "fuzz-$seed.txt" >"fuzz-$seed.log"; then
			rc=1
			echo "fuzz seed $seed: queries differ or fail in the engine (fuzz-$seed.txt)"
		fi
		tail -1 "fuzz-$seed.log"
		rm -f "fuzz-$seed.log"
		[ -s "fuzz-$seed.txt" ] || rm -f "fuzz-$seed.txt"
		setup=
		seed=$((seed + 1))
	done
	exit $rc
	;;
*)
	echo "unknown step: $step" >&2
	exit 2
	;;
esac
