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
#                              own CI does
#   ci/ci.sh cluster CBDB      start a demo cluster (3 primaries, 3 mirrors)
#                              from CBDB/gpAux/gpdemo
#   ci/ci.sh rust              install rustup; rust-toolchain.toml picks
#                              the toolchain
#   ci/ci.sh build CBDB        build and install the extension, the engines
#                              and the direct PAX reader
#   ci/ci.sh test CBDB         the regression tests, the engine check and
#                              the Rust tests
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

step=${1:?usage: ci.sh cloudberry|cluster|rust|build|test [CBDB]}
CBDB=${2:-}
[ -z "$CBDB" ] || CBDB=$(cd "$CBDB" && pwd)

case "$step" in
cloudberry)
	S=$CBDB/devops/build/automation/cloudberry/scripts
	mkdir -p "$CBDB/build-logs"	# the scripts log there, but do not create it
	cd "$CBDB"					# and run ./configure and make from there
	SRC_DIR=$CBDB BUILD_DESTINATION=$GPHOME_DIR "$S/configure-cloudberry.sh"
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
	;;
build)
	env_cloudberry
	cd "$EXT"
	make VX_PAX_SRC="$CBDB/contrib/pax_storage/src/cpp"
	make VX_PAX_SRC="$CBDB/contrib/pax_storage/src/cpp" install
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
*)
	echo "unknown step: $step" >&2
	exit 2
	;;
esac
