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
# A cluster of three hosts in containers on one Docker host, so that
# Motions cross a network between hosts and mirrors take over from failed
# primaries: coordinator on cdw, two primaries on each of sdw1 and sdw2,
# each primary's mirror on the other host.
#
#   ci/multihost.sh up      start the containers (network vxmh, vxmh-cdw,
#                           vxmh-sdw1, vxmh-sdw2) and install the extension
#   ci/multihost.sh init    gpinitsystem
#   ci/multihost.sh test    the checks below; exits 1 if one fails
#   ci/multihost.sh down    remove the containers and the network
#
# Environment:
#   MH_IMAGE    the image: Cloudberry's build or sandbox image, whose
#               containers share one gpadmin SSH key (default
#               apache/incubator-cloudberry:cbdb-build-rocky9-latest)
#   MH_GPHOME   a Cloudberry installation with the extension installed,
#               mounted at /usr/local/cloudberry-db on every host; or
#   MH_PACKAGE  a binary package (ci/ci.sh package) installed with its
#               install.sh on every host, into the image's installation
#   MH_TPCH_KIT a tpch-kit checkout for tools/tpch/load.sh (default: it
#               clones one, which needs git in the image)
#
# test, as gpadmin on cdw:
#   1. the engine check (tools/engine_check.py: each regression test with
#      the engine against PostgreSQL's rows, on this cluster's 4 segments)
#   2. TPC-H at scale factor 1 under the four configurations of
#      tools/tpch/bench.py, whose rows must agree
#   3. tools/stress.py, 4 sessions, MH_STRESS_MINUTES (default 3)
#   4. a primary on sdw1 stopped (immediate), its mirror on sdw2 promoted,
#      TPC-H again, then gprecoverseg and the roles restored
#
set -euo pipefail

EXT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
IMAGE=${MH_IMAGE:-apache/incubator-cloudberry:cbdb-build-rocky9-latest}
HOSTS=(cdw sdw1 sdw2)
NET=vxmh

on() {	# on HOST COMMAND...: as gpadmin, with Cloudberry's environment
	local h=$1
	shift
	docker exec -u gpadmin -e COORDINATOR_DATA_DIRECTORY=/data0/database/coordinator/gpseg-1 \
		"vxmh-$h" bash -c 'set +u; source /usr/local/cloudberry-db/cloudberry-env.sh; set -u; cd /home/gpadmin/vec_executor; '"$*"
}

root() {	# root HOST COMMAND...
	local h=$1
	shift
	docker exec -u root "vxmh-$h" bash -c "$*"
}

up() {
	docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
	local mounts=(-v "$EXT:/home/gpadmin/vec_executor")
	[ -z "${MH_GPHOME:-}" ] || mounts+=(-v "$MH_GPHOME:/usr/local/cloudberry-db")
	[ -z "${MH_PACKAGE:-}" ] || mounts+=(-v "$MH_PACKAGE:/tmp/vx-package.tar.gz:ro")
	[ -z "${MH_TPCH_KIT:-}" ] || mounts+=(-v "$MH_TPCH_KIT:/home/gpadmin/tpch-kit")
	for h in "${HOSTS[@]}"; do
		docker run -d --name "vxmh-$h" --hostname "$h" --network "$NET" \
			--shm-size 1g --user root "${mounts[@]}" "$IMAGE" sleep infinity >/dev/null
	done
	for h in "${HOSTS[@]}"; do
		root "$h" 'mkdir -p /run/sshd && /usr/sbin/sshd && rm -f /run/nologin
			mkdir -p /data0/database/coordinator /data0/database/primary /data0/database/mirror
			chown -R gpadmin:gpadmin /data0 && chmod -R 700 /data0/database'
		if [ -n "${MH_PACKAGE:-}" ]; then
			root "$h" 'mkdir -p /tmp/vx-package && tar -C /tmp/vx-package --strip-components=1 \
				-xzf /tmp/vx-package.tar.gz && chown -R gpadmin:gpadmin /usr/local/cloudberry-db'
			on "$h" '/tmp/vx-package/install.sh >/dev/null'
		fi
	done
	for h in "${HOSTS[@]}"; do
		on "$h" 'ssh-keyscan -t rsa cdw sdw1 sdw2 > ~/.ssh/known_hosts 2>/dev/null; chmod 600 ~/.ssh/known_hosts'
	done
	on cdw 'for h in cdw sdw1 sdw2; do ssh -o BatchMode=yes $h true; done; echo "ssh between the hosts: ok"'
}

init() {
	on cdw 'cat > /tmp/vxmh-hosts <<EOF
sdw1
sdw2
EOF
cat > /tmp/vxmh-gpinit <<EOF
ARRAY_NAME="vec_executor multihost"
SEG_PREFIX=gpseg
PORT_BASE=40000
declare -a DATA_DIRECTORY=(/data0/database/primary /data0/database/primary)
COORDINATOR_HOSTNAME=cdw
COORDINATOR_DIRECTORY=/data0/database/coordinator
COORDINATOR_PORT=5432
TRUSTED_SHELL=ssh
ENCODING=UNICODE
MIRROR_PORT_BASE=50000
declare -a MIRROR_DATA_DIRECTORY=(/data0/database/mirror /data0/database/mirror)
EOF
LC_ALL=C.UTF-8 LANG=C.UTF-8 gpinitsystem -a -c /tmp/vxmh-gpinit -h /tmp/vxmh-hosts --max_connections=100 \
	|| [ $? -eq 1 ]	# 1: done, with warnings
echo "host all all 0.0.0.0/0 trust" >> $COORDINATOR_DATA_DIRECTORY/pg_hba.conf
gpconfig -c gp_vmem_protect_limit -v 2048 >/dev/null
gpconfig -c gp_fts_probe_interval -v 10 --coordinatoronly >/dev/null
gpstop -ar >/dev/null
psql -X -d postgres -Atc "SELECT content, role, preferred_role, hostname, status FROM gp_segment_configuration ORDER BY content, role"'
}

# Rows of the four configurations agree (bench.py exits 1 if not).
tpch_round() {
	on cdw "python3 tools/tpch/bench.py run --db tpch1 --rounds 1 --warmup 0 --out /tmp/mh-$1.jsonl --label 'multihost $1' | tail -1"
}

test_all() {
	local rc=0
	echo "== 1. engine check"
	on cdw 'psql -X -q -d postgres -c "DROP DATABASE IF EXISTS contrib_regression" -c "CREATE DATABASE contrib_regression"
		python3 tools/engine_check.py --engine datafusion $(sed -n "s/^REGRESS = //p" Makefile) paxdirect' || rc=1
	echo "== 2. TPC-H, four configurations"
	local kit=""
	[ -z "${MH_TPCH_KIT:-}" ] || kit="TPCH_KIT=/home/gpadmin/tpch-kit"
	on cdw "psql -X -q -d postgres -c 'DROP DATABASE IF EXISTS tpch1'; $kit CHUNKS=4 tools/tpch/load.sh 1 tpch1 /tmp/tpch-work | head -1"
	tpch_round before || rc=1
	echo "== 3. stress"
	on cdw "python3 tools/stress.py --db tpch1 --queries tools/tpch/queries --sessions 4 \
		--minutes ${MH_STRESS_MINUTES:-3} 2>&1 | grep -v ^reference | tail -3" || rc=1
	echo "== 4. a primary fails"
	local dir
	dir=$(on cdw 'psql -X -d postgres -Atc "SELECT datadir FROM gp_segment_configuration WHERE hostname = '"'sdw1'"' AND role = '"'p'"' AND content >= 0 ORDER BY content LIMIT 1"')
	on sdw1 "pg_ctl stop -D $dir -m immediate >/dev/null"
	echo "stopped the primary in $dir on sdw1"
	on cdw 'for i in $(seq 60); do
			n=$(psql -X -d postgres -Atc "SELECT count(*) FROM gp_segment_configuration WHERE status = '"'d'"'")
			[ "$n" -gt 0 ] && break; sleep 2; done
		psql -X -d postgres -Atc "SELECT content, role, preferred_role, hostname, status FROM gp_segment_configuration WHERE content >= 0 ORDER BY content, role"'
	tpch_round failover || rc=1
	on cdw 'gprecoverseg -a >/dev/null && for i in $(seq 90); do
			n=$(psql -X -d postgres -Atc "SELECT count(*) FROM gp_segment_configuration WHERE mode <> '"'s'"' OR status <> '"'u'"'")
			[ "$n" -eq 0 ] && break; sleep 2; done
		gprecoverseg -ar >/dev/null && sleep 5
		psql -X -d postgres -Atc "SELECT count(*) AS not_in_place FROM gp_segment_configuration WHERE role <> preferred_role OR status <> '"'u'"'"' || rc=1
	tpch_round recovered || rc=1
	echo "== multihost: $([ $rc = 0 ] && echo ok || echo FAILED)"
	return $rc
}

down() {
	for h in "${HOSTS[@]}"; do docker rm -f "vxmh-$h" >/dev/null 2>&1 || true; done
	docker network rm "$NET" >/dev/null 2>&1 || true
}

case "${1:-}" in
up) up ;;
init) init ;;
test) test_all ;;
down) down ;;
*)
	echo "usage: ci/multihost.sh up|init|test|down" >&2
	exit 2
	;;
esac
