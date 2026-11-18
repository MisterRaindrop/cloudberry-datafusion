#!/bin/bash
#
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
# Print the section of CHANGELOG.md for VERSION (its "## VERSION" heading
# up to the next "## "), without the heading: the notes of a release.
#
# usage: ci/release-notes.sh VERSION
#
set -euo pipefail
v=${1:?usage: release-notes.sh VERSION}
notes=$(awk -v v="$v" '
	/^## / { if (on) exit; if ($2 == v) { on = 1; next } }
	on { print }
' "$(dirname "$0")/../CHANGELOG.md")
if [ -z "$(echo "$notes" | tr -d '[:space:]')" ]; then
	echo "release-notes.sh: CHANGELOG.md has no section \"## $v\"" >&2
	exit 1
fi
echo "$notes" | sed -e '/./,$!d'
