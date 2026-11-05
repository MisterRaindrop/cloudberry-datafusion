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
# vec_executor: vectorized execution backend for Apache Cloudberry,
# built on Apache DataFusion.  Out-of-tree extension, built with PGXS:
#
#   make                          release build (the extension and the
#                                 DataFusion engine, vec_engine_datafusion.so)
#   make DF_CARGO_PROFILE=dev     debug build of the engine
#   make install
#   make installcheck             pg_regress against a running cluster
#   make df-rust-test             Rust unit tests
#   make df-rust-clean            drop engines/datafusion/target (several GB
#                                 once DataFusion is built; deliberately not
#                                 part of `clean`)
#
# Set CARGO_TARGET_DIR to put the Rust build tree elsewhere.
#
# Point PG_CONFIG at the Cloudberry installation to build against:
#   make PG_CONFIG=/usr/local/cloudberry-db/bin/pg_config

MODULE_big = vec_executor
OBJS = src/vx_init.o src/vx_runtime.o src/vx_debug.o \
	src/vx_hooks.o src/vx_plan_check.o src/vx_translate.o src/vx_exec.o \
	src/vx_paxload.o src/vx_numeric.o src/vx_arrow.o src/vx_rewrite.o \
	src/vx_regex.o

EXTENSION = vec_executor
DATA = vec_executor--1.0.sql
PGFILEDESC = "vec_executor - vectorized execution backend on Apache DataFusion"

REGRESS = vec_executor engines runtime hooks exec memory motion receive batches collation joins types strings numeric expressions sort groupingsets window setops distinct partial rewrite storage parallel
# The direct PAX reader is only tested when it is built.
ifdef VX_PAX_SRC
REGRESS += paxdirect
endif
REGRESS_OPTS = --init-file=$(CURDIR)/init_file

# Level 0 of checking an engine (docs/engines.md): the regression tests'
# rows with vec.engine = $(ENGINE) against PostgreSQL's (the null engine).
ENGINE ?= datafusion
.PHONY: installcheck-engine
installcheck-engine:
	python3 tools/engine_check.py --engine $(ENGINE) $(REGRESS)

PG_CPPFLAGS = -Isrc -Iinclude

CARGO ?= cargo
DF_CARGO_PROFILE ?= release
DF_CARGO_FLAGS ?=
ifeq ($(DF_CARGO_PROFILE),dev)
DF_CARGO_OUTDIR = debug
else
DF_CARGO_OUTDIR = $(DF_CARGO_PROFILE)
endif
# Honour CARGO_TARGET_DIR, e.g. to keep the multi-GB build tree off a slow
# bind mount; cargo reads the same variable from the environment.
DF_CARGO_TARGET_DIR = $(if $(CARGO_TARGET_DIR),$(CARGO_TARGET_DIR),$(CURDIR)/engines/datafusion/target)
DF_RUST_LIB = $(DF_CARGO_TARGET_DIR)/$(DF_CARGO_OUTDIR)/libdf_ffi$(DLSUFFIX)
DF_ENGINE = vec_engine_datafusion$(DLSUFFIX)
# The null engine, for the tests: one of this ABI, one claiming version 1.
DF_NULL_ENGINES = vec_engine_null$(DLSUFFIX) vec_engine_null_abi1$(DLSUFFIX)

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# PGXS does not track headers: every object takes the module's and the ABI's,
# so that a change to a struct there rebuilds them all.
$(OBJS): src/vx_executor.h include/vec_engine.h

# Must come after pgxs.mk so PORTNAME is defined.  The version script keeps
# the module's symbols local but for what PostgreSQL looks up.
ifeq ($(PORTNAME),linux)
SHLIB_LINK += -ldl -Wl,--version-script=exports.map
endif

# The engine is a library of its own, which the module opens at first use
# (vec.engine).  Cargo builds it; it exports only vec_engine_entry.
all: $(DF_ENGINE) $(DF_NULL_ENGINES)

.PHONY: df-rust df-rust-test df-rust-clean
df-rust:
	cd engines/datafusion && $(CARGO) build --profile $(DF_CARGO_PROFILE) -p df_ffi $(DF_CARGO_FLAGS)

# The empty recipe makes make look at the library again after cargo has
# run, so that a change to the Rust side alone copies it again.
$(DF_RUST_LIB): df-rust
	@:
$(DF_ENGINE): $(DF_RUST_LIB)
	cp $< $@

df-rust-test:
	cd engines/datafusion && $(CARGO) test --workspace $(DF_CARGO_FLAGS)

df-rust-clean:
	-cd engines/datafusion && $(CARGO) clean

vec_engine_null$(DLSUFFIX): engines/null/vec_engine_null.c include/vec_engine.h
	$(CC) $(CFLAGS) $(CFLAGS_SL) -Iinclude -shared $< -o $@
vec_engine_null_abi1$(DLSUFFIX): engines/null/vec_engine_null.c include/vec_engine.h
	$(CC) $(CFLAGS) $(CFLAGS_SL) -Iinclude -DVEC_NULL_ABI=1 -shared $< -o $@

install: install-engine
.PHONY: install-engine
install-engine: $(DF_ENGINE) $(DF_NULL_ENGINES)
	$(INSTALL_SHLIB) $(DF_ENGINE) $(DF_NULL_ENGINES) '$(DESTDIR)$(pkglibdir)/'

EXTRA_CLEAN += $(DF_ENGINE) $(DF_NULL_ENGINES)

# ---------------------------------------------------------------------------
# Experimental direct PAX reader, vec_pax.so.  Its source is a patch to
# PAX, patches/pax/*.patch, which adds a columnar scan interface
# (access/pax_direct_api.cc).  Built only when VX_PAX_SRC names the
# pax_storage C++ sources (contrib/pax_storage/src/cpp) of the Cloudberry tree
# the installed pax.so was built from: they are copied to pax_build/, patched,
# and the files the patches add are compiled into vec_pax.so.  The
# Cloudberry tree itself and the installed pax.so are left untouched.  The
# installed PAX headers come first, they include the generated protobuf ones.
# The library records the installed pax.so's build ID and is only used with it.
# ---------------------------------------------------------------------------
ifdef VX_PAX_SRC
VX_PAX_SO := $(pkglibdir)/pax.so
VX_PAX_BUILD_ID := $(shell readelf -n $(VX_PAX_SO) 2>/dev/null | awk '/Build ID/ {print $$3}')
VX_PAX_PATCHES := $(sort $(wildcard patches/pax/*.patch))
VX_PAX_TREE := pax_build/src/cpp
VX_PAX_API_SRC := $(VX_PAX_TREE)/access/pax_direct_api.cc
CXX ?= g++

all: vec_pax.so

# The patches are against the Cloudberry tree; -p5 strips
# a/contrib/pax_storage/src/cpp.
pax_build/.patched: $(VX_PAX_PATCHES)
	rm -rf pax_build
	mkdir -p $(dir $(VX_PAX_TREE))
	cp -R $(VX_PAX_SRC) $(VX_PAX_TREE)
	for p in $(VX_PAX_PATCHES); do \
		patch -s -p5 --forward -d $(VX_PAX_TREE) < $$p || exit 1; \
	done
	touch $@

# The empty recipe makes make look at the source again once the patches
# have rewritten it, so that one run rebuilds the library.
$(VX_PAX_API_SRC): pax_build/.patched
	@:

# -Bsymbolic keeps the library's own references inside it, even if a loaded
# pax.so ever exports the same names.
vec_pax.so: $(VX_PAX_API_SRC)
	@test -n "$(VX_PAX_BUILD_ID)" || { echo "cannot read the build ID of $(VX_PAX_SO)"; exit 1; }
	$(CXX) -std=c++17 -O2 -fPIC -shared -fvisibility=hidden -Wall -DUSE_PAX_CATALOG \
		-DVX_PAX_BUILD_ID='"$(VX_PAX_BUILD_ID)"' -Wl,-Bsymbolic \
		-I$(includedir)/pax -I$(VX_PAX_TREE) -I$(includedir_server) -I$(includedir_internal) \
		$< -o $@

install: install-pax
.PHONY: install-pax
install-pax: vec_pax.so
	$(INSTALL_SHLIB) vec_pax.so '$(DESTDIR)$(pkglibdir)/'

EXTRA_CLEAN += vec_pax.so pax_build
endif
