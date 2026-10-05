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
# datafusion_executor: vectorized execution backend for Apache Cloudberry,
# built on Apache DataFusion.  Out-of-tree extension, built with PGXS:
#
#   make                          release build (Rust crate + C shim)
#   make DF_CARGO_PROFILE=dev     debug build of the Rust crate
#   make install
#   make installcheck             pg_regress against a running cluster
#   make df-rust-test             Rust unit tests
#   make df-rust-clean            drop rust/target (several GB once DataFusion
#                                 is built; deliberately not part of `clean`)
#
# Set CARGO_TARGET_DIR to put the Rust build tree elsewhere.
#
# Point PG_CONFIG at the Cloudberry installation to build against:
#   make PG_CONFIG=/usr/local/cloudberry-db/bin/pg_config

MODULE_big = datafusion_executor
OBJS = src/df_init.o src/df_runtime.o src/df_debug.o \
	src/df_hooks.o src/df_plan_check.o src/df_translate.o src/df_exec.o \
	src/df_paxload.o

EXTENSION = datafusion_executor
DATA = datafusion_executor--1.0.sql
PGFILEDESC = "datafusion_executor - vectorized execution backend on Apache DataFusion"

REGRESS = datafusion_executor runtime hooks exec memory motion receive batches storage parallel
# The direct PAX reader is only tested when it is built.
ifdef DF_PAX_SRC
REGRESS += paxdirect
endif
REGRESS_OPTS = --init-file=$(CURDIR)/init_file

PG_CPPFLAGS = -Isrc

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
DF_CARGO_TARGET_DIR = $(if $(CARGO_TARGET_DIR),$(CARGO_TARGET_DIR),$(CURDIR)/rust/target)
DF_RUST_LIB = $(DF_CARGO_TARGET_DIR)/$(DF_CARGO_OUTDIR)/libdf_ffi.a

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Must come after pgxs.mk so PORTNAME is defined.  The library list is what
# `cargo rustc -- --print native-static-libs` reports for a Rust staticlib on
# glibc.  The version script keeps every Rust symbol local, so the shared
# object exports only what PostgreSQL looks up.
SHLIB_LINK += $(DF_RUST_LIB)
ifeq ($(PORTNAME),linux)
SHLIB_LINK += -lgcc_s -lutil -lrt -lpthread -lm -ldl
SHLIB_LINK += -Wl,--version-script=exports.map
else
SHLIB_LINK += -lpthread -lm
endif

.PHONY: df-rust df-rust-test df-rust-clean
df-rust:
	cd rust && $(CARGO) build --profile $(DF_CARGO_PROFILE) -p df_ffi $(DF_CARGO_FLAGS)

$(DF_RUST_LIB): df-rust
$(shlib): $(DF_RUST_LIB)

df-rust-test:
	cd rust && $(CARGO) test --workspace $(DF_CARGO_FLAGS)

df-rust-clean:
	-cd rust && $(CARGO) clean

# ---------------------------------------------------------------------------
# Experimental direct PAX reader, datafusion_pax.so.  Its source is a patch to
# PAX, patches/pax/*.patch, which adds a columnar scan interface
# (access/datafusion_scan_api.cc).  Built only when DF_PAX_SRC names the
# pax_storage C++ sources (contrib/pax_storage/src/cpp) of the Cloudberry tree
# the installed pax.so was built from: they are copied to pax_build/, patched,
# and the files the patches add are compiled into datafusion_pax.so.  The
# Cloudberry tree itself and the installed pax.so are left untouched.  The
# installed PAX headers come first, they include the generated protobuf ones.
# The library records the installed pax.so's build ID and is only used with it.
# ---------------------------------------------------------------------------
ifdef DF_PAX_SRC
DF_PAX_SO := $(pkglibdir)/pax.so
DF_PAX_BUILD_ID := $(shell readelf -n $(DF_PAX_SO) 2>/dev/null | awk '/Build ID/ {print $$3}')
DF_PAX_PATCHES := $(sort $(wildcard patches/pax/*.patch))
DF_PAX_TREE := pax_build/src/cpp
DF_PAX_API_SRC := $(DF_PAX_TREE)/access/datafusion_scan_api.cc
CXX ?= g++

all: datafusion_pax.so

# The patches are against the Cloudberry tree; -p5 strips
# a/contrib/pax_storage/src/cpp.
pax_build/.patched: $(DF_PAX_PATCHES)
	rm -rf pax_build
	mkdir -p $(dir $(DF_PAX_TREE))
	cp -R $(DF_PAX_SRC) $(DF_PAX_TREE)
	for p in $(DF_PAX_PATCHES); do \
		patch -s -p5 --forward -d $(DF_PAX_TREE) < $$p || exit 1; \
	done
	touch $@

$(DF_PAX_API_SRC): pax_build/.patched

# -Bsymbolic keeps the library's own references inside it, even if a loaded
# pax.so ever exports the same names.
datafusion_pax.so: $(DF_PAX_API_SRC)
	@test -n "$(DF_PAX_BUILD_ID)" || { echo "cannot read the build ID of $(DF_PAX_SO)"; exit 1; }
	$(CXX) -std=c++17 -O2 -fPIC -shared -fvisibility=hidden -Wall -DUSE_PAX_CATALOG \
		-DDF_PAX_BUILD_ID='"$(DF_PAX_BUILD_ID)"' -Wl,-Bsymbolic \
		-I$(includedir)/pax -I$(DF_PAX_TREE) -I$(includedir_server) -I$(includedir_internal) \
		$< -o $@

install: install-pax
.PHONY: install-pax
install-pax: datafusion_pax.so
	$(INSTALL_SHLIB) datafusion_pax.so '$(DESTDIR)$(pkglibdir)/'

EXTRA_CLEAN += datafusion_pax.so pax_build
endif
