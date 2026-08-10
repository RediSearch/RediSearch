#-----------------------------------------------------------------------------
# RediSearch Makefile
#
# This Makefile acts as a thin wrapper around the build.sh script, providing
# backward compatibility for existing make targets while using build.sh for
# all actual build operations.
#-----------------------------------------------------------------------------

.NOTPARALLEL:
.EXPORT_ALL_VARIABLES:

MAKEFLAGS += --no-print-directory

ROOT := $(shell pwd)
BUILD_SCRIPT := $(ROOT)/build.sh

export PATH := $(HOME)/.cargo/bin:$(HOME)/.local/bin:$(PATH)

# Default target
.DEFAULT_GOAL := build

# Ensure build.sh is executable
$(BUILD_SCRIPT):
	@chmod +x $(BUILD_SCRIPT)

#-----------------------------------------------------------------------------
# Build script argument construction
#-----------------------------------------------------------------------------

# Convert Makefile variables to build.sh arguments
BUILD_ARGS :=

# Coordinator type
ifeq ($(COORD),1)
	override COORD := oss
else ifeq ($(COORD),)
	override COORD := oss
endif
BUILD_ARGS += COORD=$(COORD)

# Build flags
ifeq ($(DEBUG),1)
	BUILD_ARGS += DEBUG
endif

ifneq ($(ENABLE_ASSERT),)
	BUILD_ARGS += ENABLE_ASSERT=$(ENABLE_ASSERT)
endif

ifeq ($(PROFILE),1)
	BUILD_ARGS += PROFILE
endif

ifeq ($(TESTS),1)
	BUILD_ARGS += TESTS
endif

ifeq ($(FORCE),1)
	BUILD_ARGS += FORCE
endif

ifeq ($(VERBOSE),1)
	BUILD_ARGS += VERBOSE
endif

ifneq ($(SAN),)
	BUILD_ARGS += SAN=$(SAN)
endif

ifneq ($(MAX_WORKER_THREADS),)
	BUILD_ARGS += MAX_WORKER_THREADS=$(MAX_WORKER_THREADS)
endif

ifeq ($(COV),1)
	BUILD_ARGS += COV=1
endif

ifneq ($(RUST_PROFILE),)
	BUILD_ARGS += RUST_PROFILE=$(RUST_PROFILE)
endif

ifeq ($(RUST_DYN_CRT),1)
	BUILD_ARGS += RUST_DYN_CRT=1
	# Export so the `generate-rust-headers` recipe (which invokes
	# regen_headers.sh directly, bypassing build.sh) can apply
	# `-C target-feature=-crt-static` to its cargo invocations.
	export RUST_DYN_CRT
endif

ifeq ($(RUN_MIRI),1)
	BUILD_ARGS += RUN_MIRI=1
endif

ifeq ($(RUST_DENY_WARNS),1)
	BUILD_ARGS += RUST_DENY_WARNS=1
endif

# Test arguments
ifneq ($(TEST),)
	BUILD_ARGS += TEST=$(TEST)
endif

ifeq ($(QUICK),1)
	BUILD_ARGS += QUICK=1
endif

# If SA is set but REDIS_STANDALONE is not, use SA as REDIS_STANDALONE
ifneq ($(SA),)
ifeq ($(REDIS_STANDALONE),)
    override REDIS_STANDALONE := $(SA)
endif
endif

# Pass REDIS_STANDALONE to build script (SA is handled as fallback in test scripts)
ifneq ($(REDIS_STANDALONE),)
    BUILD_ARGS += REDIS_STANDALONE=$(REDIS_STANDALONE)
endif

ifeq ($(LTO),1)
	BUILD_ARGS += LTO
endif

ifneq ($(INLINE_LSE_ATOMICS),)
	BUILD_ARGS += INLINE_LSE_ATOMICS=$(INLINE_LSE_ATOMICS)
endif

# Package variables (used by pack target)
MODULE_NAME := search
PACKAGE_NAME ?=
RAMP_VARIANT ?=
RAMP_ARGS ?=

# Set RAMP_VARIANT and PACKAGE_NAME based on COORD if not explicitly set
ifeq ($(RAMP_VARIANT),)
ifeq ($(COORD),rlec)
	override RAMP_VARIANT := enterprise
	override PACKAGE_NAME := redisearch
else
	override RAMP_VARIANT := community
	override PACKAGE_NAME := redisearch-community
endif
endif

#-----------------------------------------------------------------------------
# Main targets
#-----------------------------------------------------------------------------

define HELPTEXT
RediSearch Build System

Setup:
  make bootstrap     Install build- and test-time system dependencies.
                     Auto-prefixes `sudo` when not root.
    SUDO=cmd           Override the privilege-escalation command (default: auto)
  make fetch         Download and prepare dependent modules

Build:
  make build         Compile and link
    COORD=oss|rlec     Build coordinator (default: oss)
    DEBUG=1            Build for debugging
    PROFILE=1          Build with profiling support
    TESTS=1            Build unit tests
    FORCE=1            Force clean build
    SAN=type           Build with sanitizer (address|memory|leak|thread)
    COV=1              Build with coverage instrumentation
    RUST_PROFILE=name  Rust profile to use (default: release)
    RUST_DYN_CRT=1     Use dynamic C runtime linking (for Alpine Linux)
    VERBOSE=1          Verbose build output
    LTO=1              Enable Rust/C LTO
    INLINE_LSE_ATOMICS=0|1
                       Inline LSE atomics on Linux AArch64 (default: 1).
                       Set to 0 on pre-Armv8.1-a cores (Cortex-A72,
                       Graviton1, Raspberry Pi 4) to avoid SIGILL on load.

  make clean         Remove build artifacts and stray *.profraw files
    ALL=1              Also remove the whole artifacts directory, which holds
                       the cargo target dir, plus any stray
                       src/redisearch_rs/target

Testing:
  make test          Run all tests
  make unit-tests    Run unit tests (C and C++)
  make rust-tests    Run Rust tests
    RUN_MIRI=1            Run Rust tests through miri to catch undefined behavior
    RUST_DENY_WARNS=1     Deny all Rust compiler warnings
    RUST_DYN_CRT=1        Use dynamic C runtime linking (for Alpine Linux)
  make pytest        Run Python tests
    COORD=oss|rlec        Test coordinator type (default: oss)
    REDIS_STANDALONE=1|0  Test with standalone/cluster Redis
    SA=1|0                Alias for REDIS_STANDALONE
    TEST=name             Run specified test
    QUICK=1               Run quick test subset

Development:
  make run           Run Redis with RediSearch
    COORD=oss|rlec     Run with coordinator type (default: oss)
    WITH_RLTEST=1      Run using RLTest framework
    GDB=1              Invoke using gdb
    CLANG=1            Use lldb instead of gdb (when GDB=1)
  make lint          Run linters
  make fmt           Format source files
    CHECK=1            Check formatting without modifying files
  make swamp-tests   Check formatting and run the swamp extension model tests
    SWAMP_DENO=path    Use a specific deno binary

Packaging:
  make pack          Create installation packages
    RAMP_VARIANT=name  Use specific RAMP variant (community|enterprise)
                       Default: community for oss, enterprise for rlec

Benchmarks:
  make benchmark        Run performance benchmarks
  make micro-benchmarks Run micro-benchmarks
  make vecsim-bench     Run VecSim micro-benchmarks

Documentation:
  make check-links         Check all links in Markdown files (failures only)
  make check-links-verbose Check all links in Markdown files (show all)
  make test-linkcheck      Test the link checker functionality
endef # HELPTEXT

help:
	$(info $(HELPTEXT))
	@:

# Auto-detect: empty when running as root (containers/CI), "sudo" otherwise.
# Override with SUDO= for environments that need a different prefix (e.g. doas)
# or to force no prefix.
SUDO ?= $(shell [ "$$(id -u)" -eq 0 ] || echo sudo)

# `list` / `dry-run` route through the REAL installer (install_script.sh),
# same as the other modules: CHECK_DEPS=1 records present/missing deps and
# installs nothing; DRY_RUN=1 prints the exact commands bootstrap would run for
# missing deps and installs nothing.
bootstrap:
ifeq ($(filter list,$(MAKECMDGOALS)),list)
	@cd $(ROOT)/.install && CHECK_DEPS=1 ./install_script.sh $(SUDO)
else ifeq ($(filter dry-run,$(MAKECMDGOALS)),dry-run)
	@cd $(ROOT)/.install && DRY_RUN=1 ./install_script.sh $(SUDO)
else
	@echo "Installing build dependencies..."
	@cd $(ROOT)/.install && ./install_script.sh $(SUDO)
endif

list: ; @:
dry-run: ; @:
bootstrap-modes: ; @echo "list dry-run"

fetch:
	@echo "Fetching dependencies..."
	@git submodule update --init --recursive

build: $(BUILD_SCRIPT) verify-deps
	@echo "Building RediSearch..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS)

verify-deps:
	@echo "Verifying build dependencies..."
	@if ! $(ROOT)/.install/verify_build_deps.sh; then \
		if [ "$(IGNORE_MISSING_DEPS)" = "1" ]; then \
			echo -e "\033[0;33mIGNORE_MISSING_DEPS is set. Ignoring dependency check failure.\033[0m"; \
		else \
			echo ""; \
			echo -e "\033[0;31mDependency check failed. You can bypass this check by running:\033[0m"; \
			echo -e "\033[0;31m\033[1mmake IGNORE_MISSING_DEPS=1 ...\033[0m"; \
			exit 1; \
		fi; \
	fi

clean:
ifeq ($(ALL),1)
	@echo "Cleaning all build artifacts..."
	@rm -rf $(ROOT)/bin $(ROOT)/src/redisearch_rs/target
else
	@echo "Cleaning build artifacts..."
	@rm -rf $(ROOT)/bin/*/search-*
endif
# An instrumented binary drops one .profraw per process into its working
# directory. cargo-llvm-cov keeps its own under the cargo target dir, which
# src/redisearch_rs/.cargo/config.toml points into bin/, but an ad-hoc
# instrumented cargo run scatters them through the source tree instead.
# Best-effort: an unwritable leftover must not fail the target.
	@echo "Removing stray LLVM coverage profiles..."
	@-find $(ROOT) -name '*.profraw' -type f -delete

test: $(BUILD_SCRIPT)
	@echo "Running all tests..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_TESTS

unit-tests: $(BUILD_SCRIPT)
	@echo "Running unit tests..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_UNIT_TESTS

rust-tests: $(BUILD_SCRIPT)
	@echo "Running Rust tests..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_RUST_TESTS

archive-rust-tests: $(BUILD_SCRIPT)
	@echo "Archiving Rust tests into a nextest archive at $$RUST_TEST_ARCHIVE_PATH..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) ARCHIVE_RUST_TESTS

rust-tests-from-archive: $(BUILD_SCRIPT)
	@echo "Running Rust tests from nextest archive at $$RUST_TEST_ARCHIVE_PATH..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_ARCHIVED_RUST_TESTS

pytest: $(BUILD_SCRIPT)
	@echo "Running Python tests..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_PYTEST

parsers:
ifeq ($(FORCE),1)
	@cd src/aggregate/expr && rm -f lexer.c parser.c
	@$(MAKE) -C src/query_parser/v1 clean
	@$(MAKE) -C src/query_parser/v2 clean
endif
	@$(MAKE) -C src/aggregate/expr
	@$(MAKE) -C src/query_parser/v1
	@$(MAKE) -C src/query_parser/v2

run:
	@find_module() { \
		if [ "$(COORD)" = "rlec" ]; then \
			MODULE_PATH=$$(find $(ROOT)/bin -name "module-enterprise.so" | head -1); \
			if [ -z "$$MODULE_PATH" ]; then \
				echo "Error: No enterprise module found. Please build first with 'make build COORD=rlec'"; \
				exit 1; \
			fi; \
		else \
			MODULE_PATH=$$(find $(ROOT)/bin -name "redisearch.so" | head -1); \
			if [ -z "$$MODULE_PATH" ]; then \
				echo "Error: No community module found. Please build first with 'make build COORD=oss'"; \
				exit 1; \
			fi; \
		fi; \
		echo "Using module: $$MODULE_PATH"; \
	}; \
	if [ "$(WITH_RLTEST)" = "1" ]; then \
		echo "Starting Redis with RediSearch using RLTest..."; \
		find_module; \
		REJSON=$(REJSON) REJSON_PATH=$(REJSON_PATH) REJSON_BRANCH=$(REJSON_BRANCH) REJSON_ARGS=$(REJSON_ARGS) \
		FORCE='' RLTEST= ENV_ONLY=1 LOG_LEVEL=$(LOG_LEVEL) MODULE=$(MODULE) REDIS_STANDALONE=$(REDIS_STANDALONE) SA=$(SA) \
		$(ROOT)/tests/pytests/runtests.sh "$$MODULE_PATH"; \
	else \
		echo "Starting Redis with RediSearch..."; \
		find_module; \
		if [ "$(GDB)" = "1" ]; then \
			echo "Starting with GDB..."; \
			if [ "$(CLANG)" = "1" ]; then \
				lldb -o run -- redis-server --loadmodule "$$MODULE_PATH"; \
			else \
				gdb -ex r --args redis-server --loadmodule "$$MODULE_PATH"; \
			fi; \
		else \
			redis-server --loadmodule "$$MODULE_PATH"; \
		fi; \
	fi

# Function to extract EXCLUDE_RUST_BENCHING_CRATES_LINKING_C from build.sh
define get_rust_exclude_crates
$(shell grep "EXCLUDE_RUST_BENCHING_CRATES_LINKING_C=" build.sh | cut -d'=' -f2 | tr -d '"' | head -n1)
endef

# Regenerate the Rust → C FFI headers under src/redisearch_rs/headers/.
#
# The recipe (cheadergen CLI args + env scrub) lives in
# src/redisearch_rs/regen_headers.sh; the CMake `cheadergen_generate`
# custom target calls the same script. See the script for the rationale
# on the env scrub.
generate-rust-headers:
	@echo "Regenerating Rust → C FFI headers via cheadergen..."
	@$(ROOT)/src/redisearch_rs/regen_headers.sh

lint: generate-rust-headers
	@echo "Running linters for debug..."
	@cd $(ROOT)/src/redisearch_rs && cargo clippy --workspace $(call get_rust_exclude_crates) -- -D warnings
	@cd $(ROOT)/src/redisearch_rs && RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace $(call get_rust_exclude_crates) --no-deps --document-private-items
	@echo "Running linters for release..."
	@cd $(ROOT)/src/redisearch_rs && cargo clippy --workspace $(call get_rust_exclude_crates) --release -- -D warnings
	@cd $(ROOT)/src/redisearch_rs && RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace $(call get_rust_exclude_crates) --no-deps --document-private-items --release

fmt:
ifeq ($(CHECK),1)
	@echo "Checking code formatting..."
	@cd $(ROOT)/src/redisearch_rs && cargo fmt --check --all
else
	@echo "Formatting code..."
	@cd $(ROOT)/src/redisearch_rs && cargo fmt --all
endif

license-check:
	@echo "Checking license headers..."
	@cd $(ROOT)/src/redisearch_rs && cargo license-check

# Deno for the swamp extension tests. Prefer the copy swamp bundles, which is
# not on PATH, and fall back to one the environment provides (CI installs it).
SWAMP_DENO ?= $(shell if [ -x "$(HOME)/.swamp/deno/deno" ]; then \
	echo "$(HOME)/.swamp/deno/deno"; else command -v deno; fi)

# The deno half: everything that gates a pull request. Kept separate from the
# definition check below so that it can be run from inside a swamp workflow,
# where invoking `swamp` again would be a swamp run driving a swamp run.
swamp-extension-tests:
ifeq ($(strip $(SWAMP_DENO)),)
	@echo "No deno found. Install swamp, or install deno and put it on PATH," >&2
	@echo "or point at one explicitly: make swamp-tests SWAMP_DENO=/path/to/deno" >&2
	@exit 1
else
	@echo "Checking swamp extension formatting..."
	@cd $(ROOT)/extensions && $(SWAMP_DENO) fmt --check models/ reports/
	@echo "Running swamp extension tests..."
# Not --allow-all, which additionally disables the sandbox. What is left is what
# the suite needs and no more: temp directories to work in (read/write), the fake
# executables the execute tests drive instead of real builds (run), and the
# environment those fakes are configured through (env).
#
# `--allow-run` is the one that cannot be narrowed: the fakes are generated per
# test into temporary directories, so there is no set of paths to allow, and a
# subprocess runs outside the sandbox whatever it is. A test could therefore
# still spawn something that reaches the network. That is why the CI job running
# this holds nothing worth reaching — see the swamp-tests job in
# .github/workflows/task-lint.yml, which has `contents: read` and no more. The
# job is the boundary; these flags only keep the default path narrow.
	@cd $(ROOT)/extensions && $(SWAMP_DENO) test \
		--allow-read --allow-write --allow-run --allow-env models/ reports/
endif

# The tests above cover the model sources. This covers the definitions that are
# actually invoked: an instance or a workflow can be malformed, or reference an
# argument or expression path that does not exist, while every TypeScript test
# passes — and the first anyone would hear of it is a workflow refusing to run.
#
# The work list is built by walking the files, not by grepping for the field
# being validated. Deriving it from `name:` means a definition whose name is
# missing or misspelled contributes no entry at all — so the one file that is
# certainly broken is the one nothing checks. Walking the files makes a missing
# name a failure in its own right.
#
# Warnings are failures here. `swamp workflow validate` reports a step naming a
# model that does not exist as a warning and still exits 0, which is exactly the
# breakage this is meant to catch.
#
# Workflows are evaluated as well as validated. Validation is structural — schema,
# dependencies, whether a step's inputs cover the method's required arguments —
# and says nothing about the expressions.
#
# Evaluation is not a gate on its own: swamp reports an expression it could not
# resolve as a *warning* and exits 0, so `swamp workflow evaluate` succeeds on a
# workflow whose guard refers to `inputs.coverageFilse`. The gate script reads
# those warnings. It cannot simply fail on all of them — evaluating without
# inputs warns about every declared input that has no value, and `run` and the
# async data lookups do not exist until there is a run — so it fails only on
# warnings that are none of those, which is what a typo looks like.
#
# What it cannot see: anything inside an expression that reads `data`, because
# those are never resolved at evaluation. Those are left to the run.
#
# Skipped rather than failed when swamp is absent, so that a contributor without
# it can still run the rest — but not in CI, where skipping would be silent and
# this is the only gate that reads the definitions at all. A pull request that
# changes nothing but `workflows/` would otherwise pass every check while
# carrying a guard that cannot evaluate. Set SWAMP_REQUIRED=1 to turn the skip
# into a failure; the CI job that installs swamp does.
swamp-definitions-check:
	@if command -v swamp >/dev/null 2>&1; then \
		echo "Validating swamp model and workflow definitions..."; \
		entries=""; \
		for file in $(ROOT)/models/*/*/*.yaml $(ROOT)/workflows/*.yaml; do \
			case "$$file" in *"/workflows/"*) kind=workflow;; *) kind=model;; esac; \
			name="$$(sed -n 's/^name: *//p' "$$file" | head -1)"; \
			if [ -z "$$name" ]; then \
				echo "$$file has no top-level name; nothing would validate it." >&2; \
				exit 1; \
			fi; \
			entries="$$entries $$kind:$$name"; \
		done; \
		for entry in $$entries; do \
			kind="$${entry%%:*}"; name="$${entry#*:}"; \
			out="$$(swamp $$kind validate "$$name" --repo-dir $(ROOT) 2>&1)" || \
				{ echo "$$out" >&2; exit 1; }; \
			case "$$out" in \
				*warning*|*Warning*|*WARNING*) \
					echo "$$out" >&2; \
					echo "$$kind $$name validated with warnings; treating as a failure." >&2; \
					exit 1;; \
			esac; \
		done; \
		python3 $(ROOT)/scripts/swamp_evaluate_gate.py $(ROOT); \
	elif [ -n "$(SWAMP_REQUIRED)" ]; then \
		echo "swamp is not on PATH and SWAMP_REQUIRED is set." >&2; \
		echo "This is the only gate that reads the workflow definitions, so" >&2; \
		echo "skipping it would let a broken guard or input expression through." >&2; \
		exit 1; \
	else \
		echo "swamp not on PATH; skipping model and workflow validation."; \
	fi

# Definitions first, deliberately. The extension tests are PR-controlled code
# running with --allow-write and --allow-run, so a test file could edit
# workflows/ or models/ — or replace the swamp binary on PATH — and the check
# that reads those definitions would then be validating whatever the tests left
# behind. Validating first means the definitions are checked as they arrived.
#
# Make runs prerequisites left to right for a serial build, which is what CI
# does; the CI job also runs the two as separate steps so the order does not
# depend on that.
swamp-tests: swamp-definitions-check swamp-extension-tests

pack: build
	@echo "Creating installation packages..."
	@if [ -z "$(MODULE_PATH)" ]; then \
		if [ "$(COORD)" = "rlec" ]; then \
			MODULE_PATH=$$(find $(ROOT)/bin -name "module-enterprise.so" | head -1); \
			if [ -z "$$MODULE_PATH" ]; then \
				echo "Error: No enterprise module found. Please build first with 'make build COORD=rlec'"; \
				exit 1; \
			fi; \
		else \
			MODULE_PATH=$$(find $(ROOT)/bin -name "redisearch.so" | head -1); \
			if [ -z "$$MODULE_PATH" ]; then \
				echo "Error: No community module found. Please build first with 'make build COORD=oss'"; \
				exit 1; \
			fi; \
		fi; \
		echo "Using module: $$MODULE_PATH"; \
	else \
		MODULE_PATH="$(MODULE_PATH)"; \
		echo "Using specified module: $$MODULE_PATH"; \
	fi; \
	if command -v python3 >/dev/null 2>&1 && python3 -c "import RAMP.ramp" >/dev/null 2>&1; then \
		echo "RAMP is available, creating RAMP packages..."; \
		RAMP=1 COORD=$(COORD) PACKAGE_NAME=$(PACKAGE_NAME) MODULE_NAME=$(MODULE_NAME) \
		RAMP_VARIANT=$(RAMP_VARIANT) RAMP_ARGS=$(RAMP_ARGS) \
		$(ROOT)/sbin/pack.sh "$$MODULE_PATH"; \
	else \
		echo "RAMP not available, skipping RAMP package creation..."; \
		echo "To install RAMP: pip install -r ./.install/build_package_requirements.txt"; \
	fi

upload-artifacts:
	@echo "Uploading artifacts..."
	@$(ROOT)/sbin/upload-artifacts

benchmark: build
	@echo "Running benchmarks..."
	@find_module() { \
		if [ "$(COORD)" = "rlec" ]; then \
			MODULE_PATH=$$(find $(ROOT)/bin -name "module-enterprise.so" | head -1); \
			if [ -z "$$MODULE_PATH" ]; then \
				echo "Error: No enterprise module found. Please build first with 'make build COORD=rlec'"; \
				exit 1; \
			fi; \
		else \
			MODULE_PATH=$$(find $(ROOT)/bin -name "redisearch.so" | head -1); \
			if [ -z "$$MODULE_PATH" ]; then \
				echo "Error: No community module found. Please build first with 'make build COORD=oss'"; \
				exit 1; \
			fi; \
		fi; \
		echo "Using module: $$MODULE_PATH"; \
		cd tests/benchmarks && redisbench-admin run-local --module_path "$$MODULE_PATH" --required-module search; \
	}; \
	find_module

micro-benchmarks: $(BUILD_SCRIPT)
	@echo "Running micro-benchmarks..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_MICRO_BENCHMARKS

vecsim-bench: $(BUILD_SCRIPT)
	@echo "Running VecSim micro-benchmarks..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) TESTS
	@RSBENCH_PATH=$$(find $(ROOT)/bin -name "rsbench" | head -1); \
	if [ -z "$$RSBENCH_PATH" ]; then \
		echo "Error: rsbench executable not found after build"; \
		exit 1; \
	fi; \
	echo "Running rsbench from $$RSBENCH_PATH"; \
	$$RSBENCH_PATH

callgrind:
	@echo "Running callgrind profiling..."
	@valgrind --tool=callgrind --dump-instr=yes --simulate-cache=no \
		--collect-jumps=yes --collect-atstart=yes --collect-systime=yes \
		--instr-atstart=yes -v redis-server --protected-mode no \
		--save "" --appendonly no \
		--loadmodule $$(find $(ROOT)/bin -name "redisearch.so" -o -name "module-enterprise.so" | head -1)

check-links:
	@echo "Checking links in Markdown files..."
	@if [ ! -f scripts/requirements-linkcheck.txt ]; then \
		echo "Error: scripts/requirements-linkcheck.txt not found"; \
		exit 1; \
	fi
	@if ! python3 -c "import requests, bs4" 2>/dev/null; then \
		echo "Installing link checker dependencies..."; \
		uv pip install -r scripts/requirements-linkcheck.txt; \
	fi
	@python3 scripts/check_links.py .

check-links-verbose:
	@echo "Checking links in Markdown files (verbose mode)..."
	@if [ ! -f scripts/requirements-linkcheck.txt ]; then \
		echo "Error: scripts/requirements-linkcheck.txt not found"; \
		exit 1; \
	fi
	@if ! python3 -c "import requests, bs4" 2>/dev/null; then \
		echo "Installing link checker dependencies..."; \
		uv pip install -r scripts/requirements-linkcheck.txt; \
	fi
	@python3 scripts/check_links.py . --verbose

test-linkcheck:
	@echo "Testing link checker functionality..."
	@if ! python3 -c "import requests, bs4" 2>/dev/null; then \
		echo "Installing link checker dependencies..."; \
		uv pip install -r scripts/requirements-linkcheck.txt; \
	fi
	@python3 scripts/test_link_checker.py

.PHONY: list dry-run bootstrap-modes help bootstrap fetch build clean test unit-tests rust-tests archive-rust-tests rust-tests-from-archive pytest
.PHONY: run lint fmt swamp-tests swamp-extension-tests swamp-definitions-check license-check pack upload-artifacts
.PHONY: benchmark micro-benchmarks vecsim-bench callgrind parsers verify-deps
.PHONY: check-links check-links-verbose test-linkcheck
