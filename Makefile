#-----------------------------------------------------------------------------
# RediSearch Makefile
#
# This Makefile acts as a thin wrapper around the build.sh script, providing
# backward compatibility for existing make targets while using build.sh for
# all actual build operations.
#-----------------------------------------------------------------------------

.NOTPARALLEL:

MAKEFLAGS += --no-print-directory

ROOT := $(shell pwd)
BUILD_SCRIPT := $(ROOT)/build.sh

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

ifeq ($(COV),1)
	BUILD_ARGS += COV=1
endif

ifneq ($(RUST_PROFILE),)
	BUILD_ARGS += RUST_PROFILE=$(RUST_PROFILE)
endif

ifeq ($(RUST_DYN_CRT),1)
	BUILD_ARGS += RUST_DYN_CRT=1
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

  make clean         Remove build artifacts
    ALL=1              Remove entire artifacts directory

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
	@rm -rf $(ROOT)/bin
else
	@echo "Cleaning build artifacts..."
	@rm -rf $(ROOT)/bin/*/search-*
endif

test: $(BUILD_SCRIPT)
	@echo "Running all tests..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_TESTS

unit-tests: $(BUILD_SCRIPT)
	@echo "Running unit tests..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_UNIT_TESTS

rust-tests: $(BUILD_SCRIPT)
	@echo "Running Rust tests..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_RUST_TESTS

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

lint:
	@echo "Running linters..."
	@cd $(ROOT)/src/redisearch_rs && cargo clippy --workspace $(call get_rust_exclude_crates) -- -D warnings
	@cd $(ROOT)/src/redisearch_rs && RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace $(call get_rust_exclude_crates)

fmt:
ifeq ($(CHECK),1)
	@echo "Checking code formatting..."
	@cd $(ROOT)/src/redisearch_rs && cargo fmt -- --check
else
	@echo "Formatting code..."
	@cd $(ROOT)/src/redisearch_rs && cargo fmt
endif

license-check:
	@echo "Checking license headers..."
	@cd $(ROOT)/src/redisearch_rs && cargo license-check

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
		echo "To install RAMP: pip install -r ./.install/build_package_requirments.txt"; \
	fi

upload-artifacts:
	@echo "Uploading artifacts..."
	@$(ROOT)/sbin/upload-artifacts

benchmark:
	@echo "Running benchmarks..."
	@cd tests/benchmarks && redisbench-admin run-local

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

.PHONY: help build clean test unit-tests rust-tests pytest
.PHONY: run lint fmt license-check pack upload-artifacts
.PHONY: benchmark micro-benchmarks vecsim-bench callgrind parsers verify-deps
.PHONY: check-links check-links-verbose test-linkcheck
