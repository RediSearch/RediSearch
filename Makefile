
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
PACKAGE_NAME ?= redisearch-oss
MODULE_NAME ?= search
RAMP_YAML ?=
RAMP_ARGS ?=

#-----------------------------------------------------------------------------
# Main targets
#-----------------------------------------------------------------------------

define HELPTEXT
RediSearch Build System

Setup:
  make setup         Install prerequisites (CAUTION: modifies system)
  make fetch         Download and prepare dependent modules

Build:
  make build         Compile and link
    COORD=oss|rlec     Build coordinator (default: oss)
    DEBUG=1            Build for debugging
    TESTS=1            Build unit tests
    FORCE=1            Force clean build
    SAN=type           Build with sanitizer (address|memory|leak|thread)
    COV=1              Build with coverage instrumentation
    RUST_PROFILE=name  Rust profile to use (default: release)
    VERBOSE=1          Verbose build output

  make clean         Remove build artifacts
    ALL=1              Remove entire artifacts directory

Testing:
  make test          Run all tests
  make unit-tests    Run unit tests (C and C++)
  make rust-tests    Run Rust tests
  make pytest        Run Python tests
    COORD=oss|rlec        Test coordinator type (default: oss)
    REDIS_STANDALONE=1|0  Test with standalone/cluster Redis
    SA=1|0                Alias for REDIS_STANDALONE
    TEST=name             Run specified test
    QUICK=1               Run quick test subset

Development:
  make run           Run Redis with RediSearch
    COORD=oss|rlec     Run with coordinator type (default: oss)
    GDB=1              Invoke using gdb
  make lint          Run linters
  make fmt           Format source files
    CHECK=1            Check formatting without modifying files

Packaging:
  make pack          Create installation packages
  make docker        Build for specified platform
endef

help:
	@echo "$$HELPTEXT"

setup:
	@echo "Setting up system..."
	@$(ROOT)/sbin/setup

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

c-tests: unit-tests
cpp-tests: unit-tests

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
	@echo "Starting Redis with RediSearch..."
	@if [ "$(COORD)" = "rlec" ]; then \
		MODULE_PATH=$$(find $(ROOT)/bin -name "module-enterprise.so" | head -1); \
	else \
		MODULE_PATH=$$(find $(ROOT)/bin -name "redisearch.so" | head -1); \
	fi; \
	if [ -z "$$MODULE_PATH" ]; then \
		echo "Error: No $(COORD) module found. Please build first with 'make build COORD=$(COORD)'"; \
		exit 1; \
	fi; \
	if [ "$(GDB)" = "1" ]; then \
		echo "Starting with GDB..."; \
		gdb -ex r --args redis-server --loadmodule $$MODULE_PATH; \
	else \
		redis-server --loadmodule $$MODULE_PATH; \
	fi

lint:
	@echo "Running linters..."
	@cd $(ROOT)/src/redisearch_rs && cargo clippy -- -D warnings
	@cd $(ROOT)/src/redisearch_rs && RUSTDOCFLAGS="-Dwarnings" cargo doc

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

pack: $(BUILD_SCRIPT)
	@echo "Creating installation packages..."
	@if [ -z "$(MODULE_PATH)" ]; then \
		MODULE_PATH=$$(find $(ROOT)/bin -name "redisearch.so" -o -name "module-enterprise.so" | head -1); \
		if [ -z "$$MODULE_PATH" ]; then \
			echo "Error: No module found. Please build first with 'make build'"; \
			exit 1; \
		fi; \
	else \
		MODULE_PATH="$(MODULE_PATH)"; \
	fi; \
	if command -v python3 >/dev/null 2>&1 && python3 -c "import RAMP.ramp" >/dev/null 2>&1; then \
		echo "RAMP is available, creating RAMP packages..."; \
		RAMP=1 COORD=$(COORD) PACKAGE_NAME=$(PACKAGE_NAME) MODULE_NAME=$(MODULE_NAME) \
		RAMP_YAML=$(RAMP_YAML) RAMP_ARGS=$(RAMP_ARGS) \
		$(ROOT)/sbin/pack.sh "$$MODULE_PATH"; \
	else \
		echo "RAMP not available, skipping RAMP package creation..."; \
		echo "To install RAMP: pip install redismodules-ramp"; \
		RAMP=0 COORD=$(COORD) PACKAGE_NAME=$(PACKAGE_NAME) MODULE_NAME=$(MODULE_NAME) \
		$(ROOT)/sbin/pack.sh "$$MODULE_PATH"; \
	fi

upload-artifacts:
	@echo "Uploading artifacts..."
	@$(ROOT)/sbin/upload-artifacts

docker:
	@echo "Building Docker image..."
	@$(MAKE) -C build/docker

benchmark:
	@echo "Running benchmarks..."
	@cd tests/benchmarks && redisbench-admin run-local

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

sanbox:
	@echo "Starting development container..."
	@docker run -it -v $(PWD):/search -w /search --cap-add=SYS_PTRACE \
		--security-opt seccomp=unconfined redisfab/clang:16-x64-bullseye bash

.PHONY: help setup fetch build clean test unit-tests rust-tests pytest
.PHONY: c-tests cpp-tests run lint fmt license-check pack upload-artifacts
.PHONY: docker benchmark vecsim-bench callgrind sanbox parsers verify-deps
