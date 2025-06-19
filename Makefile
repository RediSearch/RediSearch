
#-----------------------------------------------------------------------------
# RediSearch Makefile
#
# This Makefile acts as a thin wrapper around the build.sh script, providing
# backward compatibility for existing make targets while using build.sh for
# all actual build operations.
#-----------------------------------------------------------------------------

.NOTPARALLEL:

ROOT := $(shell pwd)
BUILD_SCRIPT := $(ROOT)/build.sh

# Default target
.DEFAULT_GOAL := build

# Ensure build.sh is executable
$(BUILD_SCRIPT):
	@chmod +x $(BUILD_SCRIPT)

#-----------------------------------------------------------------------------
# Help text
#-----------------------------------------------------------------------------

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

ifneq ($(REDIS_STANDALONE),)
	BUILD_ARGS += REDIS_STANDALONE=$(REDIS_STANDALONE)
endif

ifneq ($(SA),)
	BUILD_ARGS += SA=$(SA)
endif

# Package variables (used by pack target)
PACKAGE_NAME ?= redisearch-oss
MODULE_NAME ?= search
RAMP_YAML ?=
RAMP_ARGS ?=

#-----------------------------------------------------------------------------
# Main targets
#-----------------------------------------------------------------------------

# Help target
help:
	@echo "make setup         # install prerequisites (CAUTION: THIS WILL MODIFY YOUR SYSTEM)"
	@echo "make fetch         # download and prepare dependent modules"
	@echo ""
	@echo "make build          # compile and link"
	@echo "  COORD=oss|rlec      # build coordinator (oss: Open Source, rlec: Enterprise) default: oss"
	@echo "  DEBUG=1             # build for debugging"
	@echo "  TESTS=1             # build unit tests"
	@echo "  FORCE=1             # Force clean build"
	@echo "  SAN=type            # build with LLVM sanitizer (type=address|memory|leak|thread)"
	@echo "  COV=1               # build with coverage instrumentation"
	@echo "  RUST_PROFILE=name   # Which Rust profile should be used to build (default: release)"
	@echo "  VERBOSE=1           # verbose build output"
	@echo ""
	@echo "make clean         # remove build artifacts"
	@echo "  ALL=1              # remove entire artifacts directory"
	@echo ""
	@echo "make test          # run all tests"
	@echo "make unit-tests    # run unit tests (C and C++)"
	@echo "make rust-tests    # run Rust tests"
	@echo "make pytest        # run python tests (tests/pytests)"
	@echo "  REDIS_STANDALONE=1|0 # test with standalone/cluster Redis"
	@echo "  SA=1|0               # alias for REDIS_STANDALONE"
	@echo "  TEST=name            # run specified test"
	@echo "  QUICK=1              # run quick test subset"
	@echo ""
	@echo "make run           # run redis with RediSearch"
	@echo "  GDB=1              # invoke using gdb"
	@echo ""
	@echo "make lint          # run linters and exit with an error if warnings are found"
	@echo "make fmt           # format the source files using the appropriate auto-formatter"
	@echo "  CHECK=1            # don't modify the source files, but exit with an error if"
	@echo "                     # running the auto-formatter would result in changes"
	@echo ""
	@echo "make pack          # create installation packages"
	@echo "make docker        # build for specified platform"

# Setup target
setup:
	@echo "Setting up system..."
	@$(ROOT)/sbin/setup

# Fetch dependencies
fetch:
	@echo "Fetching dependencies..."
	@git submodule update --init --recursive

# Build target
build: $(BUILD_SCRIPT) verify-deps
	@echo "Building RediSearch..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS)

# Verify build dependencies
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

# Clean target
clean:
ifeq ($(ALL),1)
	@echo "Cleaning all build artifacts..."
	@rm -rf $(ROOT)/bin
else
	@echo "Cleaning build artifacts..."
	@rm -rf $(ROOT)/bin/*/search-*
endif

# Test targets
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

# Alias targets for backward compatibility
c-tests: unit-tests
cpp-tests: unit-tests

# Parser targets
parsers:
ifeq ($(FORCE),1)
	@cd src/aggregate/expr && rm -f lexer.c parser.c
	@$(MAKE) -C src/query_parser/v1 clean
	@$(MAKE) -C src/query_parser/v2 clean
endif
	@$(MAKE) -C src/aggregate/expr
	@$(MAKE) -C src/query_parser/v1
	@$(MAKE) -C src/query_parser/v2

# Run target
run:
	@echo "Starting Redis with RediSearch..."
	@if [ "$(GDB)" = "1" ]; then \
		echo "Starting with GDB..."; \
		gdb -ex r --args redis-server --loadmodule $$(find $(ROOT)/bin -name "redisearch.so" -o -name "module-enterprise.so" | head -1); \
	else \
		redis-server --loadmodule $$(find $(ROOT)/bin -name "redisearch.so" -o -name "module-enterprise.so" | head -1); \
	fi

# Linting and formatting
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

# License check
license-check:
	@echo "Checking license headers..."
	@cd $(ROOT)/src/redisearch_rs && cargo license-check

# Packaging
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

# Upload artifacts
upload-artifacts:
	@echo "Uploading artifacts..."
	@$(ROOT)/sbin/upload-artifacts

# Docker targets
docker:
	@echo "Building Docker image..."
	@$(MAKE) -C build/docker

# Benchmarking
benchmark:
	@echo "Running benchmarks..."
	@cd tests/benchmarks && redisbench-admin run-local

# VecSim benchmarks
vecsim-bench: $(BUILD_SCRIPT)
	@echo "Running VecSim micro-benchmarks..."
	@$(BUILD_SCRIPT) $(BUILD_ARGS) RUN_MICRO_BENCHMARKS

# Profiling
callgrind:
	@echo "Running callgrind profiling..."
	@valgrind --tool=callgrind --dump-instr=yes --simulate-cache=no \
		--collect-jumps=yes --collect-atstart=yes --collect-systime=yes \
		--instr-atstart=yes -v redis-server --protected-mode no \
		--save "" --appendonly no \
		--loadmodule $$(find $(ROOT)/bin -name "redisearch.so" -o -name "module-enterprise.so" | head -1)

# Development container
sanbox:
	@echo "Starting development container..."
	@docker run -it -v $(PWD):/search -w /search --cap-add=SYS_PTRACE \
		--security-opt seccomp=unconfined redisfab/clang:16-x64-bullseye bash

#-----------------------------------------------------------------------------
# Phony targets
#-----------------------------------------------------------------------------

.PHONY: help setup fetch build clean test unit-tests rust-tests pytest
.PHONY: c-tests cpp-tests run lint fmt license-check pack upload-artifacts
.PHONY: docker benchmark vecsim-bench callgrind sanbox parsers verify-deps
