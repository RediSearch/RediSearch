ifeq (n,$(findstring n,$(firstword -$(MAKEFLAGS))))
DRY_RUN:=1
else
DRY_RUN:=
endif

ifneq ($(filter coverage show-cov,$(MAKECMDGOALS)),)
COV=1
endif

ifeq ($(COV),1)
override DEBUG = 1
CMAKE_COV += -DUSE_COVERAGE=ON
endif

ifeq ($(NO_TESTS),1)
CMAKE_TESTS += -DVECSIM_BUILD_TESTS=off
endif

ifneq ($(SAN),)

ifeq ($(SAN),address)
override SAN=address
override DEBUG=1
export ASAN_OPTIONS=detect_odr_violation=0:allocator_may_return_null=1
CMAKE_SAN=-DUSE_ASAN=ON -DSVS_SHARED_LIB=OFF
else ifeq ($(SAN),alignment)
override DEBUG=1
CMAKE_SAN=-DUSE_ALIGNMENT_SAN=ON -DSVS_SHARED_LIB=OFF
else
$(error Unsupported sanitizer: $(SAN))
endif

export SAN
endif # SAN != ''

ROOT=.
export ROOT
#----------------------------------------------------------------------------------------------

define HELPTEXT
make build
  DEBUG=1          # build debug variant
  COV=1			   # build for code coverage
  VERBOSE=1        # print detailed build info
  SAN=address          # build with AddressSanitizer (clang)
  SLOW=1               # don't run build in parallel (for diagnostics)
  PROFILE=1            # enable profiling compile flags (and debug symbols) for release type.
make pybind        # build Python bindings
make clean         # remove binary files
  ALL=1            # remove binary directories

make unit_test     # run unit tests
  CTEST_ARGS=args    # extra CTest arguments
  SAN=address        # run tests with AddressSanitizer
  FP_64=1			# run tests with 64-bit floating point
make asan            # build with AddressSanitizer and run unit tests
make benchmark	   # run benchmarks

make format          # fix formatting of sources
make check-format    # check formatting of sources


endef

#----------------------------------------------------------------------------------------------

SEARCH_ROOT:=$(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
ROOT:=$(SEARCH_ROOT)/src/vecsim
ifeq ($(DEBUG),1)
FLAVOR=debug
else
FLAVOR=release
endif

ifneq ($(SAN),)
FLAVOR := ${FLAVOR}-$(SAN)
endif

ifeq ($(COV),1)
FLAVOR := $(FLAVOR)-coverage
endif

FULL_VARIANT:=$(shell uname)-$(shell uname -m)-$(FLAVOR)
BINROOT=$(SEARCH_ROOT)/bin/vecsim/$(FULL_VARIANT)
BINDIR=$(BINROOT)
TESTDIR=$(BINDIR)/unit_tests
BENCHMARKDIR=$(BINDIR)/benchmark
SRCDIR=$(ROOT)/src

ifeq ($(SLOW),1)
MAKE_J=
else
MAKE_J:=-j$(shell getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)
endif

CMAKE_DIR=$(ROOT)

ifeq ($(DEBUG),1)
CMAKE_BUILD_TYPE=DEBUG
else
CMAKE_BUILD_TYPE=RelWithDebInfo
endif
CMAKE_FLAGS += -DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE)

ifeq ($(PROFILE),1)
CMAKE_FLAGS += -DUSE_PROFILE=on
endif

ifeq ($(VERBOSE),1)
CMAKE_FLAGS += -DCMAKE_VERBOSE_MAKEFILE=on
endif

# CMake flags for fp64 unit tests
ifeq ($(FP_64),1)
CMAKE_FLAGS += -DFP64_TESTS=on
endif

CMAKE_FLAGS += \
	-Wno-deprecated \
	-DCMAKE_WARN_DEPRECATED=OFF \
	-Wno-dev \
	--no-warn-unused-cli \
	$(CMAKE_SAN) \
	$(CMAKE_COV) \
	$(CMAKE_TESTS) \
	$(VECSIM_CMAKE_ARGS)

build:
	$(SHOW)mkdir -p $(BINDIR)
	$(SHOW)cd $(BINDIR) && cmake $(CMAKE_FLAGS) $(CMAKE_DIR)
	@make --no-print-directory -C $(BINDIR) $(MAKE_J)

.PHONY: build

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINROOT) $(ROOT)/build $(ROOT)/dist $(ROOT)/.tox
else
	$(SHOW)$(MAKE) --no-print-directory -C $(BINDIR) clean
endif

.PHONY: clean

#----------------------------------------------------------------------------------------------

pybind:
	$(SHOW)cd $(ROOT) && poetry build
.PHONY: pybind

#----------------------------------------------------------------------------------------------

_CTEST_ARGS=$(CTEST_ARGS)
_CTEST_ARGS += \
	--output-on-failure --no-tests=error \
	$(MAKE_J)

ifeq ($(VERBOSE),1)
_CTEST_ARGS += -V
endif

# AddressSanitizer is handled via SAN=address in cmake/san.cmake

unit_test:
	$(SHOW)mkdir -p $(BINDIR)
	$(SHOW)cd $(BINDIR) && cmake $(CMAKE_FLAGS) -DVECSIM_BUILD_TESTS=ON -DVECSIM_BUILD_BENCHMARKS=OFF $(CMAKE_DIR)
	@make --no-print-directory -C $(BINDIR) $(MAKE_J)
	$(SHOW)cd $(TESTDIR) && GTEST_COLOR=1 ctest $(_CTEST_ARGS)

asan:
	$(SHOW)$(MAKE) -f $(SEARCH_ROOT)/sbin/vecsim.mk SAN=address unit_test

.PHONY: unit_test asan

#----------------------------------------------------------------------------------------------
BM_FILTER ?= benchmarks-default

benchmark:
	$(SHOW)mkdir -p $(BINDIR)
	$(SHOW)cd $(BINDIR) && cmake $(CMAKE_FLAGS) -DVECSIM_BUILD_TESTS=OFF -DVECSIM_BUILD_BENCHMARKS=ON $(CMAKE_DIR)
	@names=$$(bash $(SEARCH_ROOT)/tests/benchmarks/vecsim/benchmarks.sh "$(BM_FILTER)") && \
		cmake --build $(BINDIR) $(MAKE_J) --target $$(printf "%s\n" "$$names" | sed "s/^/bm_/")
	$(SEARCH_ROOT)/tests/benchmarks/vecsim/benchmarks.sh $(BM_FILTER) | xargs -I {} bash -lc \
		"cd $(SEARCH_ROOT) && ROOT=$(SEARCH_ROOT) $(BENCHMARKDIR)/bm_{} --benchmark_out_format=json --benchmark_out=$(BENCHMARKDIR)/{}_results.json $(BENCHMARK_ARGS) || exit 255"

check-format:
	$(SHOW)$(SEARCH_ROOT)/sbin/format-vecsim

format:
	$(SHOW)FIX=1 $(SEARCH_ROOT)/sbin/format-vecsim

.PHONY: check-format format

COV_INFO=$(BINROOT)/cov.info
COV_DIR=$(BINROOT)/cov
COV_PROFDATA=$(COV_DIR)/cov.profdata

define COVERAGE_RESET
$(SHOW)set -e ;\
echo "Starting coverage analysis." ;\
mkdir -p $(COV_DIR) ;\
lcov --directory $(BINROOT) --base-directory $(SRCDIR) -z
endef


define COVERAGE_COLLECT
$(SHOW)set -e ;\
echo "Collecting coverage data ..." ;\
lcov --rc branch_coverage=1 --capture --directory $(BINROOT) --base-directory $(SRCDIR) --output-file $(COV_INFO);\
lcov --rc branch_coverage=1 -o $(COV_INFO).1 --extract $(COV_INFO) '$(ROOT)/src/VecSim/*';\
mv $(COV_INFO).1 $(COV_INFO)
endef

define COVERAGE_REPORT
$(SHOW)set -e ;\
lcov -l $(COV_INFO) ;\
genhtml --legend --ignore-errors source -o $(COV_DIR) $(COV_INFO) > /dev/null 2>&1 ;\
echo "Coverage info at $$(realpath $(COV_DIR))/index.html"
endef

define COVERAGE_COLLECT_REPORT
$(COVERAGE_COLLECT)
$(COVERAGE_REPORT)
endef

coverage:
	$(SHOW)$(MAKE) -f $(SEARCH_ROOT)/sbin/vecsim.mk build COV=1 FP_64=1 NO_TESTS=0 CMAKE_TESTS="-DVECSIM_BUILD_TESTS=ON -DVECSIM_BUILD_BENCHMARKS=OFF"
	$(SHOW)$(COVERAGE_RESET)
	$(SHOW)cd $(TESTDIR) && GTEST_COLOR=1 ctest $(_CTEST_ARGS)
	$(SHOW)$(COVERAGE_COLLECT_REPORT)
	$(SHOW)cp $(COV_INFO) $(SEARCH_ROOT)/bin/vecsim.info

show-cov:
	$(SHOW)lcov -l $(COV_INFO)


.PHONY: coverage show-cov
