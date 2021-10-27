
ifneq ($(filter coverage,$(MAKECMDGOALS)),)
COV=1
endif

ifneq ($(VG),)
VALGRIND=$(VG)
endif

ifeq ($(VALGRIND),1)
override DEBUG ?= 1
endif

ifneq ($(SAN),)
override DEBUG ?= 1
ifeq ($(SAN),mem)
CMAKE_SAN=-DUSE_MSAN=ON -DMSAN_PREFIX=/opt/llvm-project/build-msan
SAN_DIR=msan
export SAN=memory

else ifeq ($(SAN),memory)
CMAKE_SAN=-DUSE_MSAN=ON -DMSAN_PREFIX=/opt/llvm-project/build-msan
SAN_DIR=msan
export SAN=memory

else ifeq ($(SAN),addr)
CMAKE_SAN=-DUSE_ASAN=ON
SAN_DIR=asan
export SAN=address

else ifeq ($(SAN),address)
CMAKE_SAN=-DUSE_ASAN=ON
SAN_DIR=asan
export SAN=address

else ifeq ($(SAN),leak)
else ifeq ($(SAN),thread)
else
$(error SAN=mem|addr|leak|thread)
endif
endif

#----------------------------------------------------------------------------------------------

ROOT=.
include deps/readies/mk/main

#----------------------------------------------------------------------------------------------

define HELP
make setup         # install prerequisited (CAUTION: THIS WILL MODIFY YOUR SYSTEM)
make fetch         # download and prepare dependant modules

make build         # compile and link
  COORD=oss|rlec     # build coordinator
  STATIC=1           # build as static lib
  LITE=1             # build RediSearchLight
  DEBUG=1            # build for debugging (implies WITH_TESTS=1)
  WITH_TESTS=1       # enable unit tests
  WHY=1              # explain CMake decisions (in /tmp/cmake-why)
  FORCE=1            # Force CMake rerun
  CMAKE_ARGS=...     # extra arguments to CMake
  VG=1               # build for Valgrind
  SAN=type           # build with LLVM sanitizer (type=address|memory|leak|thread) 
  SLOW=1             # do not parallelize build (for diagnostics)
make parsers       # build parsers code
make clean         # remove build artifacts
  ALL=1              # remove entire artifacts directory

make run           # run redis with RediSearch
  DEBUG=1            # invoke using gdb

make test          # run all tests (via ctest)
  COORD=oss|rlec     # test coordinator
  TEST=regex         # run tests that match regex
  TESTDEBUG=1        # be very verbose (CTest-related)
  CTEST_ARG=...      # pass args to CTest
make pytest        # run python tests (tests/pytests)
  COORD=oss|rlec     # test coordinator
  TEST=name          # e.g. TEST=test:testSearch
  RLTEST_ARGS=...    # pass args to RLTest
  REJSON=1|0         # also load RedisJSON module
  REJSON_PATH=path   # use RedisJSON module at `path`
  EXT=1              # External (existing) environment
  GDB=1              # RLTest interactive debugging
  VG=1               # use Valgrind
  SAN=type           # use LLVM sanitizer (type=address|memory|leak|thread) 
  ONLY_STABLE=1      # skip unstable tests
make c_tests       # run C tests (from tests/ctests)
make cpp_tests     # run C++ tests (from tests/cpptests)
  TEST=name          # e.g. TEST=FGCTest.testRemoveLastBlock

make callgrind     # produce a call graph
  REDIS_ARGS="args"

make pack          # create installation packages
make deploy        # copy packages to S3
make release       # release a version

make docs          # create documentation
make deploydocs    # deploy documentation

make docker
make docker_push
endef

#----------------------------------------------------------------------------------------------

ifeq ($(STATIC),1)

ifneq ($(COORD),)
$(error STATIC=1 is incompatible with COORD)
endif

CMAKE_DIR=$(ROOT)
BINDIR=$(BINROOT)/search-static
SRCDIR=src
TARGET=$(BINDIR)/redisearch.a
PACKAGE_NAME=

endif # STATIC

ifeq ($(COORD),)

CMAKE_DIR=$(ROOT)
SRCDIR=src
TARGET=$(BINDIR)/redisearch.so
PACKAGE_NAME=redisearch
RAMP_YAML=pack/ramp.yml

ifneq ($(LITE),1)
BINDIR=$(BINROOT)/search
RAMP_MODULE_NAME=search
else
BINDIR=$(BINROOT)/search-lite
RAMP_MODULE_NAME=searchlight
PACKAGE_NAME=redisearch-light
RAMP_YAML=pack/ramp-light.yml
endif

else # COORD

ifeq ($(COORD),oss)
CMAKE_DIR=$(ROOT)/coord
BINDIR=$(BINROOT)/coord-oss
SRCDIR=coord/src
TARGET=$(BINDIR)/module-oss.so
PACKAGE_NAME=redisearch-oss
RAMP_MODULE_NAME=search

else ifeq ($(COORD),rlec)
CMAKE_DIR=$(ROOT)/coord
BINDIR=$(BINROOT)/coord-rlec
SRCDIR=coord/src
TARGET=$(BINDIR)/module-enterprise.so
PACKAGE_NAME=redisearch-enterprise
RAMP_MODULE_NAME=search
RAMP_YAML=coord/pack/ramp.yml

else
$(error COORD should be either oss or rlec)
endif

export LIBUV_BINDIR=$(realpath bin/$(FULL_VARIANT.debug)/libuv)
include build/libuv/Makefile.defs

HIREDIS_BINDIR=bin/$(FULL_VARIANT.release)/hiredis
include build/hiredis/Makefile.defs

endif # COORD

#----------------------------------------------------------------------------------------------

export PACKAGE_NAME

#----------------------------------------------------------------------------------------------

ifeq ($(COV),1)
CMAKE_COV += -DUSE_COVERAGE=ON
endif

ifneq ($(SAN),)
CMAKE_SAN += -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
endif

ifeq ($(PROFILE),1)
CMAKE_PROFILE=-DPROFILE=ON
endif

ifeq ($(DEBUG),1)
CMAKE_BUILD_TYPE=DEBUG
WITH_TESTS ?= 1
else
CMAKE_BUILD_TYPE=RelWithDebInfo
endif
CMAKE_DEBUG=-DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE)

ifeq ($(WITH_TESTS),1)
CMAKE_TEST=-DRS_RUN_TESTS=ON
# -DRS_VERBOSE_TESTS=ON
endif

ifeq ($(WHY),1)
CMAKE_WHY=--trace-expand > /tmp/cmake-why 2>&1
endif

ifeq ($(STATIC),1)
CMAKE_STATIC += -DRS_BUILD_STATIC=ON
endif

ifneq ($(COORD),)
CMAKE_COORD += -DRS_COORD_TYPE=$(COORD)
endif

CMAKE_FILES= \
	CMakeLists.txt \
	build/cmake/redisearch_cflags.cmake \
	build/cmake/redisearch_debug.cmake \
	deps/friso/CMakeLists.txt \
	deps/phonetics/CMakeLists.txt \
	deps/snowball/CMakeLists.txt \
	deps/rmutil/CMakeLists.txt

ifeq ($(WITH_TESTS),1)
CMAKE_FILES+= \
	deps/googletest/CMakeLists.txt \
	deps/googletest/googlemock/CMakeLists.txt \
	deps/googletest/googletest/CMakeLists.txt \
	tests/ctests/CMakeLists.txt \
	tests/cpptests/CMakeLists.txt \
	tests/cpptests/redismock/CMakeLists.txt \
	tests/pytests/CMakeLists.txt \
	tests/c_utils/CMakeLists.txt
endif

CMAKE_FLAGS=\
	-Wno-dev \
	-DGIT_SHA=$(GIT_SHA) \
	-DGIT_VERSPEC=$(GIT_VERSPEC) \
	-DRS_MODULE_NAME=$(RAMP_MODULE_NAME)

CMAKE_FLAGS += $(CMAKE_ARGS) $(CMAKE_DEBUG) $(CMAKE_STATIC)  $(CMAKE_COORD) \
	$(CMAKE_COV) $(CMAKE_SAN) $(CMAKE_TEST) $(CMAKE_WHY) $(CMAKE_PROFILE)

#----------------------------------------------------------------------------------------------

include $(MK)/defs

MK_CUSTOM_CLEAN=1

#----------------------------------------------------------------------------------------------

MISSING_DEPS:=
ifeq ($(wildcard $(LIBUV)),)
MISSING_DEPS += $(LIBUV) $(HIREDIS)
endif
ifeq ($(wildcard $(HIREDIS)),)
MISSING_DEPS += $(HIREDIS)
endif

ifneq ($(MISSING_DEPS),)
DEPS=1
endif

DEPENDENCIES=libuv hiredis

ifneq ($(filter all deps $(DEPENDENCIES) pack,$(MAKECMDGOALS)),)
DEPS=1
endif

.PHONY: deps $(DEPENDENCIES)

#----------------------------------------------------------------------------------------------

all: bindirs $(TARGET)

include $(MK)/rules

FORCE?=1

ifeq ($(SLOW),1)
MAKE_J=
else
MAKE_J:=-j$(shell nproc)
endif

ifeq ($(FORCE),1)
.PHONY: __force

$(BINDIR)/Makefile: __force
else
$(BINDIR)/Makefile : $(CMAKE_FILES)
endif
ifeq ($(WHY),1)
	@echo CMake log is in /tmp/cmake-why
endif
	@mkdir -p $(BINROOT)
	@cd $(BINDIR) && cmake $(CMAKE_DIR) $(CMAKE_FLAGS)

$(TARGET): $(MISSING_DEPS) $(BINDIR)/Makefile
	@echo Building $(TARGET) ...
	@$(MAKE) -C $(BINDIR) $(MAKE_J)
#	@[ -f $(TARGET) ] && touch $(TARGET)

.PHONY: build clean run 

clean:
ifeq ($(ALL),1)
	rm -rf $(BINROOT)
else
	$(MAKE) -C $(BINDIR) clean
endif

#----------------------------------------------------------------------------------------------

parsers:
ifeq ($(FORCE),1)
	cd src/aggregate/expr ;\
	rm -f lexer.c parser-toplevel.c parser.c.inc
	cd src/query_parser ;\
	rm -f lexer.c parser-toplevel.c parser.c.inc
endif
	$(MAKE) -C src/aggregate/expr
	$(MAKE) -C src/query_parser

.PHONY: parsers

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

deps: $(LIBUV) $(HIREDIS)

libuv: $(LIBUV)

$(LIBUV):
	@echo Building libuv...
	$(SHOW)$(MAKE) --no-print-directory -C build/libuv DEBUG=1

hiredis: $(HIREDIS)

$(HIREDIS):
	@echo Building hiredis...
	$(SHOW)$(MAKE) --no-print-directory -C build/hiredis DEBUG=

#----------------------------------------------------------------------------------------------

else

deps: ;

endif # DEPS

#----------------------------------------------------------------------------------------------

setup:
	@echo Setting up system...
	$(SHOW)./deps/readies/bin/getpy2
	$(SHOW)./system-setup.py 

#----------------------------------------------------------------------------------------------

fetch:
	-git submodule update --init --recursive

#----------------------------------------------------------------------------------------------

run:
	@redis-server --loadmodule $(abspath $(TARGET))

#----------------------------------------------------------------------------------------------

export REJSON ?= 1

ifeq ($(TESTDEBUG),1)
override CTEST_ARGS += --debug
endif

CTEST_ARGS += --output-on-failure


ifneq ($(SAN),)
export ASAN_OPTIONS=detect_odr_violation=0
endif

ifeq ($(COV),1)
# CTEST_ARGS += --output-on-failure
endif

ifeq ($(VG),1)
# CTEST_ARGS += --output-on-failure
endif

ifneq ($(SLOW),1)
ifeq ($(SAN),)
CTEST_PARALLEL=8
else ifeq ($(COV),1)
CTEST_PARALLEL:=$(shell $(ROOT)/deps/readies/bin/nproc)
else
CTEST_PARALLEL=
endif
endif # SLOW

ifneq ($(CTEST_PARALLEL),)
override CTEST_ARGS += -j$(CTEST_PARALLEL)
endif

override CTEST_ARGS += --timeout 15000

FLOW_TESTS_ARGS=\
	VG=$(VALGRIND) VG_LEAKS=0

ifeq ($(EXT),1)
FLOW_TESTS_ARGS += EXISTING_EXT=1
endif

export EXT_TEST_PATH:=$(BINDIR)/tests/ctests/example_extension/libexample_extension.so

test:
ifneq ($(TEST),)
	$(SHOW)set -e; cd $(BINRDIR); CTEST_OUTPUT_ON_FAILURE=1 RLTEST_ARGS="-s -v" ctest $(CTEST_ARGS) -vv -R $(TEST)
else
	$(SHOW)set -e; cd $(BINDIR); ctest $(CTEST_ARGS)
ifeq ($(COORD),oss)
	$(SHOW)$(FLOW_TESTS_ARGS) FORCE='' $(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))
endif
endif

pytest:
	$(SHOW)TEST=$(TEST) $(FLOW_TESTS_ARGS) FORCE='' $(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))

ifeq ($(GDB),1)
GDB_CMD=gdb -ex r --args
else
GDB_CMD=
endif

c_tests:
	$(SHOW)find $(abspath $(BINROOT)/tests/ctests) -name "test_*" -type f -executable -exec ${GDB_CMD} {} \;

cpp_tests:
ifeq ($(TEST),)
	$(SHOW)find $(abspath $(BINROOT)/tests/cpptests) -name "test_*" -type f -executable -exec ${GDB_CMD} {} \;
else
	$(SHOW)set -e ;\
	$(GDB_CMD) $(abspath $(BINROOT)/tests/cpptests/$(TEST)) --gtest_filter=$(TEST)
endif

.PHONY: test pytest c_tests cpp_tests

#----------------------------------------------------------------------------------------------

REDIS_ARGS +=

CALLGRIND_ARGS=\
	--tool=callgrind \
	--dump-instr=yes \
	--simulate-cache=no \
	--collect-jumps=yes \
	--collect-atstart=yes \
	--collect-systime=yes \
	--instr-atstart=yes \
	-v redis-server \
	--protected-mode no \
	--save "" \
	--appendonly no

callgrind: $(TARGET)
	$(SHOW)valgrind $(CALLGRIND_ARGS) --loadmodule $(abspath $(TARGET)) $(REDIS_ARGS)

#----------------------------------------------------------------------------------------------

RAMP_VARIANT=$(subst release,,$(FLAVOR))$(_VARIANT.string)

RAMP.release:=$(shell JUST_PRINT=1 RAMP=1 DEPS=0 RELEASE=1 SNAPSHOT=0 VARIANT=$(RAMP_VARIANT) PACKAGE_NAME=$(PACKAGE_NAME) $(ROOT)/sbin/pack.sh)
# RAMP.snapshot:=$(shell JUST_PRINT=1 RAMP=1 DEPS=0 RELEASE=0 SNAPSHOT=1 VARIANT=$(RAMP_VARIANT) PACKAGE_NAME=$(PACKAGE_NAME) $(ROOT)/sbin/pack.sh)

PACK_ARGS=\
	VARIANT=$(RAMP_VARIANT) \
	PACKAGE_NAME=$(PACKAGE_NAME) \
	MODULE_NAME=$(RAMP_MODULE_NAME) \
	RAMP_YAML=$(RAMP_YAML) \
	RAMP_ARGS=$(RAMP_ARGS)

# $(TARGET) 
bin/artifacts/$(RAMP.release) : $(RAMP_YAML)
	@echo Packing module...
	$(SHOW)$(PACK_ARGS) $(ROOT)/sbin/pack.sh $(TARGET)

pack: bin/artifacts/$(RAMP.release)

#----------------------------------------------------------------------------------------------

ifeq ($(REMOTE),1)
BENCHMARK_ARGS=run-remote 
else
BENCHMARK_ARGS=run-local
endif

BENCHMARK_ARGS += --module_path $(realpath $(TARGET)) --required-module search

ifeq ($(REJSON),1)
BENCHMARK_ARGS += --module_path $(realpath $(REJSON_PATH)) --required-module ReJSON
endif

ifneq ($(BENCHMARK),)
BENCHMARK_ARGS += --test $(BENCHMARK)
endif

benchmark:
	$(SHOW)cd tests/benchmarks ;\
	redisbench-admin $(BENCHMARK_ARGS)

#----------------------------------------------------------------------------------------------

COV_EXCLUDE += \
    'deps/*' \
	'tests/*'

coverage:
	$(SHOW)$(MAKE) COV=1
	$(SHOW)$(MAKE) COORD=oss COV=1
	$(SHOW)$(COVERAGE_RESET)
	$(SHOW)$(MAKE) test COV=1
	$(SHOW)$(MAKE) test COORD=oss COV=1
	$(SHOW)$(COVERAGE_COLLECT_REPORT)

upload-cov:
	$(SHOW)bash <(curl -s https://raw.githubusercontent.com/codecov/codecov-bash/master/codecov) -f bin/linux-x64-debug-cov/cov.info

#----------------------------------------------------------------------------------------------

docs:
	$(SHOW)mkdocs build

deploydocs:
	$(SHOW)mkdocs gh-deploy

.PHONY: docs deploydocs

#----------------------------------------------------------------------------------------------

DOCKER_ARGS=

ifeq ($(CACHE),0)
DOCKER_ARGS += --no-cache
endif

DOCKER_IMAGE ?= redislabs/redisearch

docker:
	docker build . -t $(DOCKER_IMAGE) -f docker/Dockerfile $(DOCKER_ARGS)

.PHONY: docker
