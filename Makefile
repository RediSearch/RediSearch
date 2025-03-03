
.NOTPARALLEL:

ROOT=.

MK.cmake=1
SRCDIR=.

MACOS_PACKAGES=openssl

include deps/readies/mk/main

#----------------------------------------------------------------------------------------------

define HELPTEXT
make setup         # install prerequisited (CAUTION: THIS WILL MODIFY YOUR SYSTEM)
make fetch         # download and prepare dependent modules

make build          # compile and link
  COORD=oss|rlec      # build coordinator (oss: Open Source, rlec: Enterprise) default: oss
  STATIC=1            # build as static lib
  LITE=1              # build RediSearchLight
  DEBUG=1             # build for debugging
  TESTS=0             # do not build unit tests
  WHY=1               # explain CMake decisions (in /tmp/cmake-why)
  FORCE=1             # Force CMake rerun (default)
  CMAKE_ARGS=...      # extra arguments to CMake
  VG=1                # build for Valgrind
  SAN=type            # build with LLVM sanitizer (type=address|memory|leak|thread)
  SLOW=1              # do not parallelize build (for diagnostics)
  GCC=1               # build with GCC (default unless Sanitizer)
  CLANG=1             # build with CLang
  STATIC_LIBSTDCXX=0  # link libstdc++ dynamically (default: 1)
  BOOST_DIR= 		  # Custom boost headers location path (default value: .install/boost).
  					  # Can be left empty if boost is located in the standard system includes path.
  VERBOSE_UTESTS=1    # enable logging in cpp tests
  REDIS_VER=		  # Hint the redis version to run against so we choose the appropriate build params.

make parsers       # build parsers code
make clean         # remove build artifacts
  ALL=1|all          # remove entire artifacts directory (all: remove Conan artifacts)
make cc            # compile a single file
  FILE=file          # source of file to build
make run           # run redis with RediSearch
  COORD=1|oss        # run cluster
  WITH_RLTEST=1      # run with RLTest wrapper
  GDB=1              # invoke using gdb

make test          # run all tests
  REDIS_STANDALONE=1|0 # test with standalone/cluster Redis
  SA=1|0               # alias for REDIS_STANDALONE
  TEST=name            # run specified test

make pytest        # run python tests (tests/pytests)
  REDIS_STANDALONE=1|0 # test with standalone/cluster Redis
  SA=1|0               # alias for REDIS_STANDALONE
  TEST=name            # e.g. TEST=test:testSearch
  RLTEST_ARGS=...      # pass args to RLTest
  REJSON=1|0           # also load RedisJSON module (default: 1)
  REJSON_BRANCH=branch # use RedisJSON module from branch (default: 'master')
  REJSON_PATH=path     # use RedisJSON module at `path` (default: '' - build from source)
  REJSON_ARGS=''       # pass args to RedisJSON module
  EXT=1                # External (existing) environment
  GDB=1                # RLTest interactive debugging
  VG=1                 # use Valgrind
  VG_LEAKS=0           # do not search leaks with Valgrind
  SAN=type             # use LLVM sanitizer (type=address|memory|leak|thread)
  ONLY_STABLE=1        # skip unstable tests
  TEST_PARALLEL=n      # test parallalization
  LOG_LEVEL=<level>    # server log level (default: debug)
  ENABLE_ASSERT=1      # enable assertions

make unit-tests    # run unit tests (C and C++)
  TEST=name          # e.g. TEST=FGCTest.testRemoveLastBlock
make c-tests       # run C tests (from tests/ctests)
make cpp-tests     # run C++ tests (from tests/cpptests)
make vecsim-bench  # run VecSim micro-benchmark

make callgrind     # produce a call graph
  REDIS_ARGS="args"

make pack             # create installation packages (default: 'redisearch-oss' package)
  COORD=rlec|oss        # pack RLEC coordinator ('redisearch' package)
  LITE=1                # pack RediSearchLight ('redisearch-light' package)

make upload-artifacts   # copy snapshot packages to S3
  OSNICK=nick             # copy snapshots for specific OSNICK
make upload-release     # copy release packages to S3

common options for upload operations:
  STAGING=1             # copy to staging lab area (for validation)
  FORCE=1               # allow operation outside CI environment
  VERBOSE=1             # show more details
  NOP=1                 # do not copy, just print commands

make docker        # build for specified platform
  OSNICK=nick        # platform to build for (default: host platform)
  TEST=1             # run tests after build
  PACK=1             # create package
  ARTIFACTS=1        # copy artifacts to host
  VERIFY=1           # verify docker is intact

make box           # create container with volumen mapping into /search
  OSNICK=nick        # platform spec
make sanbox        # create container with CLang Sanitizer

endef

#----------------------------------------------------------------------------------------------

ifeq ($(COORD),1)
	override COORD:=oss
else ifeq ($(COORD),) # Default: OSS Coordinator build
	override COORD:=oss
endif

ifeq ($(COORD),oss) # OSS (community distribution) Coordinator
	BINDIR=$(BINROOT)/search-community
	SRCDIR=.
	TARGET=$(BINDIR)/redisearch.so
	PACKAGE_NAME=redisearch-community
	MODULE_NAME=search
	RAMP_YAML=pack/ramp-community.yml
	PACKAGE_S3_DIR=redisearch-oss

else ifeq ($(COORD),rlec) # RLEC Coordinator
	BINDIR=$(BINROOT)/search-enterprise
	SRCDIR=.
	TARGET=$(BINDIR)/module-enterprise.so
	PACKAGE_NAME=redisearch
	MODULE_NAME=search
	RAMP_YAML=pack/ramp-enterprise.yml
	PACKAGE_S3_DIR=redisearch

else
	___:=$(error COORD should be either oss or rlec)
endif

ifeq ($(LITE),1) # Search Lite - overwrite the above settings (todo: retire lite completely)
	BINDIR=$(BINROOT)/search-lite
	SRCDIR=.
	TARGET=$(BINDIR)/redisearch.so
	PACKAGE_NAME=redisearch-light
	MODULE_NAME=searchlight
	RAMP_YAML=pack/ramp-light.yml
	PACKAGE_S3_DIR=redisearch
endif

ifeq ($(STATIC),1) # Static build - overwrite the above settings
	BINDIR=$(BINROOT)/search-static
	SRCDIR=.
	TARGET=$(BINDIR)/redisearch.a
	PACKAGE_NAME=
	MODULE_NAME=
	RAMP_YAML=
	PACKAGE_S3_DIR=
endif

LIBUV_DIR=$(ROOT)/deps/libuv
export LIBUV_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/libuv
include build/libuv/Makefile.defs

HIREDIS_DIR=$(ROOT)/deps/hiredis
HIREDIS_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/hiredis
include build/hiredis/Makefile.defs

export COORD
export PACKAGE_NAME

#----------------------------------------------------------------------------------------------

CC_C_STD=gnu11
# CC_CXX_STD=c++20

# Todo: currently when we run sanitizer against latest stable redis version where libstd++ is NOT dynamically linked
# we must use static with sanitizer. When we run sanitizer against redis unstable where libstd++ is dynamically
# linked to redis, we have to use dynamic as well.
export CC_STATIC_LIBSTDCXX=0

# Equivalent to: if we run on sanitizer AND version isn't unstable (
ifneq ($(SAN),)
ifneq ($(REDIS_VER), 'unstable')
export CC_STATIC_LIBSTDCXX=1
endif
endif
#----------------------------------------------------------------------------------------------

ifeq ($(VERBOSE_UTESTS),1)
CC_FLAGS.common += -DVERBOSE_UTESTS
endif

ifeq ($(ENABLE_ASSERT),1)
CC_FLAGS.common += -DENABLE_ASSERT
endif

#----------------------------------------------------------------------------------------------

ifeq ($(TESTS),0)
CMAKE_TEST=-DBUILD_SEARCH_UNIT_TESTS=OFF
else
CMAKE_TEST=-DBUILD_SEARCH_UNIT_TESTS=ON
endif

ifeq ($(STATIC),1)
CMAKE_STATIC += -DBUILD_STATIC=ON
endif

ifeq ($(LITE),1)
CMAKE_LITE = -DBUILD_LITE=ON
endif

#----------------------------------------------------------------------------------------------
BOOST_DIR ?= $(ROOT)/.install/boost
_CMAKE_FLAGS += -DMODULE_NAME=$(MODULE_NAME) -DBOOST_DIR=$(BOOST_DIR) -DMAX_WORKER_THREADS=$(MAX_WORKER_THREADS) -DSAN=$(SAN)

ifeq ($(OS),macos)
_CMAKE_FLAGS += -DLIBSSL_DIR=$(openssl_prefix) -DAPPLE=ON
endif

CMAKE_COORD += -DCOORD_TYPE=$(COORD)
_CMAKE_FLAGS += $(CMAKE_ARGS) $(CMAKE_STATIC) $(CMAKE_COORD) $(CMAKE_TEST) $(CMAKE_LITE)


include $(MK)/defs

# If DEBUG is not set, set NDEBUG. This will remove all assertions from the code.
# We set it in `CC_FLAGS.debug` so that it is
# appended after the unsetting of this flag in cc.defs (once it is removed can
# be set regularly).
ifeq ($(DEBUG),)
CC_FLAGS.debug += -DNDEBUG
endif

MK_CUSTOM_CLEAN=1

#----------------------------------------------------------------------------------------------

MISSING_DEPS:=

ifeq ($(wildcard $(LIBUV)),)
MISSING_DEPS += $(LIBUV)
endif

ifeq ($(wildcard $(HIREDIS)),)
#@@ MISSING_DEPS += $(HIREDIS)
endif

ifneq ($(MISSING_DEPS),)
DEPS=1
endif

DEPENDENCIES=libuv #@@  s2geometry hiredis

ifneq ($(filter all deps $(DEPENDENCIES) pack,$(MAKECMDGOALS)),)
DEPS=1
endif

.PHONY: deps $(DEPENDENCIES)

#----------------------------------------------------------------------------------------------

include $(MK)/rules


#----------------------------------------------------------------------------------------------

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINROOT)
else
	$(SHOW)$(MAKE) -C $(BINDIR) clean
endif

#----------------------------------------------------------------------------------------------

parsers:
ifeq ($(FORCE),1)
	$(SHOW)cd src/aggregate/expr ;\
	rm -f lexer.c parser.c
	$(SHOW)$(MAKE) -C src/query_parser/v1 clean
	$(SHOW)$(MAKE) -C src/query_parser/v2 clean
endif
	$(SHOW)$(MAKE) -C src/aggregate/expr
	$(SHOW)$(MAKE) -C src/query_parser/v1
	$(SHOW)$(MAKE) -C src/query_parser/v2

.PHONY: parsers

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

# s2geometry: $(S2GEOMETRY)
#
# $(S2GEOMETRY):
# 	@echo Building s2geometry...
# 	$(SHOW)$(MAKE) --no-print-directory -C build/s2geometry DEBUG=''

libuv: $(LIBUV)

$(LIBUV):
	@echo Building libuv...
	$(SHOW)$(MAKE) --no-print-directory -C build/libuv DEBUG=''

hiredis: $(HIREDIS)

$(HIREDIS):
	@echo Building hiredis...
	$(SHOW)$(MAKE) --no-print-directory -C build/hiredis DEBUG=''

#----------------------------------------------------------------------------------------------

else

deps: ;

endif # DEPS

#----------------------------------------------------------------------------------------------

setup:
	@echo Setting up system...
	$(SHOW)./sbin/setup

fetch:
	-git submodule update --init --recursive

.PHONY: setup fetch

#----------------------------------------------------------------------------------------------

CMAKE_TARGET_BUILD_DIR=$(CMAKE_TARGET_DIR)CMakeFiles/$(CMAKE_TARGET).dir

cc:
	@$(READIES)/bin/sep1
	$(SHOW)$(MAKE) -C $(BINDIR) -f $(CMAKE_TARGET_BUILD_DIR)/build.make $(CMAKE_TARGET_BUILD_DIR)/$(FILE).o

.PHONY: cc

#----------------------------------------------------------------------------------------------

ifeq ($(REDIS_STANDALONE),0)
WITH_RLTEST=1
else ifeq ($(SA),0)
WITH_RLTEST=1
endif

# RedisJSON defaults:
REJSON ?= 1
REJSON_BRANCH ?= master
REJSON_PATH ?=
REJSON_ARGS ?=

run:
ifeq ($(WITH_RLTEST),1)
	$(SHOW)REJSON=$(REJSON) REJSON_PATH=$(REJSON_PATH) REJSON_BRANCH=$(REJSON_BRANCH) REJSON_ARGS=$(REJSON_ARGS) \
	 FORCE='' RLTEST= ENV_ONLY=1 LOG_LEVEL=$(LOG_LEVEL) MODULE=$(MODULE) REDIS_STANDALONE=$(REDIS_STANDALONE) SA=$(SA) \
		$(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))
else
ifeq ($(GDB),1)
ifeq ($(CLANG),1)
	$(SHOW)lldb -o run -- redis-server --loadmodule $(abspath $(TARGET))
else
	$(SHOW)gdb -ex r --args redis-server --loadmodule $(abspath $(TARGET))
endif
else
	$(SHOW)redis-server --loadmodule $(abspath $(TARGET))
endif
endif

.PHONY: run

#----------------------------------------------------------------------------------------------

CTEST_DEFS=\
	BINROOT=$(BINROOT) \
	BINDIR=$(BINDIR) \
	COV=$(COV) \
	SAN=$(SAN) \
	SLOW=$(SLOW)

#----------------------------------------------------------------------------------------------

FLOW_TESTS_DEFS=\
	BINROOT=$(BINROOT) \
	VG=$(VALGRIND) \
	VG_LEAKS=0 \
	SAN=$(SAN) \
	EXT=$(EXT)

export EXT_TEST_PATH:=$(BINDIR)/example_extension/libexample_extension.so

ifeq ($(SLOW),1)
_TEST_PARALLEL=0
else ifeq ($(TEST_PARALLEL),)
_TEST_PARALLEL=1
else
_TEST_PARALLEL=$(TEST_PARALLEL)
endif

test: unit-tests pytest

unit-tests:
	$(SHOW)BINROOT=$(BINROOT) BENCH=$(BENCHMARK) TEST=$(TEST) GDB=$(GDB) $(ROOT)/sbin/unit-tests

pytest:
	@printf "\n-------------- Running python flow test ------------------\n"
	$(SHOW)REJSON=$(REJSON) REJSON_BRANCH=$(REJSON_BRANCH) REJSON_PATH=$(REJSON_PATH) REJSON_ARGS=$(REJSON_ARGS) \
	TEST=$(TEST) $(FLOW_TESTS_DEFS) FORCE='' PARALLEL=$(_TEST_PARALLEL) \
	LOG_LEVEL=$(LOG_LEVEL) TEST_TIMEOUT=$(TEST_TIMEOUT) MODULE=$(MODULE) REDIS_STANDALONE=$(REDIS_STANDALONE) SA=$(SA) \
		$(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))

#----------------------------------------------------------------------------------------------

c-tests:
	$(SHOW)BINROOT=$(BINROOT) C_TESTS=1 TEST=$(TEST) GDB=$(GDB) $(ROOT)/sbin/unit-tests

cpp-tests:
	$(SHOW)BINROOT=$(BINROOT) CPP_TESTS=1 BENCH=$(BENCHMARK) TEST=$(TEST) GDB=$(GDB) $(ROOT)/sbin/unit-tests

vecsim-bench:
	$(SHOW)$(BINROOT)/search/tests/cpptests/rsbench

.PHONY: test unit-tests pytest c_tests cpp_tests vecsim-bench

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

ifneq ($(RAMP_YAML),)

# RAMP_VARIANT=$(subst release,,$(FLAVOR))$(_VARIANT.string)

PACK_ARGS=\
	VARIANT=$(VARIANT) \
	PACKAGE_NAME=$(PACKAGE_NAME) \
	MODULE_NAME=$(MODULE_NAME) \
	RAMP_YAML=$(RAMP_YAML) \
	RAMP_ARGS=$(RAMP_ARGS)

RAMP.release:=$(shell JUST_PRINT=1 RAMP=1 DEPS=0 RELEASE=1 SNAPSHOT=0 $(PACK_ARGS) $(ROOT)/sbin/pack.sh)

ifneq ($(FORCE),1)
bin/artifacts/$(RAMP.release): $(RAMP_YAML) # $(TARGET)
else
bin/artifacts/$(RAMP.release): __force
endif

	@echo Packing module...
	$(SHOW)$(PACK_ARGS) $(ROOT)/sbin/pack.sh $(TARGET)

pack: bin/artifacts/$(RAMP.release)

else

pack:
	@echo "Nothing to pack for this configuration."

endif # RAML_YAML

upload-release:
	$(SHOW)RELEASE=1 ./sbin/upload-artifacts

upload-artifacts:
	$(SHOW)SNAPSHOT=1 ./sbin/upload-artifacts

.PHONY: pack upload-artifacts upload-release

#----------------------------------------------------------------------------------------------
ifeq ($(REMOTE),1)
BENCHMARK_ARGS=run-remote
else
BENCHMARK_ARGS=run-local
endif

BENCHMARK_ARGS += --module_path $(realpath $(TARGET)) --required-module search

ifneq ($(BENCHMARK),)
BENCHMARK_ARGS += --test $(BENCHMARK)
endif


# Todo: fix that, currently will not work manually with rejson
benchmark:
ifeq ($(REJSON),1)
	ROOT=$(ROOT) REJSON_BRANCH=$(REJSON_BRANCH) $(shell $(ROOT)/tests/deps/setup_rejson.sh)
	BENCHMARK_ARGS += --module_path $(realpath $(JSON_BIN_DIR)) --required-module ReJSON
endif

	$(SHOW)cd tests/benchmarks ;\
	redisbench-admin $(BENCHMARK_ARGS)

.PHONY: benchmark

#----------------------------------------------------------------------------------------------

COV_EXCLUDE_DIRS += \
	bin \
	deps \
	tests \
	coord/tests

COV_EXCLUDE+=$(foreach D,$(COV_EXCLUDE_DIRS),'$(realpath $(ROOT))/$(D)/*')

coverage:
	$(SHOW)$(MAKE) build COV=1
	$(SHOW)$(COVERAGE_RESET)
	$(SHOW)$(MAKE) unit-tests COV=1
	$(SHOW)$(MAKE) pytest REDIS_STANDALONE=1 COV=1 REJSON_BRANCH=$(REJSON_BRANCH)
	$(SHOW)$(MAKE) pytest REDIS_STANDALONE=0 COV=1 REJSON_BRANCH=$(REJSON_BRANCH)
	$(SHOW)$(COVERAGE_COLLECT_REPORT)

.PHONY: coverage

#----------------------------------------------------------------------------------------------

docker:
	$(SHOW)$(MAKE) -C build/docker
ifeq ($(VERIFY),1)
	$(SHOW)$(MAKE) -C build/docker verify
endif

# box:
# ifneq ($(OSNICK),)
# 	@docker run -it -v $(PWD):/build --cap-add=SYS_PTRACE --security-opt seccomp=unconfined $(shell $(ROOT)/deps/readies/bin/platform --docker-from-osnick $(OSNICK)) bash
# else
# 	@docker run -it -v $(PWD):/build --cap-add=SYS_PTRACE --security-opt seccomp=unconfined $(shell $(ROOT)/deps/readies/bin/platform --docker) bash
# endif

ifneq ($(wildcard /w/*),)
SANBOX_ARGS += -v /w:/w
endif

sanbox:
	@docker run -it -v $(PWD):/search -w /search --cap-add=SYS_PTRACE --security-opt seccomp=unconfined $(SANBOX_ARGS) redisfab/clang:16-$(ARCH)-bullseye bash

.PHONY: box sanbox
