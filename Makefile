
.NOTPARALLEL:

ROOT=.

MK.cmake=1
SRCDIR=.

IGNORE_MISSING_DEPS=1
build: verify_build $(DEFAULT_TARGETS) $(MK_MAKEFILES) $(TARGET)
	@echo "Build completed."
verify_build:
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

include deps/readies/mk/main

#----------------------------------------------------------------------------------------------

define HELPTEXT
make setup         # install prerequisited (CAUTION: THIS WILL MODIFY YOUR SYSTEM)
make fetch         # download and prepare dependent modules

make build          # compile and link
  COORD=1|oss|rlec    # build coordinator (1|oss: Open Source, rlec: Enterprise)
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
  ENABLE_ASSERT=1     # enable assertions (disabled by default)

make parsers       # build parsers code
make clean         # remove build artifacts
  ALL=1              # remove entire artifacts directory
make cc            # compile a single file
  FILE=file          # source of file to build
make run           # run redis with RediSearch
  COORD=1|oss        # run cluster
  WITH_RLTEST=1      # run with RLTest wrapper
  GDB=1              # invoke using gdb

make test          # run all tests
  COORD=1|oss|rlec   # test coordinator (1|oss: Open Source, rlec: Enterprise)
  TEST=name          # run specified test

make pytest        # run python tests (tests/pytests)
  COORD=1|oss|rlec     # test coordinator (1|oss: Open Source, rlec: Enterprise)
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
  REDIS_VER=6    	   # redis version to run against
  LOG_LEVEL=<level>    # server log level (default: debug)

make unit-tests    # run unit tests (C and C++)
  TEST=name          # e.g. TEST=FGCTest.testRemoveLastBlock
make c-tests       # run C tests (from tests/ctests)
make cpp-tests     # run C++ tests (from tests/cpptests)
make vecsim-bench  # run VecSim micro-benchmark

make callgrind     # produce a call graph
  REDIS_ARGS="args"

make pack             # create installation packages (default: 'redisearch-oss' package)
  COORD=rlec            # pack RLEC coordinator ('redisearch' package)
  LITE=1                # pack RediSearchLight ('redisearch-light' package)

make upload-artifacts   # copy snapshot packages to S3
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

ifeq ($(COORD),) # Standalone build

	ifeq ($(STATIC),1) # Static build
		BINDIR=$(BINROOT)/search-static
		SRCDIR=.
		TARGET=$(BINDIR)/redisearch.a
		PACKAGE_NAME=
		MODULE_NAME=
		RAMP_YAML=

	else ifneq ($(LITE),1) # OSS Search
		BINDIR=$(BINROOT)/search
		SRCDIR=.
		TARGET=$(BINDIR)/redisearch.so
		PACKAGE_NAME=redisearch-oss
		MODULE_NAME=search
		RAMP_YAML=pack/ramp.yml
		PACKAGE_S3_DIR=redisearch-oss

	else # Search Lite
		BINDIR=$(BINROOT)/search-lite
		SRCDIR=.
		TARGET=$(BINDIR)/redisearch.so
		PACKAGE_NAME=redisearch-light
		MODULE_NAME=searchlight
		RAMP_YAML=pack/ramp-light.yml
		PACKAGE_S3_DIR=redisearch
	endif

else # COORD

	ifeq ($(STATIC),1)
		___:=$(error STATIC=1 is incompatible with COORD)
	endif

	ifeq ($(COORD),1)
		override COORD:=oss
	endif

	ifeq ($(COORD),oss) # OSS Coordinator
		BINDIR=$(BINROOT)/coord-oss
		SRCDIR=coord
		TARGET=$(BINDIR)/module-oss.so
		PACKAGE_NAME=redisearch
		MODULE_NAME=search
		RAMP_YAML=

	else ifeq ($(COORD),rlec) # RLEC Coordinator
		BINDIR=$(BINROOT)/coord-rlec
		SRCDIR=coord
		TARGET=$(BINDIR)/module-enterprise.so
		PACKAGE_NAME=redisearch
		MODULE_NAME=search
		RAMP_YAML=coord/pack/ramp.yml
		PACKAGE_S3_DIR=redisearch

	else
		___:=$(error COORD should be either oss or rlec)
	endif

	LIBUV_DIR=$(ROOT)/deps/libuv
	export LIBUV_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/libuv
	include build/libuv/Makefile.defs

	HIREDIS_DIR=$(ROOT)/deps/hiredis
	HIREDIS_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/hiredis
	include build/hiredis/Makefile.defs

endif # COORD

export COORD
export PACKAGE_NAME

#----------------------------------------------------------------------------------------------

CC_C_STD=gnu11
CC_CXX_STD=c++11

CC_STATIC_LIBSTDCXX ?= 1

CC_COMMON_H=src/common.h

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

ifneq ($(COORD),)
CMAKE_COORD += -DCOORD_TYPE=$(COORD)
endif

CMAKE_FILES= \
	CMakeLists.txt \
	build/cmake/redisearch_cflags.cmake \
	build/cmake/redisearch_debug.cmake \
	deps/friso/CMakeLists.txt \
	deps/phonetics/CMakeLists.txt \
	deps/snowball/CMakeLists.txt \
	deps/rmutil/CMakeLists.txt

ifneq ($(NO_TESTS),1)
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

#----------------------------------------------------------------------------------------------
BOOST_DIR ?= $(ROOT)/.install/boost
_CMAKE_FLAGS += -DMODULE_NAME=$(MODULE_NAME) -DBOOST_DIR=$(BOOST_DIR) -DSAN=$(SAN)

ifeq ($(OS),macos)
_CMAKE_FLAGS += -DLIBSSL_DIR=$(openssl_prefix)
endif

_CMAKE_FLAGS += $(CMAKE_ARGS) $(CMAKE_STATIC) $(CMAKE_COORD) $(CMAKE_TEST)

#----------------------------------------------------------------------------------------------


include $(MK)/defs

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

DEPENDENCIES=libuv #@@ hiredis

ifneq ($(filter all deps $(DEPENDENCIES) pack,$(MAKECMDGOALS)),)
DEPS=1
endif

.PHONY: deps $(DEPENDENCIES)

#----------------------------------------------------------------------------------------------

all: bindirs $(TARGET)

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
	$(SHOW)cd src/query_parser/v1 ;\
	rm -f lexer.c parser.c
	$(SHOW)cd src/query_parser/v2 ;\
	rm -f lexer.c parser.c
endif
	$(SHOW)$(MAKE) -C src/aggregate/expr
	$(SHOW)$(MAKE) -C src/query_parser/v1
	$(SHOW)$(MAKE) -C src/query_parser/v2

.PHONY: parsers

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

deps: $(LIBUV) #@@ $(HIREDIS)

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

ifeq ($(COORD),)
CMAKE_TARGET=rscore
CMAKE_TARGET_DIR=
else
CMAKE_TARGET=coordinator-core
CMAKE_TARGET_DIR=src/
endif

CMAKE_TARGET_BUILD_DIR=$(CMAKE_TARGET_DIR)CMakeFiles/$(CMAKE_TARGET).dir

cc:
	@$(READIES)/bin/sep1
	$(SHOW)$(MAKE) -C $(BINDIR) -f $(CMAKE_TARGET_BUILD_DIR)/build.make $(CMAKE_TARGET_BUILD_DIR)/$(FILE).o

.PHONY: cc

#----------------------------------------------------------------------------------------------

ifeq ($(COORD),oss)
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
	FORCE='' RLTEST= ENV_ONLY=1 LOG_LEVEL=$(LOG_LEVEL) \
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
	$(SHOW)BINROOT=$(BINROOT) COORD=$(COORD) BENCH=$(BENCHMARK) TEST=$(TEST) GDB=$(GDB) $(ROOT)/sbin/unit-tests

pytest:
	@printf "\n-------------- Running python flow test ------------------\n"
	$(SHOW)REJSON=$(REJSON) REJSON_BRANCH=$(REJSON_BRANCH) REJSON_PATH=$(REJSON_PATH) REJSON_ARGS=$(REJSON_ARGS) \
	$(FLOW_TESTS_DEFS) FORCE='' PARALLEL=$(_TEST_PARALLEL) \
	LOG_LEVEL=$(LOG_LEVEL) TEST_TIMEOUT=$(TEST_TIMEOUT) \
		$(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))

#----------------------------------------------------------------------------------------------

c-tests:
	$(SHOW)BINROOT=$(BINROOT) COORD=$(COORD) C_TESTS=1 TEST=$(TEST) GDB=$(GDB) $(ROOT)/sbin/unit-tests

cpp-tests:
	$(SHOW)BINROOT=$(BINROOT) COORD=$(COORD) CPP_TESTS=1 BENCH=$(BENCHMARK) TEST=$(TEST) GDB=$(GDB) $(ROOT)/sbin/unit-tests

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

upload-artifacts:
	./sbin/upload-artifacts

.PHONY: pack upload-artifacts

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
	$(SHOW)$(MAKE) build COORD=oss COV=1
	$(SHOW)$(COVERAGE_RESET)
	-$(SHOW)$(MAKE) unit-tests COV=1 $(REJSON_COV_ARG)
	-$(SHOW)$(MAKE) pytest COV=1 REJSON_BRANCH=$(REJSON_BRANCH)
	-$(SHOW)$(MAKE) unit-tests COORD=oss COV=1 $(REJSON_COV_ARG)
	-$(SHOW)$(MAKE) pytest COORD=oss COV=1 REJSON_BRANCH=$(REJSON_BRANCH)
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
