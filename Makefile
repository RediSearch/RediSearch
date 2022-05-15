
ifeq (n,$(findstring n,$(firstword -$(MAKEFLAGS))))
DRY_RUN:=1
else
DRY_RUN:=
endif

ifneq ($(BB),)
SLOW:=1
endif

ifneq ($(filter coverage show-cov upload-cov,$(MAKECMDGOALS)),)
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
override SAN=memory
else ifeq ($(SAN),addr)
override SAN=address
endif

ifeq ($(SAN),address)
CMAKE_SAN=-DUSE_ASAN=ON
export REDIS_SERVER ?= redis-server-asan-6.2

else ifeq ($(SAN),memory)
CMAKE_SAN=-DUSE_MSAN=ON -DMSAN_PREFIX=/opt/llvm-project/build-msan
export REDIS_SERVER ?= redis-server-msan-6.2

else ifeq ($(SAN),leak)
else ifeq ($(SAN),thread)
else
$(error SAN=mem|addr|leak|thread)
endif

export SAN
endif # SAN

#----------------------------------------------------------------------------------------------

ROOT=.

ifeq ($(wildcard $(ROOT)/deps/readies/*),)
___:=$(shell git submodule update --init --recursive &> /dev/null)
endif

MK.pyver:=3
include deps/readies/mk/main

#----------------------------------------------------------------------------------------------

define HELPTEXT
make setup         # install prerequisited (CAUTION: THIS WILL MODIFY YOUR SYSTEM)
make fetch         # download and prepare dependant modules

make build          # compile and link
  COORD=1|oss|rlec    # build coordinator (1|oss: Open Source, rlec: Enterprise)
  STATIC=1            # build as static lib
  LITE=1              # build RediSearchLight
  DEBUG=1             # build for debugging
  STATIC_LIBSTDCXX=0  # link libstdc++ dynamically (default: 1)
  NO_TESTS=1          # disable unit tests
  WHY=1               # explain CMake decisions (in /tmp/cmake-why)
  FORCE=1             # Force CMake rerun (default)
  CMAKE_ARGS=...      # extra arguments to CMake
  VG=1                # build for Valgrind
  SAN=type            # build with LLVM sanitizer (type=address|memory|leak|thread) 
  SLOW=1              # do not parallelize build (for diagnostics)
make parsers       # build parsers code
make clean         # remove build artifacts
  ALL=1              # remove entire artifacts directory

make run           # run redis with RediSearch
  GDB=1              # invoke using gdb

make test          # run all tests (via ctest)
  COORD=1|oss|rlec   # test coordinator (1|oss: Open Source, rlec: Enterprise)
  TEST=regex         # run tests that match regex
  TESTDEBUG=1        # be very verbose (CTest-related)
  CTEST_ARG=...      # pass args to CTest
  CTEST_PARALLEL=n   # run ctests in n parallel jobs
make pytest        # run python tests (tests/pytests)
  COORD=1|oss|rlec   # test coordinator (1|oss: Open Source, rlec: Enterprise)
  TEST=name          # e.g. TEST=test:testSearch
  RLTEST_ARGS=...    # pass args to RLTest
  REJSON=1|0         # also load RedisJSON module
  REJSON_PATH=path   # use RedisJSON module at `path`
  EXT=1              # External (existing) environment
  GDB=1              # RLTest interactive debugging
  VG=1               # use Valgrind
  VG_LEAKS=0         # do not search leaks with Valgrind
  SAN=type           # use LLVM sanitizer (type=address|memory|leak|thread) 
  ONLY_STABLE=1      # skip unstable tests
make c_tests       # run C tests (from tests/ctests)
make cpp_tests     # run C++ tests (from tests/cpptests)
  TEST=name          # e.g. TEST=FGCTest.testRemoveLastBlock
  BENCHMARK=1		 # run micro-benchmark

make callgrind     # produce a call graph
  REDIS_ARGS="args"

make pack             # create installation packages (default: 'redisearch-oss' package)
  COORD=rlec            # pack RLEC coordinator ('redisearch' package)
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

make box           # create container with volumen mapping into /search
  OSNICK=nick        # platform spec
make sanbox        # create container with CLang Sanitizer

endef

#----------------------------------------------------------------------------------------------

ifeq ($(COORD),) # Standalone build

	ifeq ($(STATIC),1) # Static build
		CMAKE_DIR=$(ROOT)
		BINDIR=$(BINROOT)/search-static
		SRCDIR=src
		TARGET=$(BINDIR)/redisearch.a
		PACKAGE_NAME=
		RAMP_MODULE_NAME=
		RAMP_YAML=

	else ifneq ($(LITE),1) # OSS Search
		CMAKE_DIR=$(ROOT)
		BINDIR=$(BINROOT)/search
		SRCDIR=src
		TARGET=$(BINDIR)/redisearch.so
		PACKAGE_NAME=redisearch-oss
		RAMP_MODULE_NAME=search
		RAMP_YAML=pack/ramp.yml
		PACKAGE_S3_DIR=redisearch-oss

	else # Search Lite
		CMAKE_DIR=$(ROOT)
		BINDIR=$(BINROOT)/search-lite
		SRCDIR=src
		TARGET=$(BINDIR)/redisearch.so
		PACKAGE_NAME=redisearch-light
		RAMP_MODULE_NAME=searchlight
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
		CMAKE_DIR=$(ROOT)/coord
		BINDIR=$(BINROOT)/coord-oss
		SRCDIR=coord/src
		TARGET=$(BINDIR)/module-oss.so
		PACKAGE_NAME=redisearch
		RAMP_MODULE_NAME=search
		RAMP_YAML=

	else ifeq ($(COORD),rlec) # RLEC Coordinator
		CMAKE_DIR=$(ROOT)/coord
		BINDIR=$(BINROOT)/coord-rlec
		SRCDIR=coord/src
		TARGET=$(BINDIR)/module-enterprise.so
		PACKAGE_NAME=redisearch
		RAMP_MODULE_NAME=search
		RAMP_YAML=coord/pack/ramp.yml
		PACKAGE_S3_DIR=redisearch

	else
		___:=$(error COORD should be either oss or rlec)
	endif

	export LIBUV_BINDIR=$(realpath bin/$(FULL_VARIANT.release)/libuv)
	include build/libuv/Makefile.defs

	HIREDIS_BINDIR=bin/$(FULL_VARIANT.release)/hiredis
	include build/hiredis/Makefile.defs

endif # COORD

export COORD

#----------------------------------------------------------------------------------------------

export PACKAGE_NAME

#----------------------------------------------------------------------------------------------

STATIC_LIBSTDCXX ?= 1

ifeq ($(COV),1)
CMAKE_COV += -DUSE_COVERAGE=ON
endif

ifneq ($(SAN),)
CMAKE_SAN += -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
endif

ifeq ($(PROFILE),1)
CMAKE_PROFILE=-DPROFILE=ON
endif

ifeq ($(STATIC_LIBSTDCXX),1)
CMAKE_STATIC_LIBSTDCXX=-DSTATIC_LIBSTDCXX=on
else
CMAKE_STATIC_LIBSTDCXX=-DSTATIC_LIBSTDCXX=off
endif

ifeq ($(DEBUG),1)
CMAKE_BUILD_TYPE=DEBUG
else
CMAKE_BUILD_TYPE=RelWithDebInfo
endif
CMAKE_DEBUG=-DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE)

ifneq ($(NO_TESTS),1)
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

HAVE_MARCH_OPTS:=$(shell $(MK)/cc-have-opts)
CMAKE_CXX_MARCH_FLAGS=$(foreach opt,$(HAVE_MARCH_OPTS),-D$(opt))
CMAKE_HAVE_MARCH_OPTS=$(foreach opt,$(HAVE_MARCH_OPTS),-D$(opt)=on) -DMARCH_CXX_FLAGS="$(CMAKE_CXX_MARCH_FLAGS)"

#----------------------------------------------------------------------------------------------

CMAKE_FLAGS=\
	-Wno-dev \
	-DGIT_SHA=$(GIT_SHA) \
	-DGIT_VERSPEC=$(GIT_VERSPEC) \
	-DRS_MODULE_NAME=$(RAMP_MODULE_NAME) \
	-DOS=$(OS) \
	-DOSNICK=$(OSNICK) \
	-DARCH=$(ARCH)

CMAKE_FLAGS += $(CMAKE_ARGS) $(CMAKE_DEBUG) $(CMAKE_STATIC) $(CMAKE_COORD) $(CMAKE_COV) \
	$(CMAKE_SAN) $(CMAKE_TEST) $(CMAKE_WHY) $(CMAKE_PROFILE) $(CMAKE_STATIC_LIBSTDCXX)

#----------------------------------------------------------------------------------------------

include $(MK)/defs

MK_CUSTOM_CLEAN=1

#----------------------------------------------------------------------------------------------

MISSING_DEPS:=
ifeq ($(wildcard $(LIBUV)),)
MISSING_DEPS += $(LIBUV)
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
	$(SHOW)mkdir -p $(BINROOT)
	$(SHOW)cd $(BINDIR) && cmake $(CMAKE_DIR) $(CMAKE_FLAGS)

$(TARGET): $(MISSING_DEPS) $(BINDIR)/Makefile
	@echo Building $(TARGET) ...
ifneq ($(DRY_RUN),1)
	$(SHOW)$(MAKE) -C $(BINDIR) $(MAKE_J)
else
	@make -C $(BINDIR) $(MAKE_J)
endif

.PHONY: build clean run 

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
	rm -f lexer.c parser-toplevel.c parser.c.inc
	$(SHOW)cd src/query_parser/v1 ;\
	rm -f lexer.c parser-toplevel.c parser.c.inc
	$(SHOW)cd src/query_parser/v2 ;\
	rm -f lexer.c parser-toplevel.c parser.c.inc
endif
	$(SHOW)$(MAKE) -C src/aggregate/expr
	$(SHOW)$(MAKE) -C src/query_parser/v1
	$(SHOW)$(MAKE) -C src/query_parser/v2

.PHONY: parsers

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

deps: $(LIBUV) $(HIREDIS)

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
	$(SHOW)./deps/readies/bin/getpy3
	$(SHOW)./sbin/system-setup.py 

#----------------------------------------------------------------------------------------------

fetch:
	-git submodule update --init --recursive

#----------------------------------------------------------------------------------------------

run:
ifeq ($(GDB),1)
	gdb -ex r --args redis-server --loadmodule $(abspath $(TARGET))
else
	@redis-server --loadmodule $(abspath $(TARGET))
endif

#----------------------------------------------------------------------------------------------

export REJSON ?= 1

ifneq ($(SAN),)
export ASAN_OPTIONS=detect_odr_violation=0
endif

ifeq ($(TESTDEBUG),1)
override CTEST_ARGS.debug += --debug
endif

ifeq ($(SLOW),1)
	override CTEST_PARALLEL=
else
	ifneq ($(SAN),)
		override CTEST_PARALLEL=
	else ifeq ($(COV),1)
		override CTEST_PARALLEL=
	else
		# CTEST_PARALLEL:=$(shell $(ROOT)/deps/readies/bin/nproc)
		override CTEST_PARALLEL=
	endif
endif # !SLOW

ifneq ($(CTEST_PARALLEL),)
CTEST_ARGS.parallel += -j$(CTEST_PARALLEL)
endif

override CTEST_ARGS += \
	--output-on-failure \
	--timeout 15000 \
	$(CTEST_ARGS.debug) \
	$(CTEST_ARGS.parallel)

CTEST_DEFS += \
	BINROOT=$(BINROOT)

override FLOW_TESTS_ARGS+=\
	BINROOT=$(BINROOT) \
	VG=$(VALGRIND) VG_LEAKS=0

ifeq ($(EXT),1)
FLOW_TESTS_ARGS += EXISTING_ENV=1
endif

export EXT_TEST_PATH:=$(BINDIR)/example_extension/libexample_extension.so

ifneq ($(REJSON),0)
ifneq ($(SAN),)
REJSON_SO=$(BINROOT)/RedisJSON/rejson.so

$(REJSON_SO):
	$(SHOW)BINROOT=$(BINROOT) ./sbin/build-redisjson
else
REJSON_SO=
endif
endif

ifeq ($(SLOW),1)
_RLTEST_PARALLEL=0
else
# _RLTEST_PARALLEL=1
_RLTEST_PARALLEL=8
endif

test: $(REJSON_SO)
ifneq ($(TEST),)
	$(SHOW)set -e; cd $(BINDIR); $(CTEST_DEFS) RLTEST_ARGS+="-s -v" ctest $(CTEST_ARGS) -vv -R $(TEST)
else
ifeq ($(ARCH),arm64v8)
	$(SHOW)$(FLOW_TESTS_ARGS) FORCE='' $(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))
else
	$(SHOW)set -e; cd $(BINDIR); $(CTEST_DEFS) ctest $(CTEST_ARGS)
endif
ifeq ($(COORD),oss)
	$(SHOW)$(FLOW_TESTS_ARGS) FORCE='' $(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))
endif
endif

pytest: $(REJSON_SO)
	$(SHOW)TEST=$(TEST) $(FLOW_TESTS_ARGS) FORCE='' PARALLEL=$(_RLTEST_PARALLEL) $(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))

#----------------------------------------------------------------------------------------------

ifeq ($(GDB),1)
GDB_CMD=gdb -ex r --args
else
GDB_CMD=
endif

c_tests:
ifeq ($(COORD),)
ifeq ($(TEST),)
	$(SHOW)set -e ;\
	cd tests/ctests ;\
	find $(abspath $(BINROOT)/search/tests/ctests) -name "test_*" -type f -executable -print0 | xargs -0 -n1 bash -c
else
	$(SHOW)set -e ;\
	cd tests/ctests ;\
	${GDB_CMD} $(BINROOT)/search/tests/ctests/$(TEST)
endif
else ifeq ($(COORD),oss)
ifeq ($(TEST),)
	$(SHOW)set -e; find $(abspath $(BINROOT)/coord-oss/tests/unit) -name "test_*" -type f -executable -print0 | xargs -0 -n1 bash -c
else
	$(SHOW)${GDB_CMD} $(BINROOT)/coord-oss/tests/unit/$(TEST)
endif
endif

cpp_tests:
ifeq ($(BENCHMARK), 1)
	$(SHOW)$(BINROOT)/search/tests/cpptests/rsbench
else ifeq ($(TEST),)
	$(SHOW)$(BINROOT)/search/tests/cpptests/rstest
else
	$(SHOW)$(GDB_CMD) $(abspath $(BINROOT)/search/tests/cpptests/rstest) --gtest_filter=$(TEST)
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

ifneq ($(RAMP_YAML),)

PACK_ARGS=\
	VARIANT=$(RAMP_VARIANT) \
	PACKAGE_NAME=$(PACKAGE_NAME) \
	MODULE_NAME=$(RAMP_MODULE_NAME) \
	RAMP_YAML=$(RAMP_YAML) \
	RAMP_ARGS=$(RAMP_ARGS)

bin/artifacts/$(RAMP.release) : $(RAMP_YAML) $(TARGET)
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

ifeq ($(REJSON),1)
BENCHMARK_ARGS += --module_path $(realpath $(REJSON_PATH)) --required-module ReJSON
endif

ifneq ($(BENCHMARK),)
BENCHMARK_ARGS += --test $(BENCHMARK)
endif

benchmark:
	$(SHOW)cd tests/benchmarks ;\
	redisbench-admin $(BENCHMARK_ARGS)

.PHONY: benchmark

#----------------------------------------------------------------------------------------------

COV_EXCLUDE_DIRS += \
	deps \
	tests \
	coord/tests

COV_EXCLUDE+=$(foreach D,$(COV_EXCLUDE_DIRS),'$(realpath $(ROOT))/$(D)/*')

ifneq ($(REJSON_PATH),)
export REJSON_PATH
else
REJSON_MODULE_FILE:=$(shell mktemp /tmp/rejson.XXXX)
endif

coverage:
ifeq ($(REJSON_PATH),)
	$(SHOW)MODULE_FILE=$(REJSON_MODULE_FILE) ./sbin/get-redisjson
endif
	$(SHOW)$(MAKE) build COV=1
	$(SHOW)$(MAKE) build COORD=oss COV=1
	$(SHOW)$(COVERAGE_RESET)
ifneq ($(REJSON_PATH),)
	-$(SHOW)$(MAKE) test COV=1
	-$(SHOW)$(MAKE) test COORD=oss COV=1
else
	-$(SHOW)$(MAKE) test COV=1 REJSON_PATH=$$(cat $(REJSON_MODULE_FILE))
	-$(SHOW)$(MAKE) test COORD=oss COV=1 REJSON_PATH=$$(cat $(REJSON_MODULE_FILE))
endif
	$(SHOW)$(COVERAGE_COLLECT_REPORT)

.PHONY: coverage

#----------------------------------------------------------------------------------------------

docker:
	$(SHOW)$(MAKE) -C build/docker

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
	@docker run -it -v $(PWD):/search -w /search --cap-add=SYS_PTRACE --security-opt seccomp=unconfined $(SANBOX_ARGS) redisfab/clang:13-x64-bullseye bash

.PHONY: box sanbox
