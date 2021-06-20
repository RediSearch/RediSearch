
ROOT=.
include deps/readies/mk/main

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

define HELP
make setup         # install prerequisited (CAUTION: THIS WILL MODIFY YOUR SYSTEM)
make fetch         # download and prepare dependant modules

make build         # compile and link
  DEBUG=1          # build for debugging (implies WITH_TESTS=1)
  WITH_TESTS=1     # enable unit tests
  WHY=1            # explain CMake decisions (in /tmp/cmake-why)
  FORCE=1          # Force CMake rerun
  CMAKE_ARGS=...   # extra arguments to CMake
  STATIC=1         # build as static lib
  VG=1             # build for Valgrind
  SAN=type         # build with LLVM sanitizer (type=address|memory|leak|thread) 
make parsers       # build parsers code
make clean         # remove build artifacts
  ALL=1              # remove entire artifacts directory

make run           # run redis with RediSearch
  DEBUG=1            # invoke using gdb

make test          # run all tests (via ctest)
  TEST=regex
  TESTDEBUG=1        # be very verbose (CTest-related)
  CTEST_ARG=...      # pass args to CTest
make pytest        # run python tests (tests/pytests)
  TEST=name          # e.g. TEST=test:testSearch
  RLTEST_ARGS=...    # pass args to RLTest
  REJSON=1|0         # also load RedisJSON module
  REJSON_PATH=path   # use RedisJSON module at `path`
  GDB=1              # RLTest interactive debugging
  VG=1               # use Valgrind
  SAN=type           # use LLVM sanitizer (type=address|memory|leak|thread) 
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

COMPAT_MODULE := src/redisearch.so

ifeq ($(SAN),)
COMPAT_DIR ?= build
else
COMPAT_DIR ?= build-$(SAN_DIR)
endif

BINROOT=$(COMPAT_DIR)
BINDIR=$(COMPAT_DIR)

SRCDIR=src

TARGET=$(BINDIR)/redisearch.so

PACKAGE_NAME ?= redisearch-oss
export PACKAGE_NAME

#----------------------------------------------------------------------------------------------

ifneq ($(SAN),)
CMAKE_SAN += -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
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
CMAKE_STATIC +=\
	-DRS_FORCE_NO_GITVERSION=ON \
	-DRS_BUILD_STATIC=ON
endif

CMAKE_FILES= \
	CMakeLists.txt \
	cmake/redisearch_cflags.cmake \
	cmake/redisearch_debug.cmake \
	src/dep/friso/CMakeLists.txt \
	src/dep/phonetics/CMakeLists.txt \
	src/dep/snowball/CMakeLists.txt \
	src/rmutil/CMakeLists.txt

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

CMAKE_FLAGS=$(CMAKE_ARGS) $(CMAKE_DEBUG) $(CMAKE_STATIC) $(CMAKE_SAN) $(CMAKE_TEST) $(CMAKE_WHY)

#----------------------------------------------------------------------------------------------

include $(MK)/defs

MK_CUSTOM_CLEAN=1

include $(MK)/rules

$(COMPAT_MODULE): $(BINROOT)/redisearch.so
	cp $^ $@

ifeq ($(FORCE),1)
.PHONY: __force

$(BINROOT)/Makefile: __force
else
$(BINROOT)/Makefile : $(CMAKE_FILES)
endif
	@echo Building with CMake ...
ifeq ($(WHY),1)
	@echo CMake log is in /tmp/cmake-why
endif
	@mkdir -p $(BINROOT)
	@cd $(BINROOT) && cmake .. $(CMAKE_FLAGS)

$(COMPAT_DIR)/redisearch.so: $(BINROOT)/Makefile
	@echo Building ...
	@$(MAKE) -C $(BINROOT) -j$(shell nproc)
	@[ -f $(COMPAT_DIR)/redisearch.so ] && touch $(COMPAT_DIR)/redisearch.so
#	if [ ! -f src/redisearch.so ]; then cd src; ln -s ../$(BINROOT)/redisearch.so; fi

.PHONY: build clean run 

clean:
ifeq ($(ALL),1)
	rm -rf $(BINROOT)
else
	$(MAKE) -C $(BINROOT) clean
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

setup:
	@echo Setting up system...
	$(SHOW)./deps/readies/bin/getpy2
	$(SHOW)./system-setup.py 

#----------------------------------------------------------------------------------------------

fetch:
	-git submodule update --init --recursive

#----------------------------------------------------------------------------------------------

run:
	@redis-server --loadmodule $(COMPAT_MODULE)

#----------------------------------------------------------------------------------------------

BENCHMARK_ARGS = redisbench-admin run-local

ifneq ($(REMOTE),)
	BENCHMARK_ARGS = redisbench-admin run-remote 
endif

BENCHMARK_ARGS += --module_path $(realpath $(TARGET))
ifneq ($(BENCHMARK),)
	BENCHMARK_ARGS += --test $(BENCHMARK)
endif


benchmark: $(TARGET)
	cd ./tests/ci.benchmarks; $(BENCHMARK_ARGS) ; cd ../../

#----------------------------------------------------------------------------------------------
export REJSON ?= 1

ifeq ($(TESTDEBUG),1)
override CTEST_ARGS += --debug
endif

ifneq ($(CTEST_PARALLEL),)
override CTEST_ARGS += -j$(CTEST_PARALLEL)
endif

test:
ifneq ($(TEST),)
	@set -e; cd $(BINROOT); CTEST_OUTPUT_ON_FAILURE=1 RLTEST_ARGS="-s -v" ctest $(CTEST_ARGS) -vv -R $(TEST)
else
	@set -e; cd $(BINROOT); ctest $(CTEST_ARGS)
endif

pytest:
	@TEST=$(TEST) FORCE= $(ROOT)/tests/pytests/runtests.sh $(abspath $(TARGET))

ifeq ($(GDB),1)
GDB_CMD=gdb -ex r --args
else
GDB_CMD=
endif

c_tests:
	find $(abspath $(BINROOT)/tests/ctests) -name "test_*" -type f -executable -exec ${GDB_CMD} {} \;

cpp_tests:
ifeq ($(TEST),)
	find $(abspath $(BINROOT)/tests/cpptests) -name "test_*" -type f -executable -exec ${GDB_CMD} {} \;
else
	set -e ;\
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

callgrind: $(COMPAT_MODULE)
	$(SHOW)valgrind $(CALLGRIND_ARGS) --loadmodule $(abspath $(TARGET)) $(REDIS_ARGS)

#----------------------------------------------------------------------------------------------

RAMP_VARIANT=$(subst release,,$(FLAVOR))$(_VARIANT.string)

RAMP.release:=$(shell JUST_PRINT=1 RAMP=1 DEPS=0 RELEASE=1 SNAPSHOT=0 VARIANT=$(RAMP_VARIANT) PACKAGE_NAME=$(PACKAGE_NAME) ./pack.sh)
RAMP.snapshot:=$(shell JUST_PRINT=1 RAMP=1 DEPS=0 RELEASE=0 SNAPSHOT=1 VARIANT=$(RAMP_VARIANT) PACKAGE_NAME=$(PACKAGE_NAME) ./pack.sh)

RAMP_YAML ?= ramp.yml

PACK_ARGS=\
	VARIANT=$(RAMP_VARIANT) \
	ARTDIR=$(ROOT)/artifacts \
	PACKAGE_NAME=$(PACKAGE_NAME) \
	RAMP_YAML=$(RAMP_YAML) \
	RAMP_ARGS=$(RAMP_ARGS)

artifacts/$(RAMP.release) : $(TARGET) $(RAMP_YAML)
	@echo Packing module...
	$(SHOW)$(PACK_ARGS) ./pack.sh $(TARGET)

pack: artifacts/$(RAMP.release)

#----------------------------------------------------------------------------------------------

docs:
	$(SHOW)mkdocs build

deploydocs:
	$(SHOW)mkdocs gh-deploy

.PHONY: docs deploydocs

#----------------------------------------------------------------------------------------------

MODULE_VERSION := $(shell git describe)

DOCKER_ARGS=

ifeq ($(CACHE),0)
DOCKER_ARGS += --no-cache
endif

DOCKER_IMAGE ?= redislabs/redisearch

docker:
	docker build . -t $(DOCKER_IMAGE) -f docker/Dockerfile $(DOCKER_ARGS) \
		--build-arg=GIT_DESCRIBE_VERSION=$(MODULE_VERSION)

docker_push: docker
	docker push redislabs/redisearch:latest
	docker tag redislabs/redisearch:latest redislabs/redisearch:$(MODULE_VERSION)
	docker push redislabs/redisearch:$(MODULE_VERSION)

.PHONY: docker docker_push
