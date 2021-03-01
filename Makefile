
ROOT=.
include deps/readies/mk/main

define HELP
make setup         # install prerequisited (CAUTION: THIS WILL MODIFY YOUR SYSTEM)
make fetch         # download and prepare dependant modules

make build         # compile and link
  DEBUG=1          # build for debugging (implies TEST=1)
  TEST=1           # enable unit tests
  WHY=1            # explain CMake decisions (in /tmp/cmake-why)
  CMAKE_ARGS       # extra arguments to CMake
make parsers       # build parsers code
make clean         # remove build artifacts
  ALL=1              # remove entire artifacts directory

make run           # run redis with RediSearch
  DEBUG=1            # invoke using gdb

make test          # run all tests (via ctest)
  TEST=regex
make pytest        # run python tests (tests/pytests)
  TEST=name          # e.g. TEST=test:testSearch
  GDB=1              # RLTest interactive debugging
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

COMPAT_DIR ?= build

BINROOT=$(COMPAT_DIR)
BINDIR=$(COMPAT_DIR)

SRCDIR=src

TARGET=$(BINDIR)/redisearch.so

PACKAGE_NAME ?= redisearch-oss
export PACKAGE_NAME

#----------------------------------------------------------------------------------------------

ifeq ($(DEBUG),1)
CMAKE_BUILD_TYPE=DEBUG
TEST ?= 1
else
CMAKE_BUILD_TYPE=RelWithDebInfo
endif
CMAKE_DEBUG=-DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE)

ifeq ($(TEST),1)
CMAKE_TEST=-DRS_RUN_TESTS=ON
endif

ifeq ($(WHY),1)
CMAKE_WHY=--trace-expand > /tmp/cmake-why 2>&1
endif

#----------------------------------------------------------------------------------------------

include $(MK)/defs

MK_CUSTOM_CLEAN=1

include $(MK)/rules

$(COMPAT_MODULE): $(BINROOT)/redisearch.so
	cp $^ $@

$(BINROOT)/Makefile : CMakeLists.txt
ifeq ($(WHY),1)
	@echo CMake log is in /tmp/cmake-why
endif
	@mkdir -p $(BINROOT)
	@cd $(BINROOT) && cmake .. $(CMAKE_ARGS) $(CMAKE_TEST) $(CMAKE_DEBUG) $(CMAKE_WHY)

$(COMPAT_DIR)/redisearch.so: $(COMPAT_DIR)/Makefile
	$(MAKE) -C $(BINROOT) -j$(shell nproc)
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

test:
ifneq ($(TEST),)
	@set -e; cd $(BINROOT); CTEST_OUTPUT_ON_FAILURE=1 ctest -R $(TEST)
else
	@set -e; cd $(BINROOT); ctest
endif

ifeq ($(GDB),1)
RLTEST_GDB=-i
endif

pytest:
	@set -e ;\
	if ! command -v redis-server > /dev/null; then \
		echo "Cannot find redis-server. Aborting." ;\
		exit 1 ;\
	fi
ifneq ($(TEST),)
	@cd tests/pytests; PYDEBUG=1 python -m RLTest --test $(TEST) $(RLTEST_GDB) -s --module $(abspath $(TARGET))
else
	@cd tests/pytests; python -m RLTest --module $(abspath $(TARGET))
endif

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

