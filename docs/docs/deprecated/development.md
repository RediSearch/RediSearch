---
title: "Developer notes"
linkTitle: "Developer notes"
weight: 3
description: Notes on debugging, testing and documentation
aliases: 
    - /docs/stack/search/development/
---

# Developing Search and Query

Developing Search and Query involves setting up the development environment (which can be either Linux-based or macOS-based), building the module, running tests and benchmarks, and debugging both the module and its tests.

## Cloning the git repository
By invoking the following command, the Search and Query module and its submodules are cloned:
```sh
git clone --recursive https://github.com/RediSearch/RediSearch.git
```
## Working in an isolated environment
There are several reasons to develop in an isolated environment, like keeping your workstation clean, and developing for a different Linux distribution.
The most general option for an isolated environment is a virtual machine (it's very easy to set one up using [Vagrant](https://www.vagrantup.com)).
Docker is even a more agile, as it offers an almost instant solution:

```
search=$(docker run -d -it -v $PWD:/build debian:bullseye bash)
docker exec -it $search bash
```
Then, from within the container, ```cd /build``` and go on as usual.

In this mode, all installations remain in the scope of the Docker container.
Upon exiting the container, you can either re-invoke it with the above ```docker exec``` or commit the state of the container to an image and re-invoke it on a later stage:

```
docker commit $search redisearch1
docker stop $search
search=$(docker run -d -it -v $PWD:/build rediseatch1 bash)
docker exec -it $search bash
```

You can replace `debian:bullseye` with your OS of choice, with the host OS being the best choice (so you can run the Search and Query binary on your host once it is built).

## Installing prerequisites

To build and test Search and Query one needs to install several packages, depending on the underlying OS. Currently, we support Ubuntu/Debian, CentOS, Fedora, and macOS.

First, enter the `RediSearch` directory.
Execute:

```
./sbin/setup
```
Followed by:

```
bash -l
```
Note that this **will install various packages on your system** using the native package manager and pip. It will invoke `sudo` on its own, prompting for permission.

If you prefer to avoid that, you can:

* Review `sbin/system-setup.py` and install packages manually,
* Use `./sbin/system-setup.py --nop` to display installation commands without executing them,
* Use an isolated environment like explained above,
* Use a Python virtual environment, as Python installations are known to be sensitive when not used in isolation: `python3 -m virtualenv venv; . ./venv/bin/activate`

## Installing Redis
As a rule of thumb, you're better off running the latest Redis version.

If your OS has a Redis 6.x or 7.x package, you can install it using the OS package manager.

Otherwise, you can invoke ```./deps/readies/bin/getredis```.

## Getting help
```make help``` provides a quick summary of the development features:

```
make setup         # install prerequisited (CAUTION: THIS WILL MODIFY YOUR SYSTEM)
make fetch         # download and prepare dependant modules

make build          # compile and link
  COORD=1|oss|rlec    # build coordinator (1|oss: Open Source, rlec: Enterprise)
  STATIC=1            # build as static lib
  LITE=1              # build RediSearchLight
  DEBUG=1             # build for debugging
  NO_TESTS=1          # disable unit tests
  WHY=1               # explain CMake decisions (in /tmp/cmake-why)
  FORCE=1             # Force CMake rerun (default)
  CMAKE_ARGS=...      # extra arguments to CMake
  VG=1                # build for Valgrind
  SAN=type            # build with LLVM sanitizer (type=address|memory|leak|thread) 
  SLOW=1              # do not parallelize build (for diagnostics)
  GCC=1               # build with GCC (default unless Sanitizer)
  CLANG=1             # build with CLang
  STATIC_LIBSTDCXX=0  # link libstdc++ dynamically (default: 1)
make parsers       # build parsers code
make clean         # remove build artifacts
  ALL=1              # remove entire artifacts directory

make run           # run redis with Search and Query
  GDB=1              # invoke using gdb

make test          # run all tests
  COORD=1|oss|rlec   # test coordinator (1|oss: Open Source, rlec: Enterprise)
  TEST=name          # run specified test
make pytest        # run python tests (tests/pytests)
  COORD=1|oss|rlec   # test coordinator (1|oss: Open Source, rlec: Enterprise)
  TEST=name          # e.g. TEST=test:testSearch
  RLTEST_ARGS=...    # pass args to RLTest
  REJSON=1|0|get     # also load JSON module (default: 1)
  REJSON_PATH=path   # use JSON module at `path`
  EXT=1              # External (existing) environment
  GDB=1              # RLTest interactive debugging
  VG=1               # use Valgrind
  VG_LEAKS=0         # do not search leaks with Valgrind
  SAN=type           # use LLVM sanitizer (type=address|memory|leak|thread) 
  ONLY_STABLE=1      # skip unstable tests
make unit-tests    # run unit tests (C and C++)
  TEST=name          # e.g. TEST=FGCTest.testRemoveLastBlock
make c_tests       # run C tests (from tests/ctests)
make cpp_tests     # run C++ tests (from tests/cpptests)
make vecsim-bench  # run VecSim micro-benchmark

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
```

## Building from source
```make build``` will build Search and Query.

`make build COORD=oss` will build OSS Search and Query Coordinator.

`make build STATIC=1` will build as a static lib

Notes:

* Binary files are placed under `bin`, according to platform and build variant.

* Search and Query uses [CMake](https://cmake.org) as its build system. ```make build``` will invoke both CMake and the subsequent make command that's required to complete the build.


Use ```make clean``` to remove built artifacts. ```make clean ALL=1``` will remove the entire bin subdirectory.

### Diagnosing build process
`make build` will build in parallel by default.

For purposes of build diagnosis, `make build SLOW=1 VERBOSE=1` can be used to examine compilation commands.

## Running Redis with Search and Query
The following will run ```redis``` and load the Search and Query module.
```
make run
```
You can open ```redis-cli``` in another terminal to interact with it.

## Running tests
There are several sets of unit tests:
* C tests, located in ```tests/ctests```, run by ```make c_tests```.
* C++ tests (enabled by GTest), located in ```tests/cpptests```, run by ```make cpp_tests```.
* Python tests (enabled by RLTest), located in ```tests/pytests```, run by ```make pytest```.

One can run all tests by invoking ```make test```.
A single test can be run using the ```TEST``` parameter, e.g. ```make test TEST=regex```.

## Debugging
To build for debugging (enabling symbolic information and disabling optimization), run ```make DEBUG=1```.
One can the use ```make run DEBUG=1``` to invoke ```gdb```.
In addition to the usual way to set breakpoints in ```gdb```, it is possible to use the ```BB``` macro to set a breakpoint inside the Search and Query code. It will only have an effect when running under ```gdb```.

Similarly, Python tests in a single-test mode, one can set a breakpoint by using the ```BB()``` function inside a test.
