# Developer notes

## Cloning the project

```bash
git clone --recursive https://github.com/RediSearch/RediSearch.git
```

## Installing prerequisites

### Build dependencies
To build and test RediSearch you need to install several packages, depending on the underlying OS. The following OSes are supported and tested in our CI:

* Ubuntu 18.04
* Ubuntu 20.04
* Ubuntu 22.04
* Ubuntu 24.04
* Debian linux 11
* Debian linux 12
* Rocky linux 8
* Rocky linux 9
* Amazon linux 2
* Amazon linux 2023
* Mariner 2.0
* Azure linux 3
* MacOS
* Alpine linux 3

For installing the prerequisites you can take the following approaches:
1. Install the dependencies manually - check our install script at ".install/install_script.sh" for the list of dependencies. In general, the common installations are:
   - `rust` (latest stable version)
   - `cmake >= 3.25.1`
   - `boost == 1.84.0` you can skip this dependency so our CMake script will have it for you, but this has build time penalty.
   - `build-essential` (on Debian/Ubuntu) or equivalent build tools on other systems
   - `python3` and `python3-pip` (for running tests)
   - `openssl-devel` / `libssl-dev` (for secure connections)

   - Ubuntu 18.04: GCC 10 (not default, installed via PPA)
   - Ubuntu 20.04: GCC 12 (not default, installed via PPA)
   - Ubuntu 22.04: Default GCC is sufficient
   - Ubuntu 24.04: Default GCC is sufficient
   - Debian 11: Default GCC is sufficient
   - Debian 12: Default GCC is sufficient
   - Rocky Linux 8: GCC 13 (not default, installed via devtoolset-13)
   - Rocky Linux 9: GCC 13 (not default, installed via devtoolset-13)
   - Amazon Linux 2: GCC 11 (not default, installed via Amazon's SCL)
   - Amazon Linux 2023: Default GCC is sufficient
   - Mariner 2.0: Default GCC is sufficient
   - MacOS: Install clang-18 via brew
   - Alpine Linux 3: Default GCC is sufficient

- you can find the dependencies in each OS script under the [`.install`](.install) directory.
 
2. Use our CI installation scripts to install the dependencies - you can find the scripts under the [`.install`](.install) directory.

    ```bash
    cd .install
    ./install_script.sh sudo  
    ```
    Note that this will install various packages on your system using the native package manager (sudo is not required in a Docker environment and the script will fail calling it so). 

### Redis
You will not be able to run and test your code without Redis, since you need to load the module. You can build it from source and install it as described in [redis GitHub page](https://github.com/redis/redis).

### RedisJSON
Some of our behavioral tests require RedisJSON to be present. Our testing framework will clone and build RedisJSON for you.



## Building from source
To build RediSearch from source, you need to run the following commands:

```bash
make build
```

### Running Redis with RediSearch
To run Redis with RediSearch, you need to load the module. You can do this by running the following command:

```bash
make run
```

## Testing

### Running the tests
To run the tests, you need to execute the following commands:

```bash
make test
```
For running specific tests you can use the following commands:
* C tests, located in tests/ctests, run by `make c-tests`.
* C++ tests (enabled by GTest), located in tests/cpptests, run by `make cpp-tests`.
* Python behavioral tests (enabled by [RLTest](https://github.com/RedisLabsModules/RLTest)), located in tests/pytests, run by `make pytest`.

### Test prerequisites
To run the python behavioral tests you need to install the following dependencies:
* python test requirements listed in [requirements.txt](tests/pytests/requirements.txt)
* If you want to execute RediSearch+RedisJSON behavioral tests you need to install [rust](https://www.rust-lang.org/tools/install), which is required to compile the RedisJSON module 

if you don't want to install those manually, you can use the CI installation scripts to install the dependencies.

```bash
.install/test_deps/common_installations.sh
```
Note those scripts will create a python virtual environment called `venv` and install the required dependencies in it.

### RedisJSON
Some of our behavioral tests require RedisJSON to be present. You can skip the RedisJSON tests by setting the `REJSON=0` in the command
```bash
make pytest REJSON=0
```
If you have RedisJSON module already built on your machine you can specify the path to it by setting the `REJSON_PATH` variable.
```bash
make pytest REJSON_PATH=/path/to/redisjson.so
```
If the module does not exist in the specified path, our testing framework will clone and build RedisJSON for you. You can specify the branch of RedisJSON you want to use by setting the `REJSON_BRANCH` variable.

```bash
make pytest REJSON_BRANCH=branch
```

### Sanitizers
Currently address sanitizer is supported. To run the tests with address sanitizer you can use the following command:

```bash
make build test SAN=address
```

## Debug
To build the code with debug symbols, you can use the following commands:

```bash
make DEBUG=1
```