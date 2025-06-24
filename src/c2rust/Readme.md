# Readme

This folder holds a CMakeLists that defines `c2rust` bindings used for Rust unit tests and micro benchmarks. 
`c2rust` is a static library that will be linked to the crate `redis_mock`. This crate combines
the static c-symbols from `c2rust` with mock function for the [Redis Module API](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/) 
and is compiled as a dynamic library to be linked against Rust unit test and benchmark binaries.

For C++ tests the file [`redismock.cpp`](../../tests/cpptests/redismock/redismock.cpp) is used to define a mock of the redis module functions.

