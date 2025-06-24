# Readme

This folder holds a CMakeLists that defines `c2rust` bindings used for Rust unit tests and micro benchmarks. 
`c2rust` is a static library that will be linked to the crate `redis_mock`. This crate combines
the static c-symbols from `c2rust` with mock function for the [Redis Module API](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/) 
and is compiled as a dynamic library to be linked against Rust unit test and benchmark binaries.

For C++ tests the file [`redismock.cpp`](../../tests/cpptests/redismock/redismock.cpp) is used to define a mock of the redis module functions.

## Why do we need this

The build command `./build.sh FORCE DEBUG COV=1 RUN_RUST_TESTS` ends with unresolved symbols. The reason is not 100% clear, but we use `cargo +nightly llvm-cov` in this case.

This problem only arises on Linux systems.

## Unresolved Symbols

- __gcov_init
- __gcov_exit
- __gcov_merge_add
- RedisModule_ReplyWithVerbatimStringType
- RedisModule_ReplyWithBool
- RedisModule_ReplyWithCallReply
- RedisModule_ReplyWithBigNumber
- RedisModule_ReplyWithLongDouble
- RedisModule_SelectDb
- RedisModule_GetSelectedDb
- RedisModule_KeyExists
- RedisModule_OpenKey
- RedisModule_GetOpenKeyModesAll
- RedisModule_CloseKey
- RedisModule_KeyType
- RedisModule_ValueLength
- RedisModule_ListInsert
- RedisModule_ListDelete
- RedisModule_ListPush
- RedisModule_ListPop
- RedisModule_ListGet
- RedisModule_ListSet
- RedisModule_StringToLongLong
- RedisModule_StringToULongLong
