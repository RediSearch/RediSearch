# Readme

This folder holds a CMakeLists that defines the bindings used by Rust for unit tests and benches, called `c2rust`. 

Here you can add more c files with types that need to be (temporary) accessible by Rust.

In the crate `redis_mock` we also need to define a mock of the `RedisModule` functions.

## c2rust Mocking the RedisModule for Tests

Rust tests compile a binary and this binary needs to resolve symbols. In most cases we add the right c files to CMakeLists.txt but for the RedisModule API we need to provide a mock.

In the C++ world the file `redismock.cpp` is used to define a mock of the redis module functions if those are required by unit tests. We can use that as resource.

### Why do we need this? 

We get problems in CI when Coverage is taken. 

- [Error - Original Unresolved symbosl](https://github.com/RediSearch/RediSearch/actions/runs/15755356051/job/44409489248?pr=6342#step:17:1888)
- [Error - When ignoring symbols](https://github.com/RediSearch/RediSearch/actions/runs/15761116511/job/44427857483?pr=6342#step:17:4458)

### TODOs

Use the alloc shims for the following:

- [x] RedisModule_TryAlloc
- [x] RedisModule_TryCalloc
- [x] RedisModule_TryRealloc
- [x] RedisModule_Strdup

These are doable in Rust:

- [x] RedisModule_GetApi
- [x] RedisModule_SetModuleAttribs
- [x] RedisModule_AddACLCategory
- [x] RedisModule_SetCommandACLCategories

- [x] RedisModule_ReplyWithDouble
- [x] RedisModule_ReplyWithLongLong
- [x] RedisModule_ReplyWithError
- [ ] RedisModule_ReplyWithErrorFormat
- [x] RedisModule_ReplyWithString
- [x] RedisModule_ReplyWithStringBuffer
- [x] RedisModule_ReplyWithSimpleString
- [x] RedisModule_ReplyWithArray
- [x] RedisModule_ReplyWithMap

These are better linked from `redismock.cpp` for now:

- [ ] RedisModule_CreateCommand
- [ ] RedisModule_GetCommand
- [ ] RedisModule_CreateSubcommand

These are undefined in redismock.cpp:

- [x] RedisModule_WrongArity
- [x] RedisModule_SetCommandInfo
- [x] RedisModule_IsModuleNameBusy