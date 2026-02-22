# Onboarding Notes

This document contains supplementary information for new developers.

For build and test instructions, see [developer.md](https://github.com/RediSearch/RediSearch/blob/master/developer.md).

For Rust specific guidelines, see [CONTRIBUTING.md](https://github.com/RediSearch/RediSearch/blob/master/src/redisearch_rs/CONTRIBUTING.md).

## TODOs

- [] Get access to Okta/Jira/Confluence.
- [] Get access to GitHub repository as a contributor with push access (now goes through Okta).

## Useful Links

- [Redis Modules API Reference](https://redis.io/docs/latest/develop/reference/modules/)
- [redismodule-rs](https://github.com/RedisLabsModules/redismodule-rs/tree/master) - Rust bindings for Redis modules
- Key RediSearch commands:
  - [FT.SEARCH](https://redis.io/docs/latest/commands/ft.search/)
  - [FT.AGGREGATE](https://redis.io/docs/latest/commands/ft.aggregate/)
  - [FT.CREATE](https://redis.io/docs/latest/commands/ft.create/)

## Additional info

### Migration Plan

When porting modules from C to Rust, prioritize modules with many dependents but few dependencies. Two types of modules stand out:

1. **Data structures** - Self-contained, easy to test, straightforward usage patterns
2. **Utility classes**

Data structures are the preferred starting point because:
- They won't necessarily surface or reduce memory safety bugs, but they won't introduce new ones either
- They're used in pretty much every code path, so we avoid having to write C FFI layers over existing C data structures later

### Redis Global Lock

Redis is fundamentally single-threaded.

All operations that touch its data structures must happen while holding the Redis global lock, which you may think of as a close sibling of Python’s Global Interpreter Lock.

- 💡 We should use `pyo3`'s modelling as a reference when reviewing the way the lock is modelled in `redis_module_rs`. It’s currently relatively crude.
- ⚠️ `redis_module_rs` is actively used in `RedisJSON`, so any breaking change will require a corresponding PR to upgrade the codebase to the newer version. Meir prefers to stick to backwards-compatible changes when possible.

### Active defragmentation

It’s a process Redis uses to optimise the packing of its existing memory allocations, thus reducing overall memory usage.

Key API calls: https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#section-defrag-api

For modules, it plays out as follows:

- Redis kicks off active defrag
- Redis tells the module: do you have something to defrag?
- The module may try to defrag its own data structures (or not)
- The process may be paused multiple times to avoid blocking

**After the data buffer behind a pointer has been defragmented, the old pointer MUST NOT be used anymore. It’s invalid.**

Active defragmentation holds the Redis global lock.

- ⚠️ RediSearch has a background thread pool running concurrently with whatever holds the Redis global lock then.
We’ll have to acquire secondary locks over those indexes to defrag its data structures and ensure that nothing else is using them, including from those background threads.
- ‼️ RediSearch doesn’t currently implement active defragmentation. It’s implemented, today, in RedisJSON, which we can use as a case study, although it’s a muuuuch simpler setup.
- Defragmenting a data structure requires accessing its raw heap allocations. This may or may not be possible for data structures coming from existing Rust crates. When that requirement is not satisfied, we will have to fork.
E.g. we can decompose `Vec` into raw parts, defrag, and then recompose. We can’t do it for `HashSet` or `HashMap`.
- 🗺️ I managed to get Meir to agree on a few key points:
    - We should keep active defrag in mind but not try to port code *and* introduce active defrag simultaneously.
    - We shouldn’t pay the cost of forking data structures up front. We can fork later when implementing active defrag.
    - Implementing active defrag in a mixed C/Rust codebase will be a nightmare because tracking whether a pointer is actively being used will be tricky. We should wait for the Rust migration to be well underway before starting this endeavor.
