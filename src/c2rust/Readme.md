# Readme

This folder holds a CMakeLists that defines the bindings used by Rust for unit tests and benches.

Here we started with the initialization that uses global variables that are required for some Rust Unit tests to run:

- `RSGlobalConfig` of type `RSConfig`
- `RSDummyContext` of type `RedisModuleCtx`
