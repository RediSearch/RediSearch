# Readme

The module initialization uses global variables that are required for some Rust Unit tests to run:

- `RSGlobalConfig` of type `RSConfig`
- `RSDummyContext` of type `RedisModuleCtx`

We're working with Mocks here, but in the future other Rust unit tests or  integration tests may require a proper initialization of those variables.
