---
name: port-c-module
description: Guide for porting a C module to Rust
disable-model-invocation: true
---

# Port Module Skill

Guide for porting a C module to Rust.

## Arguments
The module name to port should be provided as an argument (e.g., `/port-module triemap`).

Module to port: `$ARGUMENTS`

## Usage
Use this skill when starting to port a C module to Rust.

## Instructions

### 1. Analyze the C Code
First, understand the C module you're porting (look for `$ARGUMENTS.c` and `$ARGUMENTS.h` in `src/`):
- Read the `.c` and `.h` files in `src/`
- Identify what is exposed by the header file:
  - Does the rest of the codebase have access to the inner fields of the data structures defined in this module?
  - Are types always passed by value or by reference? A mix?
  - Can the corresponding Rust types be passed over the FFI boundary?
- Understand data structures and their lifetimes
- Identify which types and functions this module imports from other modules:
  - Determine if those types are implemented in Rust or C
  - If those types are implemented in Rust, identify the relevant Rust crate
  - If those types are implemented in C, understand if it makes sense to port them first to Rust
    or if it's preferable to invoke the C implementation from Rust via FFI
- Note any global state or Redis module interactions
- Identify which tests under `tests/` are relevant to this module

### 2. Define A Porting Plan

Create a `$ARGUMENTS_plan.md` file to outline the steps and decisions for porting the module.
Determine if the C code should be modified, at this stage, to ease the porting process.
For example:
- Introduce getters and setters to avoid exposing inner fields of data structures defined in this module.
- Split the module into smaller, more manageable parts.

### 3. Create the Rust Crate
```bash
cd src/redisearch_rs
cargo new $ARGUMENTS --lib
```

### 4. Implement Pure Rust Logic
- Create idiomatic Rust code
- Add comprehensive tests
  - Ensure that all C/C++ tests have equivalent Rust tests
- Document public APIs with doc comments
- For performance sensitive code, create microbenchmarks using `criterion`
- Use `proptest` for property-based testing where appropriate
- Testing code should be written with the same care reserved to production code

### 5. Compare Rust API with C API
- Review the public API of the new Rust module against the C API in the header file
- Ensure that differences can be bridged by adding appropriate wrappers or adapters
- Go back to step 1 if discovered differences cannot be bridged without a re-design

### 6. Create FFI Wrapper
Create an FFI crate to expose the new Rust module to the C codebase:
```bash
cd src/redisearch_rs/c_entrypoint
cargo new ${ARGUMENTS}_ffi --lib
```

FFI crate should:
- Expose `#[unsafe(no_mangle)] pub extern "C" fn` functions
- Handle null pointers and error cases
- Convert between C and Rust types safely
- Document all unsafe blocks with `// SAFETY:` comments

### 7. Wire Up C Code
- Delete the C header file and its implementation
- Update the rest of the C codebase to import the new Rust header wherever the old C header was used

C header files for Rust FFI crates are auto-generated. No need to use their full path in imports,
use just their name (e.g. `#include $ARGUMENTS.h;` for `${ARGUMENTS}_ffi`)

### 8. Test The Integration
```bash
./build.sh RUN_UNIT_TESTS               # C/C++ unit tests
./build.sh RUN_PYTEST                   # Integration tests
```

## Example: Well-Ported Module
See `src/redisearch_rs/trie_rs/` for a high-quality example:
- Pure Rust implementation with comprehensive docs
- Extensive test coverage
- Clean FFI boundary in `c_entrypoint/trie_ffi/`
