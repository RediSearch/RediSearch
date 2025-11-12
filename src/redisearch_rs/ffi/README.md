# The FFI crate

This crate uses `bindgen` to generate the FFI bindings for Redisearch's C API. All Rust code should
use this to interact with Redisearch C API.

## Missing API

This crate only generates bindings for the C API that is actually used by the Rust code. If you
require additional bindings, you can add the C and header files in the [build script](./build.rs).
