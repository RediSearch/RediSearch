# Build the `spec` module as a standalone static library.
# This is make it easier for Rust code to call this code by linking to this library.
file(GLOB SPEC_SOURCES "*.c")
add_library(spec STATIC ${SPEC_SOURCES})
target_include_directories(spec PRIVATE . ..)
