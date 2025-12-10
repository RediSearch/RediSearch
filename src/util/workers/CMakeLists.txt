# Build the `workers` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB WORKERS_SOURCES "*.c")
add_library(workers STATIC ${WORKERS_SOURCES})
target_include_directories(workers PRIVATE . .. ../..)
