# Build the `aggregate_functions` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB FUNCTIONS_SOURCES "*.c")
add_library(aggregate_functions STATIC ${FUNCTIONS_SOURCES})
target_include_directories(aggregate_functions PRIVATE . ../..)
