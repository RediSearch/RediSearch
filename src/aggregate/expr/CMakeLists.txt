# Build the `aggregate_expr` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB EXPR_SOURCES "*.c")
add_library(aggregate_expr STATIC ${EXPR_SOURCES})
target_include_directories(aggregate_expr PRIVATE . ../..)
