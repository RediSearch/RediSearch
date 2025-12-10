# Build the `global_stats` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB GLOBAL_STATS_SOURCES "*.c")
add_library(global_stats STATIC ${GLOBAL_STATS_SOURCES})
target_include_directories(global_stats PRIVATE . ../..)
