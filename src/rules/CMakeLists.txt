# Build the `rules` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB RULES_SOURCES "*.c")
add_library(rules STATIC ${RULES_SOURCES})
target_include_directories(rules PRIVATE . ..)
