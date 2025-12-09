# Build the `dict` module as a standalone static library
# This is make it easier for Rust code to call this code by linking to this library.
file(GLOB DICT_SOURCES "dict.c")
add_library(dict STATIC ${DICT_SOURCES})
target_include_directories(dict PRIVATE . ../..)
