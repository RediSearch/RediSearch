# Build the `language` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB LANGUAGE_SOURCES "*.c")
add_library(language STATIC ${LANGUAGE_SOURCES})
target_include_directories(language PRIVATE . ..)
