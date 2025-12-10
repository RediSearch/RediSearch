# Build the `references` module as a standalone static library.
# This is make it easier for Rust code to call this code by linking to this library.
file(GLOB REFERENCES_SOURCES "*.c")
add_library(references STATIC ${REFERENCES_SOURCES})
target_include_directories(references PRIVATE . ../..)
