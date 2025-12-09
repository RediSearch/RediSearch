# Build the `doc_table` module as a standalone static library.
# This is make it easier for Rust code to call this code by linking to this library.
file(GLOB DOC_TABLE_SOURCES "*.c")
add_library(doc_table STATIC ${DOC_TABLE_SOURCES})
target_include_directories(doc_table PRIVATE . ..)
