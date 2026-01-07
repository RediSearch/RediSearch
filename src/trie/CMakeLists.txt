# Build the `trie` module as a standalone static library.
# This is make it easier for Rust code to call this code by linking to this library.
file(GLOB TRIE_SOURCES "*.c")
add_library(trie STATIC ${TRIE_SOURCES})
target_include_directories(trie PRIVATE . ..)
