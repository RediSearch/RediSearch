# Build the `obfuscation` module as a standalone static library.
# This is make it easier for Rust code to call this code by linking to this library.
file(GLOB OBFUSCATION_SOURCES "*.c")
add_library(obfuscation STATIC ${OBFUSCATION_SOURCES})
target_include_directories(obfuscation PRIVATE . ..)
