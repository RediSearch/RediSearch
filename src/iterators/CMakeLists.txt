# Build the `iterators` module as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the iterators against the original C implementation.
file(GLOB ITERATORS_SOURCES "*.c")
add_library(iterators STATIC ${ITERATORS_SOURCES})
target_include_directories(iterators PRIVATE . ..)
