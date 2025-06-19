# Build the cbindings module used by Rust as a standalone static library
# Here initialization means the setup of the global variables for config and context in Redis
add_library(c2rust STATIC)

# won't work: https://stackoverflow.com/questions/62772356/add-subdirectory-twice-with-different-properties
#add_subdirectory(../../tests/cpptests/redismock)

# For the target sources we use a folder `c2rust` such that the main cmake file does not 
# compile duplicate symbols by its glob * usage.
target_sources(c2rust PRIVATE 
    "module-init.c" 
    "../c2rust/config.c"
    "../c2rust/value.c"
    "../util/c2rust/mempool.c"
    "../../tests/cpptests/redismock/redismock.cpp"
    "../../tests/cpptests/redismock/util.cpp")
target_compile_definitions(c2rust PRIVATE RUSTMOCK)
target_include_directories(c2rust PRIVATE . .. "../../tests/cpptests/redismock")
