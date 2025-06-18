# Build the cbindings module used by Rust as a standalone static library
# Here initialization means the setup of the global variables for config and context in Redis
add_library(rust_binded STATIC)

# For the target sources we use a folder `rust-binded` such that the main cmake file does not 
# compile duplicate symbols by its glob * usage.
target_sources(rust_binded PRIVATE 
    "module-init.c" 
    "../rust-binded/config.c"
    "../rust-binded/value.c"
    "../util/rust-binded/mempool.c")
target_include_directories(rust_binded PRIVATE . ..)
