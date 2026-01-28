# query_term is now implemented in Rust (src/redisearch_rs/c_entrypoint/query_term_ffi/)
# This is an INTERFACE library that only provides the header
add_library(query_term INTERFACE)
target_include_directories(query_term INTERFACE ${CMAKE_SOURCE_DIR}/src)
