# Build the c2rust bindings module used a standalone static library
# This is invoked by the root CMakeLists.txt and therefore inherits the configuration
# Different from other CMakeLists invoked via add_subdirectory, 
# this library is not linked to the redisearch target of the root CMakeLists.txt
add_library(c2rust STATIC)

# List the source files that are part of the c2rust bindings here:
target_sources(c2rust PRIVATE 
    "../module-init/module-init.c" 
    "../config.c"
    "../value.c"
    "../util/mempool.c"
    "variadic_helper.c"
)
target_include_directories(c2rust PRIVATE . ..)