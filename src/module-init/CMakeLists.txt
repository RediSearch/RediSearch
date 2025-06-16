# Build the initialization module as a standalone static library
# Here initialization means the setup of the global variables for config and context in Redis
add_library(module_init STATIC)
target_sources(module_init PRIVATE "module-init.c" "config.c")
target_include_directories(module_init PRIVATE . ..)
