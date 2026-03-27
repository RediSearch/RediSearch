# Build the `buffer` module as an object library for inclusion in libredisearch_all.a
# This is required because the main CMakeLists.txt uses $<TARGET_OBJECTS:buffer>
file(GLOB BUFFER_SOURCES "buffer.c")
add_library(buffer OBJECT ${BUFFER_SOURCES})
target_include_directories(buffer PRIVATE . ..)
