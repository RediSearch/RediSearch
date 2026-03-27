# Build the `value` module as an object library for inclusion in libredisearch_all.a
# This is required because the main CMakeLists.txt uses $<TARGET_OBJECTS:value>
file(GLOB SOURCES "value.c")
add_library(value OBJECT ${SOURCES})
target_include_directories(value PRIVATE . ..)
