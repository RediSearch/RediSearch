# Build the `index_result` module as an object library for inclusion in libredisearch_all.a
# This is required because the main CMakeLists.txt uses $<TARGET_OBJECTS:index_result>
file(GLOB SOURCES "index_result.c")
add_library(index_result OBJECT ${SOURCES})
target_include_directories(index_result PRIVATE . ..)
