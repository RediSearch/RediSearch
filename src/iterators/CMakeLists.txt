# Build the `iterators` module as an object library for inclusion in libredisearch_all.a
# This is required because the main CMakeLists.txt uses $<TARGET_OBJECTS:iterators>
file(GLOB ITERATORS_SOURCES "*.c")
add_library(iterators OBJECT ${ITERATORS_SOURCES})
target_include_directories(iterators PRIVATE . ..)
