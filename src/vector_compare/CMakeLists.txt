# The comparison has to be C++: VecSim exposes stored vectors through `getDataByLabel`, a
# template method no C caller can name. Kept in its own library, like src/geometry, so it
# inherits VecSim's include paths and C++ standard instead of imposing them on rscore -- which
# is otherwise a pure-C target.
set(CMAKE_CXX_STANDARD 20)

add_library(redisearch-vector-compare STATIC vector_compare.cpp)
target_link_libraries(redisearch-vector-compare PUBLIC VectorSimilarity)
