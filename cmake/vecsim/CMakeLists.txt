cmake_minimum_required(VERSION 3.10)
cmake_policy(SET CMP0015 NEW)

project(VectorSimilarity)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../deps)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../deps/VectorSimilarity)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions")

add_library(vector_similarity STATIC
    ../../deps/VectorSimilarity/src/algorithms/hnsw_c.cpp
    ../../deps/VectorSimilarity/src/vecsim.c)
