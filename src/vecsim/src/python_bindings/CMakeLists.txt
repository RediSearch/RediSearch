cmake_minimum_required(VERSION 3.25)
project(VecSim LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)

# build bindings with -DBUILD_TESTS flag
option(VECSIM_BUILD_TESTS "Build tests" ON)
ADD_DEFINITIONS(-DBUILD_TESTS)
get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../.. ABSOLUTE)

include(FetchContent)
FetchContent_Declare(
    pybind11
    GIT_REPOSITORY https://github.com/pybind/pybind11
    GIT_TAG v2.10.1
)
FetchContent_GetProperties(pybind11)

if(NOT pybind11_POPULATED)
    FetchContent_Populate(pybind11)
    add_subdirectory(${pybind11_SOURCE_DIR} ${pybind11_BINARY_DIR})
endif()

include(${root}/cmake/svs.cmake)
add_subdirectory(${root}/src/VecSim VectorSimilarity)

include_directories(${root}/src ${root}/tests/utils)

pybind11_add_module(VecSim ../../tests/utils/mock_thread_pool.cpp bindings.cpp)

target_link_libraries(VecSim PRIVATE VectorSimilarity)

add_dependencies(VecSim VectorSimilarity)
