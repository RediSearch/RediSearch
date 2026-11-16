cmake_minimum_required(VERSION 3.25)
project(VecSim LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)

# build bindings with -DBUILD_TESTS flag
set(VECSIM_BUILD_TESTS OFF CACHE BOOL "Build tests" FORCE)
set(VECSIM_BUILD_TEST_HELPERS ON CACHE BOOL "Build test helpers" FORCE)
ADD_DEFINITIONS(-DBUILD_TESTS)
get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/.. ABSOLUTE)

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

add_subdirectory(${root} VectorSimilarity)

include_directories(${root}/.. ${root}/../../tests/cpptests/vecsim/utils)

pybind11_add_module(VecSim ${root}/../../tests/cpptests/vecsim/utils/mock_thread_pool.cpp bindings.cpp)

target_link_libraries(VecSim PRIVATE VectorSimilarity)
get_directory_property(VECSIM_COMPILE_DEFINITIONS DIRECTORY ${root} COMPILE_DEFINITIONS)
target_compile_definitions(VecSim PRIVATE ${VECSIM_COMPILE_DEFINITIONS})

add_dependencies(VecSim VectorSimilarity)
