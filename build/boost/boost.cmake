message(STATUS "BOOST_DIR: ${BOOST_DIR}")

if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

if(NOT BOOST_DIR STREQUAL "" AND EXISTS ${BOOST_DIR})
    message(STATUS "BOOST_DIR is not empty: ${BOOST_DIR}")
    include_directories(${BOOST_DIR})
else()
    message(STATUS "BOOST_DIR is not defined or empty")
    # set(BOOST_INCLUDE_LIBRARIES boost geometry optional unordered)
    set(BOOST_ENABLE_CMAKE ON)

    message(STATUS "fetching boost")

    include(FetchContent)
    FetchContent_Declare(
            Boost
            GIT_REPOSITORY https://github.com/boostorg/boost.git
            GIT_TAG boost-1.84.0
            GIT_PROGRESS TRUE
            GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(Boost)

    message(STATUS "boost fetched")
    message(STATUS "boost source dir: ${boost_SOURCE_DIR}")
    message(STATUS "boost binary dir: ${boost_BINARY_DIR}")
endif()
