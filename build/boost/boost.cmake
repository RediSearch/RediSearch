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
            URL https://github.com/boostorg/boost/releases/download/boost-1.84.0/boost-1.84.0.tar.gz
            USES_TERMINAL_DOWNLOAD TRUE
            DOWNLOAD_NO_EXTRACT FALSE
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_MakeAvailable(Boost)

    message(STATUS "boost fetched")
    message(STATUS "boost source dir: ${boost_SOURCE_DIR}")
    message(STATUS "boost binary dir: ${boost_BINARY_DIR}")

    # Build Boost headers if they don't exist
    if(NOT EXISTS "${boost_SOURCE_DIR}/boost")
        message(STATUS "Building Boost headers...")
        execute_process(
            COMMAND ./bootstrap.sh --with-libraries=headers
            WORKING_DIRECTORY ${boost_SOURCE_DIR}
            RESULT_VARIABLE bootstrap_result
        )
        if(NOT bootstrap_result EQUAL 0)
            message(FATAL_ERROR "Boost bootstrap failed")
        endif()

        execute_process(
            COMMAND ./b2 headers
            WORKING_DIRECTORY ${boost_SOURCE_DIR}
            RESULT_VARIABLE b2_result
        )
        if(NOT b2_result EQUAL 0)
            message(FATAL_ERROR "Boost header generation failed")
        endif()
        message(STATUS "Boost headers built successfully")
    endif()

    set(BOOST_DIR ${boost_SOURCE_DIR})
endif()
