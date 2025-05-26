message(STATUS "BOOST_DIR: ${BOOST_DIR}")

if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

if (IS_DIRECTORY ${BOOST_DIR})
    message(STATUS "BOOST_DIR points to a valid directory: ${BOOST_DIR}")
else()
    message(STATUS "BOOST_DIR '${BOOST_DIR}' is not a valid boost directory path")
    set(BOOST_ENABLE_CMAKE ON)

    message(STATUS "fetching boost")

    include(FetchContent)
    # we want to see the progress of the download, so we set FETCHCONTENT_QUIET to false
    Set(FETCHCONTENT_QUIET FALSE)
    FetchContent_Declare(
            Boost
            URL https://archives.boost.io/release/1.84.0/source/boost_1_84_0.tar.gz
            USES_TERMINAL_DOWNLOAD TRUE
            DOWNLOAD_NO_EXTRACT FALSE
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_MakeAvailable(Boost)

    message(STATUS "boost fetched")
    message(STATUS "boost source dir: ${boost_SOURCE_DIR}")
    message(STATUS "boost binary dir: ${boost_BINARY_DIR}")
    set(BOOST_DIR ${boost_SOURCE_DIR})
endif()
