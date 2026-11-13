cmake_minimum_required(VERSION 3.12)
cmake_policy(SET CMP0074 NEW)
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

set(SVS_SUPPORTED 1)

# GCC < v11 does not support C++20 features required for SVS
# https://gcc.gnu.org/projects/cxx-status.html
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.0")
        set(SVS_SUPPORTED 0)
        message(STATUS "Skipping SVS: requires GCC >= 11.")
    endif()
endif()

include(CMakeDependentOption)

# USE_SVS option forcibly OFF if CPU or compiler is not supported
# elsewhere let user disable SVS
cmake_dependent_option(USE_SVS "Build with SVS library support" ON "SVS_SUPPORTED" OFF)

if(USE_SVS)
    message(STATUS "SVS support enabled")
    # Configure SVS build
    add_compile_definitions("HAVE_SVS=1")

    if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "(x86_64)|(AMD64|amd64)")
        set(SVS_LVQ_SUPPORTED 1)
    else()
        set(SVS_LVQ_SUPPORTED 0)
        message(STATUS "SVS LVQ is not supported on this architecture")
    endif()

    # detect if build environment is using glibc
    include(CheckSymbolExists)
    check_symbol_exists(__GLIBC__ "features.h" GLIBC_FOUND)
    if(GLIBC_FOUND)
        # Detect glibc version via __GLIBC__.
        file(WRITE "${CMAKE_BINARY_DIR}/detect_glibc.cpp"
            "#include <cstdio>\n#include <features.h>\nint main(){ printf(\"%d.%d\", __GLIBC__, __GLIBC_MINOR__); return 0; }\n")
        try_run(_glibc_run_result _glibc_compiled
            "${CMAKE_BINARY_DIR}" "${CMAKE_BINARY_DIR}/detect_glibc.cpp"
            RUN_OUTPUT_VARIABLE GLIBC_VERSION)
        if(NOT _glibc_compiled OR NOT _glibc_run_result EQUAL 0)
            set(GLIBC_VERSION "0")
        endif()
        message(STATUS "Detected GLIBC version: ${GLIBC_VERSION}")

        # Detect libstdc++ version via _GLIBCXX_RELEASE (GCC major version of the headers).
        file(WRITE "${CMAKE_BINARY_DIR}/detect_glibcxx.cpp"
            "#include <stdio.h>\n#include <string>\nint main(){ printf(\"%d\", _GLIBCXX_RELEASE); return 0; }\n")
        try_run(_glibcxx_run_result _glibcxx_compiled
            "${CMAKE_BINARY_DIR}" "${CMAKE_BINARY_DIR}/detect_glibcxx.cpp"
            RUN_OUTPUT_VARIABLE GLIBCXX_VERSION)
        if(NOT _glibcxx_compiled OR NOT _glibcxx_run_result EQUAL 0)
            set(GLIBCXX_VERSION 0)
        endif()
        message(STATUS "Detected GLIBCXX_RELEASE (GCC major): ${GLIBCXX_VERSION}")
    endif()

    cmake_dependent_option(SVS_SHARED_LIB "Use SVS pre-compiled shared library" ON "USE_SVS AND GLIBC_FOUND AND SVS_LVQ_SUPPORTED" OFF)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        if(GLIBC_VERSION VERSION_GREATER_EQUAL "2.31" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "21.0")
            if(GLIBCXX_VERSION VERSION_GREATER_EQUAL "11")
                set(SVS_URL "https://github.com/intel/ScalableVectorSearch/releases/download/v0.5.1/svs-shared-library-reduced-clang21-gcc11.tar.gz" CACHE STRING "SVS URL")
            else()
                message(STATUS "libstdc++ >= GCC 11 is required for Clang SVS binaries - disabling SVS_SHARED_LIB")
                set(SVS_SHARED_LIB OFF)
            endif()
        else()
            message(STATUS "GLIBC >= 2.31 and Clang >= 21.0 is required for Clang SVS binaries - disabling SVS_SHARED_LIB")
            set(SVS_SHARED_LIB OFF)
        endif()
    else()
        if(GLIBC_VERSION VERSION_GREATER_EQUAL "2.28")
            if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "14.0")
		    set(SVS_URL "https://github.com/intel/ScalableVectorSearch/releases/download/v0.5.1/svs-shared-library-reduced-gcc14.tar.gz" CACHE STRING "SVS URL")
            else()
                set(SVS_URL "https://github.com/intel/ScalableVectorSearch/releases/download/v0.5.1/svs-shared-library-reduced.tar.gz" CACHE STRING "SVS URL")
            endif()
        else()
            message(STATUS "GLIBC >= 2.28 is required for GCC SVS binaries - disabling SVS_SHARED_LIB")
            set(SVS_SHARED_LIB OFF)
        endif()
    endif()

    if(SVS_SHARED_LIB)
        include(FetchContent)
        FetchContent_Declare(
            svs
            URL "${SVS_URL}"
        )
        FetchContent_MakeAvailable(svs)
        list(APPEND CMAKE_PREFIX_PATH "${svs_SOURCE_DIR}")
        find_package(svs REQUIRED)
        set(SVS_LVQ_HEADER "svs/extensions/vamana/lvq.h")
        set(SVS_LEANVEC_HEADER "svs/extensions/vamana/leanvec.h")
    else()
        # This file is included from both CMakeLists.txt and python_bindings/CMakeLists.txt
        # Set `root` relative to this file, regardless of where it is included from.
        get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/.. ABSOLUTE)
        set(SVS_EXPERIMENTAL_CHECK_BOUNDS OFF CACHE BOOL "Disable SVS bounds checking" FORCE)
        add_subdirectory(
            ${root}/deps/ScalableVectorSearch
            deps/ScalableVectorSearch
        )
        set(SVS_LVQ_HEADER "svs/quantization/lvq/impl/lvq_impl.h")
        set(SVS_LEANVEC_HEADER "svs/leanvec/impl/leanvec_impl.h")
    endif()

    if(SVS_LVQ_SUPPORTED AND EXISTS "${svs_SOURCE_DIR}/include/${SVS_LVQ_HEADER}")
        message("SVS LVQ implementation found")
        add_compile_definitions(VectorSimilarity
            PUBLIC "HAVE_SVS_LVQ=1"
            PUBLIC "SVS_LVQ_HEADER=\"${SVS_LVQ_HEADER}\""
            PUBLIC "SVS_LEANVEC_HEADER=\"${SVS_LEANVEC_HEADER}\""
        )
    else()
        message("SVS LVQ implementation not found")
        add_compile_definitions(VectorSimilarity PUBLIC "HAVE_SVS_LVQ=0")
    endif()

    # `replace_external_id` is newer than the pre-built SVS releases that SVS_SHARED_LIB downloads,
    # so whether it is available depends on which SVS this build ended up with rather than on the
    # platform. Detect it in the header instead of assuming: it is a method, not a file, so this
    # greps the header the LVQ check would have tested for existence.
    set(SVS_DYNAMIC_INDEX_HEADER "svs/concurrent/dynamic_index.h")
    set(SVS_HAS_REPLACE_EXTERNAL_ID 0)
    if(EXISTS "${svs_SOURCE_DIR}/include/${SVS_DYNAMIC_INDEX_HEADER}")
        file(READ "${svs_SOURCE_DIR}/include/${SVS_DYNAMIC_INDEX_HEADER}" SVS_DYNAMIC_INDEX_SRC)
        string(FIND "${SVS_DYNAMIC_INDEX_SRC}" "replace_external_id" SVS_REPLACE_EXTERNAL_ID_POS)
        if(NOT SVS_REPLACE_EXTERNAL_ID_POS EQUAL -1)
            set(SVS_HAS_REPLACE_EXTERNAL_ID 1)
        endif()
        unset(SVS_DYNAMIC_INDEX_SRC)
    endif()

    if(SVS_HAS_REPLACE_EXTERNAL_ID)
        message("SVS replace_external_id found - SVS relabeling enabled")
        add_compile_definitions(VectorSimilarity PUBLIC "HAVE_SVS_REPLACE_EXTERNAL_ID=1")
    else()
        message("SVS replace_external_id not found - SVS relabeling reports unsupported")
        add_compile_definitions(VectorSimilarity PUBLIC "HAVE_SVS_REPLACE_EXTERNAL_ID=0")
    endif()
else()
    message(STATUS "SVS support disabled")
    add_compile_definitions("HAVE_SVS=0")
endif()
