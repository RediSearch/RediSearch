INCLUDE(CheckCCompilerFlag)

# This file exposes the following:
# RS_C_FLAGS
# RS_CXX_FLAGS
# RS_COMMON_FLAGS
# RS_DEFINES variables

CHECK_C_COMPILER_FLAG("-Wincompatible-pointer-types" HAVE_W_INCOMPATIBLE_POINTER_TYPES)
CHECK_C_COMPILER_FLAG("-Wincompatible-pointer-types-discards-qualifiers" HAVE_W_DISCARDS_QUALIFIERS)

set(RS_COMMON_FLAGS "-Wall -Wno-unused-function -Wno-unused-variable -Wno-sign-compare")
set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fPIC")
set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -pthread")
set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fno-strict-aliasing")

if (HAVE_W_INCOMPATIBLE_POINTER_TYPES)
    SET(RS_C_FLAGS "${RS_C_FLAGS} -Werror=incompatible-pointer-types")
    if (HAVE_W_DISCARDS_QUALIFIERS)
        set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -Wno-error=incompatible-pointer-types-discards-qualifiers")
    endif()
endif()

set(RS_C_FLAGS "${RS_C_FLAGS}  -Werror=implicit-function-declaration")

#----------------------------------------------------------------------------------------------

if (USE_ASAN)
    set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fno-omit-frame-pointer -fsanitize=address")
elseif(USE_TSAN)
    set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fno-omit-frame-pointer -fsanitize=thread -pie")
elseif(USE_MSAN)
    set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fno-omit-frame-pointer -fsanitize=memory -fsanitize-memory-track-origins=2")
    set(CMAKE_LINKER "${CMAKE_C_COMPILER}")
    if (NOT MSAN_PREFIX)
        message(FATAL_ERROR "Need MSAN_PREFIX")
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -Wl,-rpath=${MSAN_PREFIX}/lib -L${MSAN_PREFIX}/lib -lc++abi -I${MSAN_PREFIX}/include -I${MSAN_PREFIX}/include/c++/v1")
endif()

#----------------------------------------------------------------------------------------------

if (USE_COVERAGE)
    if (NOT CMAKE_BUILD_TYPE STREQUAL "DEBUG")
        message(FATAL_ERROR "Build type must be DEBUG for coverage")
    endif()
    set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -coverage")
endif()

#----------------------------------------------------------------------------------------------

set(RS_C_FLAGS "${RS_COMMON_FLAGS} -std=gnu99")
set(RS_CXX_FLAGS "${RS_COMMON_FLAGS} -fno-rtti -fno-exceptions -std=c++11")

#----------------------------------------------------------------------------------------------

if (NOT APPLE)
    set(RS_SO_FLAGS "-Wl,-Bsymbolic,-Bsymbolic-functions")
endif()
