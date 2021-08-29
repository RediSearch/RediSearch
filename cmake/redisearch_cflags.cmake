INCLUDE(CheckCCompilerFlag)

# This file exposes the following:
# RS_C_FLAGS
# RS_CXX_FLAGS
# RS_COMMON_FLAGS
# RS_DEFINES variables
# This does not handle linker flags.

FUNCTION(ADD_LDFLAGS _TARGET NEW_FLAGS)
    GET_TARGET_PROPERTY(LD_FLAGS ${_TARGET} LINK_FLAGS)
    IF(LD_FLAGS)
        SET(NEW_FLAGS "${LD_FLAGS} ${NEW_FLAGS}")
    ENDIF()
    SET_TARGET_PROPERTIES(${_TARGET} PROPERTIES LINK_FLAGS ${NEW_FLAGS})
ENDFUNCTION()

CHECK_C_COMPILER_FLAG("-Wincompatible-pointer-types" HAVE_W_INCOMPATIBLE_POINTER_TYPES)
CHECK_C_COMPILER_FLAG("-Wincompatible-pointer-types-discards-qualifiers" HAVE_W_DISCARDS_QUALIFIERS)

SET(RS_COMMON_FLAGS "-Wall -Wno-unused-function -Wno-unused-variable -Wno-sign-compare")
SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fPIC -Werror=implicit-function-declaration")
SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -pthread")
SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fno-strict-aliasing")


IF (HAVE_W_INCOMPATIBLE_POINTER_TYPES)
    SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -Werror=incompatible-pointer-types")
    IF (HAVE_W_DISCARDS_QUALIFIERS)
        SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -Wno-error=incompatible-pointer-types-discards-qualifiers")
    ENDIF()
ENDIF()


IF (USE_ASAN)
    SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fno-omit-frame-pointer -fsanitize=address")
ELSEIF(USE_TSAN)
    SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fno-omit-frame-pointer -fsanitize=thread -pie")
ELSEIF(USE_MSAN)
    SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fno-omit-frame-pointer -fsanitize=memory -fsanitize-memory-track-origins=2")
    SET(CMAKE_LINKER "${CMAKE_C_COMPILER}")
    IF (NOT MSAN_PREFIX)
        MESSAGE(FATAL_ERROR "Need MSAN_PREFIX")
    ENDIF()
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -Wl,-rpath=${MSAN_PREFIX}/lib -L${MSAN_PREFIX}/lib -lc++abi -I${MSAN_PREFIX}/include -I${MSAN_PREFIX}/include/c++/v1")
ENDIF()

IF (USE_COVERAGE)
    IF (NOT CMAKE_BUILD_TYPE STREQUAL "DEBUG")
        MESSAGE(FATAL_ERROR "Build type must be DEBUG for coverage")
    ENDIF()
    SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -coverage")
ENDIF()

SET(RS_C_FLAGS "${RS_COMMON_FLAGS} -std=gnu99")
SET(RS_CXX_FLAGS "${RS_COMMON_FLAGS} -fno-rtti -fno-exceptions -std=c++11")

IF (NOT APPLE)
    SET(RS_SO_FLAGS "-Wl,-Bsymbolic,-Bsymbolic-functions")
ENDIF()
