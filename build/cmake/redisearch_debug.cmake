if(CMAKE_BUILD_TYPE STREQUAL DEBUG)
    SET(CMAKE_C_DEBUG_FLAGS "-include ${PROJECT_SOURCE_DIR}/src/common.h -I${PROJECT_SOURCE_DIR}/deps -D_DEBUG")
	SET(CMAKE_CXX_DEBUG_FLAGS "-include ${PROJECT_SOURCE_DIR}/src/common.h -I${PROJECT_SOURCE_DIR}/deps -D_DEBUG")
	SET(RS_DEBUG_SRC "${PROJECT_SOURCE_DIR}/deps/readies/cetara/diag/gdb.c")
endif()

# Preserve frame pointer for profile related builds
IF (PROFILE)
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -fno-omit-frame-pointer")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
ENDIF()

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_DEBUG_FLAGS}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_DEBUG_FLAGS}")
