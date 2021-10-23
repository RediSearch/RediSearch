
if(CMAKE_BUILD_TYPE STREQUAL DEBUG)
    set(CMAKE_C_DEBUG_FLAGS "-include ${PROJECT_SOURCE_DIR}/src/common.h -I${PROJECT_SOURCE_DIR}/deps -D_DEBUG")
	set(CMAKE_CXX_DEBUG_FLAGS "-include ${PROJECT_SOURCE_DIR}/src/common.h -I${PROJECT_SOURCE_DIR}/deps -D_DEBUG")
	set(RS_DEBUG_SRC "${PROJECT_SOURCE_DIR}/deps/readies/cetara/diag/gdb.c")
endif()

# Preserve frame pointer for profile-related builds
if (PROFILE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_DEBUG_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_DEBUG_FLAGS}")
