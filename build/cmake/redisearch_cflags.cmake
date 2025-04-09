
include(CheckCCompilerFlag)

# This file exposes the following:
# RS_C_FLAGS
# RS_CXX_FLAGS
# RS_COMMON_FLAGS
# RS_DEFINES variables

function(ADD_LDFLAGS _TARGET NEW_FLAGS)
    get_target_property(LD_FLAGS ${_TARGET} LINK_FLAGS)
    if (LD_FLAGS)
        set(NEW_FLAGS "${LD_FLAGS} ${NEW_FLAGS}")
    endif()
    set_target_properties(${_TARGET} PROPERTIES LINK_FLAGS ${NEW_FLAGS})
endfunction()

CHECK_C_COMPILER_FLAG("-Wincompatible-pointer-types" HAVE_W_INCOMPATIBLE_POINTER_TYPES)
CHECK_C_COMPILER_FLAG("-Wincompatible-pointer-types-discards-qualifiers" HAVE_W_DISCARDS_QUALIFIERS)

set(RS_COMMON_FLAGS "-Wall -Wno-unused-function -Wno-unused-variable -Wno-sign-compare")
set(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fPIC -pthread -fno-strict-aliasing")

set(RS_C_FLAGS "${RS_C_FLAGS} -Werror=implicit-function-declaration")

if (HAVE_W_INCOMPATIBLE_POINTER_TYPES)
    set(RS_C_FLAGS "${RS_C_FLAGS} -Werror=incompatible-pointer-types")
    if (HAVE_W_DISCARDS_QUALIFIERS)
        set(RS_C_FLAGS "${RS_C_FLAGS} -Wno-error=incompatible-pointer-types-discards-qualifiers")
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
        set(MSAN_PREFIX "/opt/llvm-project/build-msan")
        if (NOT EXISTS ${MSAN_PREFIX})
            message(FATAL_ERROR "LLVM/MSAN stdlibc++ directory '${MSAN_PREFIX}' is missing")
        endif()
    endif()

    set(LLVM_CXX_FLAGS "-stdlib=libc++ -I${MSAN_PREFIX}/include -I${MSAN_PREFIX}/include/c++/v1")
    set(LLVM_LD_FLAGS "-stdlib=libc++ -Wl,-rpath=${MSAN_PREFIX}/lib -L${MSAN_PREFIX}/lib -lc++abi")

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LLVM_CXX_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${LLVM_LD_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LLVM_LD_FLAGS}")
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
# set(RS_CXX_FLAGS "${RS_COMMON_FLAGS} -fno-rtti -fno-exceptions -std=c++11")

#----------------------------------------------------------------------------------------------

if (${OS} STREQUAL "linux")
	set(RS_LINK_LIBS m dl rt)
elseif (${OS} STREQUAL "macos")
	set(RS_LINK_LIBS m dl)
endif()

message("# CMAKE_C_COMPILER_ID: " ${CMAKE_C_COMPILER_ID})

if (CMAKE_C_COMPILER_ID STREQUAL "GNU")
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libgcc -static-libstdc++")
	set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libgcc -static-libstdc++")
elseif (CMAKE_C_COMPILER_ID STREQUAL "Clang" OR CMAKE_C_COMPILER_ID STREQUAL "Intel")
	if (APPLE)
		set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}")
		set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}")
	endif()
endif()

if (NOT APPLE)
    set(RS_SO_FLAGS "${RS_SO_FLAGS} -Wl,-Bsymbolic,-Bsymbolic-functions")
endif()
