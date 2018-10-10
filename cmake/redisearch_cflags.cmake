INCLUDE(CheckCCompilerFlag)

# This file exposes the following:
# RS_C_FLAGS
# RS_CXX_FLAGS
# RS_COMMON_FLAGS
# RS_DEFINES variables
# This does not handle linker flags.

CHECK_C_COMPILER_FLAG("-Wincompatible-pointer-types" HAVE_W_INCOMPATIBLE_POINTER_TYPES)
CHECK_C_COMPILER_FLAG("-Wincompatible-pointer-types-discards-qualifiers" HAVE_W_DISCARDS_QUALIFIERS)

SET(RS_COMMON_FLAGS "-Wall -Wno-unused-function -Wno-unused-variable -Wno-unused-result")
SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -fPIC -Werror=implicit-function-declaration")
IF (HAVE_W_INCOMPATIBLE_POINTER_TYPES)
    SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -Werror=incompatible-pointer-types")
    IF (HAVE_W_DISCARDS_QUALIFIERS)
        SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -Wno-error=incompatible-pointer-types-discards-qualifiers")
    ENDIF()
ENDIF()
SET(RS_COMMON_FLAGS "${RS_COMMON_FLAGS} -pthread")

SET(RS_C_FLAGS "${RS_COMMON_FLAGS} -std=gnu99")
SET(RS_CXX_FLAGS "${RS_COMMON_FLAGS} -fno-rtti -fno-exceptions")
