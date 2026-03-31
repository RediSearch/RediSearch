# cmake/patch_snowball_alloc.cmake
#
# Portable CMake -P script to patch snowball source files:
#   - Adds `#include "rmalloc.h"` after the first `#include <stdlib.h>` line
#   - Replaces standard allocator calls with rm_* equivalents
#   - Fixes relative include paths for build-tree files
#
# Usage:
#   cmake -DINPUT=<src> -DOUTPUT=<dst> -P cmake/patch_snowball_alloc.cmake

if(NOT DEFINED INPUT OR NOT DEFINED OUTPUT)
    message(FATAL_ERROR "INPUT and OUTPUT must be defined")
endif()

file(READ "${INPUT}" content)

# Add #include "rmalloc.h" after #include <stdlib.h>
string(REPLACE "#include <stdlib.h>" "#include <stdlib.h>\n#include \"rmalloc.h\"" content "${content}")

# For files that don't include <stdlib.h> (e.g., api.c includes "header.h"),
# add after #include "header.h" instead.
string(FIND "${content}" "rmalloc.h" _has_rmalloc)
if(_has_rmalloc EQUAL -1)
    string(REPLACE "#include \"header.h\"" "#include \"header.h\"\n#include \"rmalloc.h\"" content "${content}")
endif()

# Fix relative include paths that don't resolve from the build tree.
# Converts e.g. "../include/libstemmer.h" → "libstemmer.h" so they
# can be found via target_include_directories instead.
string(REPLACE "#include \"../include/" "#include \"" content "${content}")
string(REPLACE "#include \"../runtime/" "#include \"" content "${content}")

# Replace allocator calls with rm_* equivalents.
# The regex [^_a-zA-Z] prevents matching already-prefixed names like rm_free.
string(REGEX REPLACE "([^_a-zA-Z])calloc\\(" "\\1rm_calloc(" content "${content}")
string(REGEX REPLACE "([^_a-zA-Z])malloc\\(" "\\1rm_malloc(" content "${content}")
string(REGEX REPLACE "([^_a-zA-Z])realloc\\(" "\\1rm_realloc(" content "${content}")
string(REGEX REPLACE "([^_a-zA-Z])free\\(" "\\1rm_free(" content "${content}")

# Handle calls at the very start of a line
string(REGEX REPLACE "(^|\n)calloc\\(" "\\1rm_calloc(" content "${content}")
string(REGEX REPLACE "(^|\n)malloc\\(" "\\1rm_malloc(" content "${content}")
string(REGEX REPLACE "(^|\n)realloc\\(" "\\1rm_realloc(" content "${content}")
string(REGEX REPLACE "(^|\n)free\\(" "\\1rm_free(" content "${content}")

file(WRITE "${OUTPUT}" "${content}")
