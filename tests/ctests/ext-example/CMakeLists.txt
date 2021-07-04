ADD_LIBRARY(example_extension SHARED example.c)

SET_TARGET_PROPERTIES(example_extension PROPERTIES PREFIX "lib")
SET_TARGET_PROPERTIES(example_extension PROPERTIES SUFFIX ".so")

SET_TARGET_PROPERTIES(example_extension PROPERTIES COMPILE_FLAGS "-fvisibility=default")
