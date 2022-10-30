
add_library(example_extension SHARED example.c)

set_target_properties(example_extension PROPERTIES PREFIX "lib")
set_target_properties(example_extension PROPERTIES SUFFIX ".so")

set_target_properties(example_extension PROPERTIES COMPILE_FLAGS "-fvisibility=default")
