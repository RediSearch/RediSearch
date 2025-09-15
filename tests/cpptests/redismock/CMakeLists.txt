
add_library(redismock STATIC redismock.cpp util.cpp)
message(STATUS "redismock compiler: ${CMAKE_CXX_COMPILER} ${CMAKE_C_COMPILER}")
# set_target_properties(redismock
#     PROPERTIES
#     LINKER_LANGUAGE CXX
# )
