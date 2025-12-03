
add_library(redismock STATIC redismock.cpp util.cpp)
set_target_properties(redismock PROPERTIES LINKER_LANGUAGE CXX)
