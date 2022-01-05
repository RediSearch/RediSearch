
add_library(redismock SHARED redismock.cpp util.cpp)

if (APPLE) 
    set_target_properties(redismock PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif()

set_target_properties(redismock PROPERTIES COMPILE_FLAGS "-fvisibility=default")
