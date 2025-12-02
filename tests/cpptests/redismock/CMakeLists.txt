
add_library(redismock STATIC redismock.cpp util.cpp ${CMAKE_LD_LIBS})
set_target_properties(redismock PROPERTIES
    LINKER_LANGUAGE CXX
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)
