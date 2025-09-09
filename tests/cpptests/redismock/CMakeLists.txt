
add_library(redismock STATIC redismock.cpp util.cpp)

# Handle C++ standard library linking based on compiler
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    find_library(LIBCXX_FOUND c++)
    if(LIBCXX_FOUND)
        target_compile_options(redismock PRIVATE -stdlib=libc++)
        target_link_options(redismock PRIVATE -stdlib=libc++)
    else()
        target_link_libraries(redismock stdc++)
        target_link_options(redismock PRIVATE -static-libstdc++ -static-libgcc)
    endif()
else()
    target_link_libraries(redismock stdc++)
    target_link_options(redismock PRIVATE -shared-libgcc)
endif()
