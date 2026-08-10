
add_library(redismock STATIC redismock.cpp util.cpp)
target_compile_features(redismock PUBLIC cxx_std_17)
