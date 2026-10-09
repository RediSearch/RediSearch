ADD_LIBRARY(fast_float_strtod OBJECT fast_float_strtod.cpp)
# fast_float.h needs C++11 or newer (scoped enums, constexpr). Nothing set a
# standard for this target, so it fell back to the compiler default — fine on
# gcc, but Apple clang defaults to C++98 and the header fails to parse.
target_compile_features(fast_float_strtod PRIVATE cxx_std_20)
target_compile_definitions(fast_float_strtod PRIVATE FASTFLOAT_SKIP_WHITE_SPACE)
target_compile_definitions(fast_float_strtod PRIVATE FASTFLOAT_ALLOWS_LEADING_PLUS)
