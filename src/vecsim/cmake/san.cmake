option(USE_ASAN "Use AddressSanitizer (clang)" OFF)
if (USE_ASAN)
	# define this before project()
	# Respect the toolchain selected by the caller, including versioned CI compilers.
	if(NOT DEFINED CMAKE_C_COMPILER AND NOT DEFINED ENV{CC})
		find_program(CMAKE_C_COMPILER NAMES clang clang-21 clang-18)
	endif()
	if(NOT DEFINED CMAKE_CXX_COMPILER AND NOT DEFINED ENV{CXX})
		find_program(CMAKE_CXX_COMPILER NAMES clang++ clang++-21 clang++-18)
	endif()
	set(CLANG_SAN_FLAGS "-fno-omit-frame-pointer -fsanitize=address -fsized-deallocation")

endif()
