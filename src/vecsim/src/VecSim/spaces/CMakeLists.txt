# Build non optimized code in a single project without architecture optimization flag.
project(VectorSimilaritySpaces_no_optimization)
add_library(VectorSimilaritySpaces_no_optimization
	L2/L2.cpp
	IP/IP.cpp
)

include(${root}/cmake/cpu_features.cmake)

project(VectorSimilarity_Spaces)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall")

set(OPTIMIZATIONS "")

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(AMD64|amd64)|(^i.86$)")
	# Check that the compiler supports instructions flag.
	# from gcc14+ -mavx512bw is implicitly enabled when -mavx512vbmi2 is requested
	include(${root}/cmake/x86_64InstructionFlags.cmake)

	# build SSE/AVX* code only on x64 processors.
	# This will add the relevant flag both to the space selector and the optimization.
	if(CXX_AVX512BF16 AND CXX_AVX512VL)
		message("Building with AVX512BF16 and AVX512VL")
		set_source_files_properties(functions/AVX512BF16_VL.cpp PROPERTIES COMPILE_FLAGS "-mavx512bf16  -mavx512vl")
		list(APPEND OPTIMIZATIONS functions/AVX512BF16_VL.cpp)
	endif()

	if(CXX_AVX512VL AND CXX_AVX512FP16)
		message("Building with AVX512FP16 and AVX512VL")
		set_source_files_properties(functions/AVX512FP16_VL.cpp PROPERTIES COMPILE_FLAGS "-mavx512fp16 -mavx512vl")
		list(APPEND OPTIMIZATIONS functions/AVX512FP16_VL.cpp)
	endif()

	if(CXX_AVX512BW AND CXX_AVX512VBMI2)
		message("Building with AVX512BW and AVX512VBMI2")
		set_source_files_properties(functions/AVX512BW_VBMI2.cpp PROPERTIES COMPILE_FLAGS "-mavx512bw  -mavx512vbmi2")
		list(APPEND OPTIMIZATIONS functions/AVX512BW_VBMI2.cpp)
	endif()

	if(CXX_AVX512F)
		message("Building with AVX512F")
		set_source_files_properties(functions/AVX512F.cpp PROPERTIES COMPILE_FLAGS "-mavx512f")
		list(APPEND OPTIMIZATIONS functions/AVX512F.cpp)
	endif()

	if(CXX_AVX512F AND CXX_AVX512BW AND CXX_AVX512VL AND CXX_AVX512VNNI)
		message("Building with AVX512F, AVX512BW, AVX512VL and AVX512VNNI")
		set_source_files_properties(functions/AVX512F_BW_VL_VNNI.cpp PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512bw -mavx512vl -mavx512vnni")
		list(APPEND OPTIMIZATIONS functions/AVX512F_BW_VL_VNNI.cpp)
	endif()

	# F16C is VEX-encoded and requires AVX state, so it is only meaningful when the toolchain
	# can also emit AVX/FMA. Mirrors the OPT_F16C macro condition in x86_64InstructionFlags.cmake.
	set(_has_full_f16c FALSE)
	if(CXX_F16C AND CXX_FMA AND CXX_AVX)
		set(_has_full_f16c TRUE)
	endif()

	# Base AVX2 / AVX2+FMA dispatcher TUs hold only kernels with no F16C dependency.
	# SQ8↔FP16 kernels (which require F16C) live in sibling TUs functions/AVX2_F16C.cpp and
	# functions/AVX2_FMA_F16C.cpp, compiled only when _has_full_f16c is true.
	if(CXX_AVX2)
		message("Building with AVX2")
		set_source_files_properties(functions/AVX2.cpp PROPERTIES COMPILE_FLAGS -mavx2)
		list(APPEND OPTIMIZATIONS functions/AVX2.cpp)
	endif()

	if(CXX_AVX2 AND _has_full_f16c)
		message("Building functions/AVX2_F16C.cpp with AVX2 and F16C")
		set_source_files_properties(functions/AVX2_F16C.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mf16c")
		list(APPEND OPTIMIZATIONS functions/AVX2_F16C.cpp)
	endif()

	if(CXX_AVX2 AND CXX_FMA)
		message("Building with AVX2 and FMA")
		set_source_files_properties(functions/AVX2_FMA.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mfma")
		list(APPEND OPTIMIZATIONS functions/AVX2_FMA.cpp)
	endif()

	if(CXX_AVX2 AND CXX_FMA AND _has_full_f16c)
		message("Building functions/AVX2_FMA_F16C.cpp with AVX2, FMA, and F16C")
		set_source_files_properties(functions/AVX2_FMA_F16C.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mfma -mf16c")
		list(APPEND OPTIMIZATIONS functions/AVX2_FMA_F16C.cpp)
	endif()

	if(CXX_F16C AND CXX_FMA AND CXX_AVX)
		message("Building with CXX_F16C")
		set_source_files_properties(functions/F16C.cpp PROPERTIES COMPILE_FLAGS "-mf16c -mfma -mavx")
		list(APPEND OPTIMIZATIONS functions/F16C.cpp)
	endif()

	if(CXX_AVX)
		message("Building with AVX")
		set_source_files_properties(functions/AVX.cpp PROPERTIES COMPILE_FLAGS -mavx)
		list(APPEND OPTIMIZATIONS functions/AVX.cpp)
	endif()

	if(CXX_SSE3)
		message("Building with SSE3")
		set_source_files_properties(functions/SSE3.cpp PROPERTIES COMPILE_FLAGS -msse3)
		list(APPEND OPTIMIZATIONS functions/SSE3.cpp)
	endif()

	if(CXX_SSE4)
		message("Building with SSE4")
		set_source_files_properties(functions/SSE4.cpp PROPERTIES COMPILE_FLAGS -msse4.1)
		list(APPEND OPTIMIZATIONS functions/SSE4.cpp)
	endif()

	# SSE4 SQ8↔FP16 kernels need F16C, which is VEX-encoded → require -mavx alongside -mf16c
	# (mirrors the F16C.cpp recipe above).
	if(CXX_SSE4 AND _has_full_f16c)
		message("Building functions/SSE4_F16C.cpp with SSE4.1, AVX, and F16C")
		set_source_files_properties(functions/SSE4_F16C.cpp PROPERTIES COMPILE_FLAGS "-msse4.1 -mavx -mf16c")
		list(APPEND OPTIMIZATIONS functions/SSE4_F16C.cpp)
	endif()

	if(CXX_SSE)
		message("Building with SSE")
		set_source_files_properties(functions/SSE.cpp PROPERTIES COMPILE_FLAGS -msse)
		list(APPEND OPTIMIZATIONS functions/SSE.cpp)
	endif()
endif()

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(aarch64)|(arm64)|(ARM64)|(armv.*)")
	include(${root}/cmake/aarch64InstructionFlags.cmake)

	# Create different optimization implementations for ARM architecture
	if (CXX_NEON_DOTPROD)
		message("Building with ARMV8.2 with dotprod")
		set_source_files_properties(functions/NEON_DOTPROD.cpp PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+dotprod")
		list(APPEND OPTIMIZATIONS functions/NEON_DOTPROD.cpp)
	endif()
	if (CXX_ARMV8A)
		message("Building with ARMV8A")
		set_source_files_properties(functions/NEON.cpp PROPERTIES COMPILE_FLAGS "-march=armv8-a")
		list(APPEND OPTIMIZATIONS functions/NEON.cpp)
	endif()

	# NEON half-precision support
	if (CXX_NEON_HP AND CXX_ARMV8A)
		message("Building with NEON+HP")
		set_source_files_properties(functions/NEON_HP.cpp PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+fp16")
		list(APPEND OPTIMIZATIONS functions/NEON_HP.cpp)
  endif()

	# NEON FHM (FEAT_FHM / asimdfhm) support
	if (CXX_NEON_FHM AND CXX_ARMV8A)
		message("Building with NEON+FHM")
		set_source_files_properties(functions/NEON_FHM.cpp PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+fp16fml")
		list(APPEND OPTIMIZATIONS functions/NEON_FHM.cpp)
	endif()

	# NEON bfloat16 support
	if (CXX_NEON_BF16)
		message("Building with NEON + BF16")
		set_source_files_properties(functions/NEON_BF16.cpp PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+bf16")
		list(APPEND OPTIMIZATIONS functions/NEON_BF16.cpp)
	endif()

	# SVE support
	if (CXX_SVE)
		message("Building with SVE")
		set_source_files_properties(functions/SVE.cpp PROPERTIES COMPILE_FLAGS "-march=armv8-a+sve")
		list(APPEND OPTIMIZATIONS functions/SVE.cpp)
	endif()

	# SVE with BF16 support
	if (CXX_SVE_BF16)
		message("Building with SVE + BF16")
		set_source_files_properties(functions/SVE_BF16.cpp PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+sve+bf16")
		list(APPEND OPTIMIZATIONS functions/SVE_BF16.cpp)
	endif()

	# SVE2 support
	if (CXX_SVE2)
		message("Building with ARMV9A and SVE2")
		set_source_files_properties(functions/SVE2.cpp PROPERTIES COMPILE_FLAGS "-march=armv9-a+sve2")
		list(APPEND OPTIMIZATIONS functions/SVE2.cpp)
	endif()
endif()

# Here we are compiling the space selectors with the relevant optimization flag.
add_library(VectorSimilaritySpaces
	L2_space.cpp
	IP_space.cpp
	spaces.cpp
	${OPTIMIZATIONS}
	computer/preprocessor_container.cpp
)

target_link_libraries(VectorSimilaritySpaces VectorSimilaritySpaces_no_optimization cpu_features)
