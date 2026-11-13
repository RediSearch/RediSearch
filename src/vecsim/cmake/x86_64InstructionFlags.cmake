# This file checks if the compiler supports certain CPU flags. If the flags are supported,
# it adds compilation definitions to include code sections that require these flags.

include(CheckCXXCompilerFlag)

# build SSE/AVX* code only on x64 processors.
# Check that the compiler supports instructions flag.
# This will add the relevant flag both the the space selector and the optimization.
CHECK_CXX_COMPILER_FLAG(-mavx512vl CXX_AVX512VL)
CHECK_CXX_COMPILER_FLAG(-mavx512bf16 CXX_AVX512BF16)
CHECK_CXX_COMPILER_FLAG(-mavx512bw CXX_AVX512BW)
CHECK_CXX_COMPILER_FLAG(-mavx512vbmi2 CXX_AVX512VBMI2)
CHECK_CXX_COMPILER_FLAG(-mavx512fp16 CXX_AVX512FP16)
CHECK_CXX_COMPILER_FLAG(-mavx512f CXX_AVX512F)
CHECK_CXX_COMPILER_FLAG(-mavx512vnni CXX_AVX512VNNI)
CHECK_CXX_COMPILER_FLAG(-mavx2 CXX_AVX2)
CHECK_CXX_COMPILER_FLAG(-mavx CXX_AVX)
CHECK_CXX_COMPILER_FLAG(-mf16c CXX_F16C)
CHECK_CXX_COMPILER_FLAG(-mfma CXX_FMA)
CHECK_CXX_COMPILER_FLAG(-msse4.1 CXX_SSE4)
CHECK_CXX_COMPILER_FLAG(-msse3 CXX_SSE3)
CHECK_CXX_COMPILER_FLAG(-msse CXX_SSE)

# Check binutils version for AVX512 instruction support.
# Even if the compiler supports certain flags, the assembler (binutils) may not.
# - AVX512-BF16 requires binutils >= 2.34
# - AVX512-FP16 requires binutils >= 2.38
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
	# Get binutils/assembler version
	execute_process(COMMAND as --version
				OUTPUT_VARIABLE AS_VERSION_OUTPUT
				OUTPUT_STRIP_TRAILING_WHITESPACE
				ERROR_QUIET)
	# Extract version number (e.g., "2.34" from "GNU assembler (GNU Binutils for Ubuntu) 2.34")
	string(REGEX MATCH "[0-9]+\\.[0-9]+" BINUTILS_VERSION "${AS_VERSION_OUTPUT}")

	if(BINUTILS_VERSION)
		message(STATUS "Detected binutils version: ${BINUTILS_VERSION}")

		# AVX512-BF16 requires binutils >= 2.34
		if(BINUTILS_VERSION VERSION_LESS "2.34")
			message(STATUS "binutils ${BINUTILS_VERSION} < 2.34, turning off CXX_AVX512BF16 flag.")
			set(CXX_AVX512BF16 FALSE)
		endif()

		# AVX512-FP16 requires binutils >= 2.38
		if(BINUTILS_VERSION VERSION_LESS "2.38")
			message(STATUS "binutils ${BINUTILS_VERSION} < 2.38, turning off CXX_AVX512FP16 flag.")
			set(CXX_AVX512FP16 FALSE)
		endif()
	else()
		message(WARNING "Could not detect binutils version, AVX512 features may fail to assemble")
	endif()
endif()

if(CXX_AVX512VL AND CXX_AVX512BF16)
	add_compile_definitions(OPT_AVX512_BF16_VL)
endif()

if(CXX_AVX512VL AND CXX_AVX512FP16)
	add_compile_definitions(OPT_AVX512_FP16_VL)
endif()

if(CXX_AVX512F)
	add_compile_definitions(OPT_AVX512F)
endif()

if(CXX_AVX512BW AND CXX_AVX512VBMI2)
	add_compile_definitions(OPT_AVX512_BW_VBMI2)
endif()

if(CXX_AVX512F AND CXX_AVX512BW AND CXX_AVX512VL AND CXX_AVX512VNNI)
	add_compile_definitions(OPT_AVX512_F_BW_VL_VNNI)
endif()

# OPT_F16C is unusual compared to the other OPT_* macros above:
#
#  1. It is a *capability* gate, not a dispatch tier. Every other OPT_* maps 1:1 to a single
#     ISA tier that owns its own translation unit (OPT_AVX2 -> AVX2.cpp, OPT_SSE4 -> SSE4.cpp).
#     F16C owns no tier of its own; it only enables the vcvtph2ps (FP16<->FP32) conversion that
#     several tiers need. So it is hoisted *around* multiple tiers (AVX2_FMA / AVX2 / SSE4 for
#     the SQ8<->FP16 kernels) rather than selecting one.
#
#  2. It is a compound guard (CXX_F16C AND CXX_FMA AND CXX_AVX), not a single flag. F16C is
#     VEX-encoded, so vcvtph2ps requires AVX state to execute -- emitting it without AVX is
#     invalid. Defining OPT_F16C therefore implies AVX is present, and the F16C kernels must be
#     compiled with -mf16c added *on top of* -mavx (see functions/*_F16C.cpp in
#     src/VecSim/spaces/CMakeLists.txt). The base AVX2.cpp / SSE4.cpp objects stay F16C-free so
#     they still run on CPUs without F16C.
#
#  3. The AVX-512 tier deliberately does NOT use this gate: _mm512_cvtph_ps is part of AVX512F
#     itself, so the AVX-512 SQ8<->FP16 path needs only OPT_AVX512F and lives outside any
#     OPT_F16C guard.
if(CXX_F16C AND CXX_FMA AND CXX_AVX)
	add_compile_definitions(OPT_F16C)
endif()

if(CXX_AVX2)
	add_compile_definitions(OPT_AVX2)
endif()

if(CXX_AVX2 AND CXX_FMA)
	add_compile_definitions(OPT_AVX2_FMA)
endif()

if(CXX_AVX)
	add_compile_definitions(OPT_AVX)
endif()

if(CXX_SSE4)
	add_compile_definitions(OPT_SSE4)
endif()

if(CXX_SSE3)
	add_compile_definitions(OPT_SSE3)
endif()

if(CXX_SSE)
	add_compile_definitions(OPT_SSE)
endif()
