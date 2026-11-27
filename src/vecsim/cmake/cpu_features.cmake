include(FetchContent)
option(BUILD_TESTING "" OFF)
option(CMAKE_POSITION_INDEPENDENT_CODE "" ON)
FetchContent_Declare(
	cpu_features
	GIT_REPOSITORY  https://github.com/google/cpu_features.git
	# v0.11.0 plus google/cpu_features#458, which fixes UBSan misaligned loads in
	# IsVendor/SetVendor. Switch to v0.12.0 once it is released.
	GIT_TAG         a1c8a2fcf49d534799b966c63e0b2124cd2f60ae
)
FetchContent_MakeAvailable(cpu_features)
