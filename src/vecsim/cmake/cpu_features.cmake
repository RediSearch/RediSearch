include(FetchContent)
option(BUILD_TESTING "" OFF)
option(CMAKE_POSITION_INDEPENDENT_CODE "" ON)
FetchContent_Declare(
	cpu_features
	GIT_REPOSITORY  https://github.com/google/cpu_features.git
	GIT_TAG         v0.10.1
)
FetchContent_MakeAvailable(cpu_features)
