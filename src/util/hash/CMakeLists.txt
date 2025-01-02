# Set the C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Collect source files
file(GLOB SOURCES "*.cpp")

# Find Boost
find_package(Boost REQUIRED)

# Add the library
add_library(redisearch-hash STATIC ${SOURCES})

# Set include directories
target_include_directories(redisearch-hash PRIVATE ${Boost_INCLUDE_DIRS})
