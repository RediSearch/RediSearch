set(CMAKE_CXX_STANDARD 20)

if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

# Collect source files
file(GLOB SOURCES "*.cpp")

# Find Boost (no need to specify `geometry` as it is header-only)
find_package(Boost REQUIRED)

# Add the library
add_library(redisearch-geometry STATIC ${SOURCES})

# Include Boost headers
target_include_directories(redisearch-geometry PRIVATE ${Boost_INCLUDE_DIRS})

# Link Boost libraries (optional for header-only components, but safe to keep)
target_link_libraries(redisearch-geometry PUBLIC ${Boost_LIBRARIES})
