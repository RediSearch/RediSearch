
file(GLOB COORDINATOR_SRC *.c *.cpp)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(coordinator-core OBJECT ${COORDINATOR_SRC})
