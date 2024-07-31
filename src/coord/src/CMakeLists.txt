
file(GLOB COORDINATOR_SRC *.c *.cpp)

include_directories(.)

add_library(coordinator-core OBJECT ${COORDINATOR_SRC})
