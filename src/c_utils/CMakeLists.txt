ADD_EXECUTABLE(sizes sizes.cpp)
ADD_EXECUTABLE(apibench apibench.cpp)

TARGET_LINK_LIBRARIES(sizes redisearch)
TARGET_LINK_LIBRARIES(apibench redisearchS redismock)
