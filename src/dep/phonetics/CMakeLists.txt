ADD_LIBRARY(metaphone OBJECT double_metaphone.cpp)
ADD_EXECUTABLE(dmtest dmtest.cpp $<TARGET_OBJECTS:metaphone>)
ADD_TEST(NAME dmtest COMMAND dmtest)
