ADD_LIBRARY(redismock SHARED redismock.cpp util.cpp)
IF (APPLE) 
    SET_TARGET_PROPERTIES(redismock
        PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
ENDIF()
