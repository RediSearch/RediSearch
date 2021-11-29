
get_filename_component(root ${CMAKE_CURRENT_SOURCE_DIR}/../../.. ABSOLUTE)
message("# coord/rmr: root: " ${root})

file(GLOB RMR_SRC
    *.c
    redise_parser/*.c)

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${root}/src)

add_library(rmr OBJECT ${RMR_SRC})
