
get_filename_component(root ${CMAKE_CURRENT_SOURCE_DIR}/../../.. ABSOLUTE)
message("# coord/rmr: root: " ${root})

file(GLOB RMR_SRC
    *.c
    redise_parser/*.c)

include_directories(
	${PROJECT_SOURCE_DIR}/src
	${root}/src
	${root}/deps/libuv
	${root}/deps/libuv/include)

if (APPLE)
	include_directories(${LIBSSL_DIR}/include)
endif()

add_library(rmr OBJECT ${RMR_SRC})
