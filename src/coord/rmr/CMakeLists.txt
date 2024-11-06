
get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../../.. ABSOLUTE)
message("# coord/rmr: root: " ${root})

file(GLOB RMR_SRC
    *.c
    redise_parser/*.c)

include_directories(
	${root}/src
	${root}/src/coord
	${root}/deps/libuv
	${root}/deps/libuv/include)

if (APPLE)
	include_directories(${LIBSSL_DIR}/include)
endif()

add_library(rmr OBJECT ${RMR_SRC})
