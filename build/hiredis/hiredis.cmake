
set(BUILD_SHARED_LIBS OFF)
set(ENABLE_SSL ON CACHE BOOL "Build hiredis with ssl")
option(DISABLE_TESTS "If tests should be compiled or not" ON)

add_subdirectory(${root}/deps/hiredis hiredis)

if (APPLE)
    include_directories(/usr/local/opt/openssl/include)
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L/usr/local/opt/openssl/lib")
endif()

set(HIREDIS_LIBS hiredis hiredis_ssl)
