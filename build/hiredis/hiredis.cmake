
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
set(ENABLE_SSL ON CACHE BOOL "Build hiredis with ssl")
option(DISABLE_TESTS "If tests should be compiled or not" ON)

add_subdirectory(${root}/deps/hiredis hiredis)

# Override hiredis debug postfix to prevent 'd' suffix on debug builds
# since we already separate debug and release artifacts into different directories
set_target_properties(hiredis PROPERTIES DEBUG_POSTFIX "")
set_target_properties(hiredis_ssl PROPERTIES DEBUG_POSTFIX "")

if(APPLE)
    include_directories(/usr/local/opt/openssl/include)
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L/usr/local/opt/openssl/lib")
else()
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-Bsymbolic,-Bsymbolic-functions")
endif()

set(HIREDIS_LIBS hiredis hiredis_ssl)
