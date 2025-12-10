
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
set(ENABLE_SSL ON CACHE BOOL "Build hiredis with ssl")
option(DISABLE_TESTS "If tests should be compiled or not" ON)

add_subdirectory(${root}/deps/hiredis hiredis)

# Disable -Werror for hiredis (third-party dependency)
if(TARGET hiredis)
    target_compile_options(hiredis PRIVATE -Wno-error)
endif()
if(TARGET hiredis_ssl)
    target_compile_options(hiredis_ssl PRIVATE -Wno-error)
endif()

if(APPLE)
    include_directories(/usr/local/opt/openssl/include)
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L/usr/local/opt/openssl/lib")
else()
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-Bsymbolic,-Bsymbolic-functions")
endif()

set(HIREDIS_LIBS hiredis hiredis_ssl)
