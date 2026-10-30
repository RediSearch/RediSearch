
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
set(ENABLE_SSL ON CACHE BOOL "Build hiredis with ssl")
option(DISABLE_TESTS "If tests should be compiled or not" ON)

add_subdirectory(${root}/deps/hiredis hiredis)

# On macOS, OpenSSL comes from the root CMakeLists.txt (OPENSSL_ROOT_DIR).
if(NOT APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-Bsymbolic,-Bsymbolic-functions")
endif()

set(HIREDIS_LIBS hiredis hiredis_ssl)
