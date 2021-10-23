
set(ENABLE_SSL ON CACHE BOOL "Build hiredis with ssl")
option(DISABLE_TESTS "If tests should be compiled or not" ON)

add_subdirectory(${RS_DIR}/deps/hiredis hiredis)

# set(HIREDIS_LIBS "hiredis_static hiredis_ssl_static")
set(HIREDIS_LIBS hiredis hiredis_ssl)
