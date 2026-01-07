# Build libnu as a static library so Rust code can link to it
file(GLOB LIBNU_SOURCES "*.c")
add_library(libnu STATIC ${LIBNU_SOURCES})
target_include_directories(libnu PUBLIC .)
