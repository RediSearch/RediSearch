file(GLOB CURSOR_SOURCES "*.c")

add_library(cursor STATIC ${CURSOR_SOURCES})

target_include_directories(cursor PRIVATE
    .
    ..
)
