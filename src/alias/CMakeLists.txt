file(GLOB ALIAS_SOURCES "*.c")

add_library(alias STATIC ${ALIAS_SOURCES})

target_include_directories(alias PRIVATE
    .
    ..
)
