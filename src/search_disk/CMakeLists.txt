file(GLOB SEARCH_DISK_SOURCES "*.c")

add_library(search_disk STATIC ${SEARCH_DISK_SOURCES})

target_include_directories(search_disk PRIVATE
    .
    ..
)
