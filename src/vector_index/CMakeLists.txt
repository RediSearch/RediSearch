file(GLOB VECTOR_INDEX_SOURCES "*.c")

add_library(vector_index STATIC ${VECTOR_INDEX_SOURCES})

target_include_directories(vector_index PRIVATE
    .
    ..
)
