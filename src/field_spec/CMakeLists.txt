file(GLOB FIELD_SPEC_SOURCES "*.c")

add_library(field_spec STATIC ${FIELD_SPEC_SOURCES})

target_include_directories(field_spec PRIVATE
    .
    ..
)
