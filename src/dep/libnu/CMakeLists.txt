set(LIBNU_SRC
	cesu8.c
	ducet.c
	extra.c
	strcoll.c
	strings.c
	tofold.c
	tolower.c
	toupper.c
	utf16.c
	utf16be.c
	utf16he.c
	utf16le.c
	utf32.c
	utf32be.c
	utf32he.c
	utf32le.c
	utf8.c
	validate.c
	version.c
)

add_library(nu STATIC ${LIBNU_SRC})

target_include_directories(nu PUBLIC
	$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
	$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
	$<INSTALL_INTERFACE:include>)

target_compile_definitions(nu PUBLIC
	${NU_BUILD_OPTIONS}
	$<$<OR:$<CONFIG:Release>,$<CONFIG:PROF>>:NU_DISABLE_CONTRACTIONS>
)

set_target_properties(nu PROPERTIES DEBUG_POSTFIX d)
if(NOT UNIX)
	set_target_properties(nu PROPERTIES OUTPUT_NAME libnu)
endif()

file(GLOB INCLUDES *.h)
install(FILES ${INCLUDES} DESTINATION include/libnu)
install(TARGETS nu DESTINATION lib EXPORT nunicode-targets)
install(EXPORT nunicode-targets DESTINATION lib/cmake/nunicode
	NAMESPACE nunicode:: FILE nunicode-config.cmake)
