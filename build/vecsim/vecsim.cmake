
if (NOT DEFINED __VECSIM__)
set(__VECSIM__ 1)

if (NOT DEFINED VECSIM_ARCH)
	set(VECSIM_ARCH "native")
endif()
message("RediSearch/VecSim VECSIM_ARCH: " ${VECSIM_ARCH})

set(RS_VECSIM_SO_FLAGS "-lstdc++")

#----------------------------------------------------------------------------------------------

include (ExternalProject)

if (USE_ASAN)
	set(VECSIM_SAN_ARGS -DUSE_ASAN:bool=ON)
elseif(USE_MSAN)
	set(VECSIM_SAN_ARGS -DUSE_MSAN:bool=ON)
endif()

message("RediSearch/VecSim dir: ${root}/deps/VectorSimilarity/src")

ExternalProject_Add (VectorSimilarity
	SOURCE_DIR      ${root}/deps/VectorSimilarity/src
	PREFIX          ${binroot}/VectorSimilarity
	CMAKE_ARGS      -DCMAKE_INSTALL_PREFIX:PATH=${binroot}/VectorSimilarity -Dbinroot:string=${binroot}
	CMAKE_ARGS      -DCMAKE_BUILD_TYPE:string=${CMAKE_BUILD_TYPE} -DVECSIM_STATIC:bool=ON
	CMAKE_ARGS      -DOS:string=${OS} -DARCH:string=${ARCH} -DOSNICK:string=${OSNICK}
	CMAKE_ARGS      -DVECSIM_ARCH:string=${VECSIM_ARCH}
	CMAKE_ARGS      -DBUILD_TESTS:bool=OFF
	CMAKE_ARGS      ${VECSIM_SAN_ARGS}
)

ExternalProject_Get_Property (VectorSimilarity install_dir)
set(VectorSimilarity_bindir ${install_dir})
message("RediSearch/VecSim bindir: ${VectorSimilarity_bindir}")

include_directories (${VectorSimilarity_bindir}/include)

add_library (vecsim STATIC IMPORTED)
set_target_properties (vecsim PROPERTIES IMPORTED_LOCATION ${VectorSimilarity_bindir}/libVectorSimilarity.a)

add_library (vecsim-spaces STATIC IMPORTED)
set_target_properties (vecsim-spaces PROPERTIES IMPORTED_LOCATION ${VectorSimilarity_bindir}/libVectorSimilaritySpaces.a)

set(VECSIM_LIBS vecsim vecsim-spaces)

endif() # __VECSIM__
