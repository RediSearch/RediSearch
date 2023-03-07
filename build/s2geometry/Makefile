
ROOT=../..

MK.cmake=1

include $(ROOT)/deps/readies/mk/main

#----------------------------------------------------------------------------------------------

define HELPTEXT
make build          # compile and link
  DEBUG=1             # build for debugging
  WHY=1               # explain CMake decisions (in /tmp/cmake-why)
  FORCE=1             # Force CMake rerun (default)
  CMAKE_ARGS=...      # extra arguments to CMake
  SLOW=1              # do not parallelize build (for diagnostics)
make clean         # remove build artifacts
  ALL=1              # remove entire artifacts directory

endef

#----------------------------------------------------------------------------------------------

SRCDIR=$(ROOT)/deps/s2geometry

BINDIR=$(BINROOT)/s2geometry

TARGET=$(BINDIR)/libs2.a

#----------------------------------------------------------------------------------------------

include $(MK)/defs

define CMAKE_DEFS +=
	CMAKE_BUILD_TYPE=Release
	BUILD_SHARED_LIBS=off
	WITH_GFLAGS=off
	WITH_GTEST=off
	BUILD_EXAMPLES=off
	CMAKE_POSITION_INDEPENDENT_CODE=on
	CMAKE_PREFIX_PATH=$(CONAN_BINDIR)
endef

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/rules

#----------------------------------------------------------------------------------------------

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR)
else
	$(SHOW)$(MAKE) -C $(BINDIR) clean
endif

.PHONY: clean
