
ROOT=../..

include $(ROOT)/deps/readies/mk/main

SRCDIR=$(ROOT)/build/conan

override BINROOT:=$(ROOT)/bin/$(shell $(READIES)/bin/platform -t)
override BINDIR=$(BINROOT)/conan

CONAN=python3 -m conans.conan

include $(MK)/defs

all: $(BINDIR)/CMakePresets.json

include $(MK)/rules

$(BINDIR)/CMakePresets.json: conanfile.txt
	$(SHOW)if [[ ! -e $$HOME/.conan2/profiles/default ]]; then $(CONAN) profile detect --force; fi
	$(SHOW)mkdir -p $(BINDIR)
ifeq ($(OSNICK),macos)
	$(SHOW)$(CONAN) install . --output-folder=$(BINDIR) --build=missing -s compiler.cppstd=20 -s compiler.libcxx=libc++
else
	$(SHOW)$(CONAN) install . --output-folder=$(BINDIR) --build=missing -s compiler.cppstd=20
endif
