
ROOT=../..

include $(ROOT)/deps/readies/mk/main

SRCDIR=$(ROOT)/build/conan

override BINROOT:=$(ROOT)/bin/$(shell $(READIES)/bin/platform -t)
override BINDIR=$(BINROOT)/conan

include $(MK)/defs

all: $(BINDIR)/CMakePresets.json

include $(MK)/rules

$(BINDIR)/CMakePresets.json: conanfile.txt
	@if [[ ! -e .conan2/profiles/default ]]; then conan profile detect --force; fi
	@mkdir -p $(BINDIR)
	@conan install . --output-folder=$(BINDIR) --build=missing
