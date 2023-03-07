
ROOT=../..

include $(ROOT)/deps/readies/mk/main

SRCDIR=$(ROOT)/build/conan

BINDIR=$(BINROOT)/conan

include $(MK)/defs

all: $(BINROOT)/conan/CMakePresets.json

include $(MK)/rules

$(BINROOT)/conan/CMakePresets.json: conanfile.txt
	@if [[ ! -e .conan2/profiles/default ]]; then conan profile detect --force; fi
	@mkdir -p $(BINROOT)/conan
	@conan install . --output-folder=$(BINROOT)/conan --build=missing
