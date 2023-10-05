
ROOT=../..

include $(ROOT)/deps/readies/mk/main

SRCDIR=$(ROOT)/build/conan

override BINROOT:=$(ROOT)/bin/$(shell $(READIES)/bin/platform -t)
override BINDIR=$(BINROOT)/conan

ifneq ($(shell command -v conan),)
CONAN=conan
else
CONAN=python3 -m conans.conan
endif

include $(MK)/defs

MK_CUSTOM_CLEAN=1

ifneq ($(wildcard conanfile.txt.$(OSNICK)),)
CONANFILE=conanfile.txt.$(OSNICK)
else
CONANFILE=conanfile.txt
endif

all: $(BINDIR)/CMakePresets.json

include $(MK)/rules

CONAN_INSTLL_ARGS.macos=-s compiler.libcxx=libc++

$(BINDIR)/CMakePresets.json: conanfile.txt
ifeq ($(FORCE),1)
	$(SHOW)$(CONAN) profile detect --force
else
	$(SHOW)if [[ ! -e $$HOME/.conan2/profiles/default ]]; then $(CONAN) profile detect --force; fi
endif
	$(SHOW)mkdir -p $(BINDIR)
	$(SHOW)$(CONAN) install  --output-folder=$(BINDIR) --build=missing -s compiler.cppstd=20 $(CONAN_INSTALL_ARGS.$(OS)) $(CONANFILE)

clean:
	$(SHOW)rm -rf $(BINDIR)
ifeq ($(ALL),1)
	$(SHOW)rm -rf $$HOME/.conan2
endif

.PHONY: clean
