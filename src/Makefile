
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
export CC

# if DEBUG env var is set, we compile with "debug" cflags
DEBUGFLAGS = -g -ggdb -O3 
ifeq ($(DEBUG), 1)
	DEBUGFLAGS = -g -ggdb -O0
endif

# Default CFLAGS
CFLAGS= -Wall -Wno-unused-function -Wno-unused-variable -Wno-unused-result -fPIC \
 	-D_GNU_SOURCE -std=gnu99 -I"$(shell pwd)" -DREDIS_MODULE_TARGET \
	-DREDISMODULE_EXPERIMENTAL_API 
CFLAGS += $(DEBUGFLAGS)

# Compile flags for non-osx / osx
ifneq ($(uname_S),Darwin)
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions -ldl -lpthread
else
	CFLAGS += -mmacosx-version-min=10.6
	SHOBJ_LDFLAGS ?= -macosx_version_min 10.6 -exported_symbol _RedisModule_OnLoad -bundle -undefined dynamic_lookup -ldl -lpthread
endif
export CFLAGS



# Sources
SOURCEDIR:=$(shell pwd -P)
CC_SOURCES := $(wildcard $(SOURCEDIR)/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/query_parser/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/ext/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/util/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/trie/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/dep/thpool/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/dep/cndict/*.c)

# Convert all sources to .o files
CC_OBJECTS = $(patsubst %.c, %.o, $(CC_SOURCES) )

# .d files for each c file. These make sure that changing a header file
# will also change the dependent .c files of it
CC_DEPS = $(patsubst %.c, %.d, $(CC_SOURCES) )

MODULE_ARTIFACT=redisearch.so

# Library dependencies
LIBRMUTIL=rmutil/librmutil.a
LIBTRIE=trie/libtrie.a
LIBTRIEMAP=dep/triemap/libtriemap.a
LIBNU=dep/libnu/libnu.a
LIBSTEMMER=dep/snowball/libstemmer.o
LIBFRISO=dep/friso/libfriso.a
LIBMINIZ=dep/miniz/libminiz.a

# Compilation deps for the module
LIBS=$(LIBTRIE) $(LIBTRIEMAP) $(LIBRMUTIL) $(LIBNU) $(LIBFRISO) $(LIBSTEMMER)
# TODO: make conditional on chinese support
LIBS += $(LIBMINIZ)
MODULE=$(CC_OBJECTS) $(LIBS)

%.c: %.y

# Compile C file while generating a .d file for it
%.o: %.c
%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) $(TARGET_ARCH) -c $< -o $@ -MMD -MF $(@:.o=.d)

all: $(MODULE_ARTIFACT)


# Include all dependency files for C files
-include $(CC_DEPS)

# Library compile rules
$(LIBRMUTIL):
	$(MAKE) -C rmutil
.PHONY: $(LIBRMUTIL)
$(LIBTRIE):
	$(MAKE) -C trie
.PHONY: $(LIBTRIE)
$(LIBTRIEMAP):
	$(MAKE) -C dep/triemap
.PHONY: $(LIBTRIEMAP)
$(LIBSTEMMER):
	$(MAKE) -C dep/snowball libstemmer.o
.PHONY: $(LIBSTEMMER)
$(LIBNU):
	$(MAKE) -C dep/libnu
.PHONY: $(LIBNU)
$(LIBFRISO):
	$(MAKE) -C dep/friso -f Makefile.RediSearch
$(LIBMINIZ):
	$(MAKE) -C dep/miniz

# Compile query parse .y and .rl files. 
# This is not included in the usual make target!
query_parser:
	$(MAKE) -C $@
.PHONY: query_parser

build_cndict:
	$(MAKE) -C dep/cndict

build_tests: $(MODULE_ARTIFACT) libredisearch.a
	$(MAKE) -C tests build

test: $(MODULE) build_tests
	# low level redis-independant tests
	$(MAKE) -C ./tests test
	# high level python integration tests
	$(MAKE) -C pytest test
	# Triemap tests
	$(MAKE) -C dep/triemap/test test

buildall: $(MODULE_ARTIFACT) libredisearch.a build_tests

# Build the module...
$(MODULE_ARTIFACT): $(shell rm -f module.o)
$(MODULE_ARTIFACT): $(MODULE) version.h
	@# Just to make sure old versions of the modules are deleted
	rm -f module.so
	$(LD) -o $@ $(MODULE) $(SHOBJ_LDFLAGS) -lc -lm

# Build a stand-alone static library without the module entry point.
# This is used to include the module's functionality in other modules
libredisearch.a: CFLAGS+=-DRS_CMD_PREFIX='"_FT"' -DRS_STATIC
# remove module.o to make sure we are building it with the right defines
libredisearch.a: $(shell rm -f module.o)
libredisearch.a: $(CC_OBJECTS) $(LIBS) version.h
	
	ar rcs $@ $(CC_OBJECTS)

staticlib: libredisearch.a

clean:
	rm -fv *.[oad] trie/*.[oad] query_parser/*.[oad] ext/*.[oad]
	$(MAKE) -C tests clean

distclean: clean
	find . -type f \( -name '*.[oad]' -o -name '*.so' \) -delete -print

print_version: version.h print_version.c
	@$(CC) -o $@ -DPRINT_VERSION_TARGET $@.c

rebuild: clean all
