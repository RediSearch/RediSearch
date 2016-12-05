
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')


# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS= -fPIC -lc -lm -std=gnu99 -I./ 
RELEASEFLAGS=-O3 -g
DEBUGFLAGS=-O0 -g 
VARINT=varint.o buffer.o
INDEX=index.o forward_index.o score_index.o skip_index.o numeric_index.o
TEXT=tokenize.o stemmer.o dep/snowball/libstemmer.o
REDIS=redis_buffer.o module.o redis_index.o query.o spec.o
UTILOBJS=util/heap.o util/logging.o util/fnv.o
RMUTILOBJS=rmutil/librmutil.a
LIBTRIE=trie/libtrie.a
LIBNU=dep/libnu/libnu.a
TESTS=test.o

SRCDIR := $(shell pwd)
MODULE=$(patsubst %, $(SRCDIR)/%, $(VARINT) $(TEXT) $(INDEX) $(REDIS) $(UTILOBJS) $(RMUTILOBJS) $(LIBTRIE))

CC=gcc
.SUFFIXES: .c .so .xo .o

UTIL = util
RMUTIL = rmutil
TRIE = trie

.PHONY: rmutil $(RMUTIL) 
.PHONY: util $(UTIL)
.PHONY: trie $(TRIE)
.PHONY: tests  

# util: $(UTIL)

all: release

$(UTIL):
	$(MAKE) -C $@

$(RMUTIL):
	$(MAKE) -C $@

$(TRIE):
	$(MAKE) -C $@

tests:
	# low level redis-independant tests
	$(MAKE) -C $@ build test
	# high level python integration tests
	$(MAKE) -C pytest test

snowball:
	$(MAKE) -C dep/snowball libstemmer.o

libnu:
	$(MAKE) -C dep/libnu

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

module.so: $(MODULE)
	$(LD) -o $@ $(VARINT) $(INDEX) $(TEXT) $(REDIS) $(UTILOBJS) $(RMUTILOBJS) $(LIBTRIE) $(SHOBJ_LDFLAGS) $(LIBS) $(LIBNU) -lc -lm -Bsymbolic


release: CFLAGS += $(RELEASEFLAGS)
release: util rmutil snowball libnu trie | module.so

debug: CFLAGS += $(DEBUGFLAGS)
debug: util rmutil snowball libnu trie | module.so
	

clean:
	find . -type f -name '*.o' -delete -print

test: tests

rebuild: clean all
