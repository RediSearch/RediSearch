
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

CFLAGS= -Wall -Werror -Wno-unused-function -Wno-unused-variable -Wno-pointer-sign -fPIC  -std=gnu99 -I./
# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions
else
	CFLAGS += -mmacosx-version-min=10.6
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -macosx_version_min 10.6 -bundle -undefined dynamic_lookup 
endif
export CFLAGS

RELEASEFLAGS=-O3 -g
DEBUGFLAGS=-O0 -g 
VARINT=varint.o buffer.o
INDEX=index.o forward_index.o score_index.o skip_index.o index_result.o expander.o doc_table.o numeric_filter.o numeric_index.o geo_index.o
TEXT=tokenize.o stemmer.o dep/snowball/libstemmer.o
REDIS=redis_buffer.o module.o redis_index.o query.o spec.o
QUERY_PARSE=query_parser/tokenizer.o query_parser/parser.o query_parser/parse.o
UTILOBJS=util/heap.o util/logging.o util/fnv.o util/triemap.o
RMUTILOBJS=rmutil/librmutil.a
LIBTRIE=trie/libtrie.a
LIBNU=dep/libnu/libnu.a
TESTS=test.o

SRCDIR := $(shell pwd)
#MODULE=$(patsubst %, $(SRCDIR)/%, $(VARINT) $(TEXT) $(INDEX) $(REDIS) $(UTILOBJS) $(RMUTILOBJS))

MODULE=$(patsubst %, $(SRCDIR)/%, $(VARINT) $(TEXT) $(INDEX) $(REDIS) $(UTILOBJS) $(RMUTILOBJS) $(QUERY_PARSE) $(LIBTRIE))

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

query_parser:
	$(MAKE) -C $@
	
.PHONY: query_parser

test:
	# low level redis-independant tests
	$(MAKE) -C tests build test
	# high level python integration tests
	$(MAKE) -C pytest test

snowball:
	$(MAKE) -C dep/snowball libstemmer.o

libnu:
	$(MAKE) -C dep/libnu

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

module.so: $(MODULE)
	$(LD) -o $@ $(VARINT) $(INDEX) $(TEXT) $(REDIS) $(UTILOBJS) $(RMUTILOBJS) $(LIBTRIE) \
		$(QUERY_PARSE) $(SHOBJ_LDFLAGS) $(LIBS) $(LIBNU) -lc -lm


release: CFLAGS += $(RELEASEFLAGS)
release: util rmutil snowball libnu trie | module.so

debug: CFLAGS += $(DEBUGFLAGS)
debug: util rmutil snowball libnu trie  | module.so
	

clean:
	find . -type f -name '*.o' -delete -print

test: tests

rebuild: clean all
