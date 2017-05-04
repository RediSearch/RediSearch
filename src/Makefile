
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

CFLAGS= -Wall -Wno-unused-function -Wno-unused-variable -Wno-unused-result -Wno-pointer-sign -fPIC \
 	-D_GNU_SOURCE -std=gnu99 -I"$(shell pwd)" -DREDIS_MODULE_TARGET
# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions -ldl
else
	CFLAGS += -mmacosx-version-min=10.6
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb 
	SHOBJ_LDFLAGS ?= -macosx_version_min 10.6 -bundle -undefined dynamic_lookup -ldl
endif
export CFLAGS

RELEASEFLAGS=-O3 -g
DEBUGFLAGS=-O0 -g 
VARINT=varint.o buffer.o util/qint.o
INDEX=index.o forward_index.o index_result.o doc_table.o numeric_filter.o \
		numeric_index.o geo_index.o inverted_index.o id_list.o id_filter.o offset_vector.o sortable.o
TEXT=tokenize.o stemmer.o dep/snowball/libstemmer.o
EXTOBJS=ext/default.o  extension.o
REDIS= redis_index.o query.o spec.o  module.o
QUERY_PARSE=query_parser/lexer.o query_parser/parser.o
UTILOBJS=util/heap.o util/logging.o util/fnv.o util/mempool.o
RMUTILOBJS=rmutil/librmutil.a
LIBTRIE=trie/libtrie.a
LIBTRIEMAP=dep/triemap/libtriemap.a
LIBNU=dep/libnu/libnu.a
TESTS=test.o


MODULE=$(VARINT) $(TEXT) $(INDEX) $(REDIS) $(UTILOBJS) $(RMUTILOBJS) \
	$(EXTOBJS) $(QUERY_PARSE) $(LIBTRIE) $(LIBTRIEMAP)
%.c: %.y
	
YACC=lemon
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
export CC
export YACC
.SUFFIXES: .c .so .xo .o

UTIL = util
RMUTIL = rmutil
TRIE = trie
TRIEMAP = triemap

.PHONY: rmutil $(RMUTIL) 
.PHONY: util $(UTIL)
.PHONY: trie $(TRIE)
.PHONY: tests  
.PHONY: triemap ($TRIEMAP)

# util: $(UTIL)

all: release

$(UTIL):
	$(MAKE) -C $@

$(RMUTIL):
	$(MAKE) -C $@

$(TRIE):
	$(MAKE) -C $@

$(TRIEMAP):
	$(MAKE) -C dep/triemap

query_parser:
	$(MAKE) -C $@
	
.PHONY: query_parser

test:
	# low level redis-independant tests
	$(MAKE) -C ./tests clean build test
	# high level python integration tests
	$(MAKE) -C pytest test
	# Triemap tests
	$(MAKE) -C dep/triemap/test test

snowball:
	$(MAKE) -C dep/snowball libstemmer.o

libnu:
	$(MAKE) -C dep/libnu

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

module.so: $(MODULE)
	$(LD) -o $@ $(MODULE) $(LIBNU) $(LIBTRIE) $(SHOBJ_LDFLAGS) -lc -lm


release: CFLAGS += $(RELEASEFLAGS)
release: util rmutil snowball libnu trie triemap | module.so

debug: CFLAGS += $(DEBUGFLAGS)
debug: util rmutil snowball libnu trie  triemap | module.so
	

clean:
	find . -type f -name '*.o' -delete -print


rebuild: clean all
