ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS ?= -g -O0 -fPIC -std=gnu99 -I./ 
CFLAGS += -I$(RM_INCLUDE_DIR) 
VARINT=varint.o buffer.o
INDEX=index.o forward_index.o inverted_index.o numeric_index.o index_result.o doc_table.o\
		numeric_filter.o geo_index.o id_list.o id_filter.o offset_vector.o extension.o 
TEXT=tokenize.o stemmer.o dep/snowball/libstemmer.o 
REDIS=redis_index.o query.o spec.o 
UTILOBJS=util/heap.o util/logging.o util/fnv.o
RMUTILOBJS=rmutil/vector.o rmutil/util.o rmutil/strings.o rmutil/sds.o rmutil/alloc.o
QUERY_PARSE=query_parser/tokenizer.o query_parser/parser.o query_parser/parse.o
LIBTRIE=trie/libtrie.a
LIBTRIEMAP=dep/triemap/libtriemap.a
LIBNU=dep/libnu/libnu.a
TESTS=test.o

SRCDIR := $(shell pwd)
DEPS := $(patsubst %, $(SRCDIR)/../%, $(TEXT) $(LIBTRIE) $(LIBNU) $(LIBTRIEMAP) $(VARINT) $(INDEX) $(UTILOBJS) $(RMUTILOBJS) $(QUERY_PARSE) $(REDIS) )


CC=gcc
.SUFFIXES: .c .so .xo .o


stemmer: test_stemmer.o
	$(CC) $(CFLAGS)  -o test_stemmer test_stemmer.o  $(DEPS) -lc -lm
test_stemmer: 
	@(sh -c ./test_stemmer)
.PHONY: test_stemmer

extensions: test_extensions.o
	$(CC) $(CFLAGS)  -o test_extensions test_extensions.o  $(DEPS) -lc -lm
test_extensions:
	@(sh -c ./test_extensions)
.PHONY: test_extensions

trie: test_trie.o
	$(CC) $(CFLAGS)  -o test_trie test_trie.o $ $(DEPS) -lc -lm
test_trie: 	 
	@(sh -c ./test_trie)
.PHONY: test_trie

query: test_query.o
	$(CC) $(CFLAGS)  -o test_query test_query.o $ $(DEPS) -lc -lm

test_query: query 	 
	@(sh -c ./test_query)
	
.PHONY: test_query

index: test_index.o
	$(CC) $(CFLAGS)  -o test_index test_index.o $ $(DEPS) -lc -lm

test_index: 	
	@(sh -c ./test_index)
.PHONY: test_index

range: test_range.o
	$(CC) $(CFLAGS)  -o test_range test_range.o $ $(DEPS) -lc -lm

test_range: range
	@(sh -c ./test_range)
.PHONY: test_range

build: stemmer trie index range

	
test: test_index test_stemmer test_trie	 test_range

all: build test

clean:
	-rm -f *.o
.PHONY: clean

rebuild: clean all
