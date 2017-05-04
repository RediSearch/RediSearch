ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS = -g -O2 -fPIC -std=gnu99 -I./ 
CFLAGS += -I$(RM_INCLUDE_DIR) 
VARINT=varint.o buffer.o
INDEX=index.o forward_index.o inverted_index.o numeric_index.o index_result.o doc_table.o\
		numeric_filter.o geo_index.o id_list.o id_filter.o offset_vector.o extension.o sortable.o
TEXT=tokenize.o stemmer.o dep/snowball/libstemmer.o 
REDIS=redis_index.o query.o spec.o 
UTILOBJS=util/heap.o util/logging.o util/fnv.o util/qint.o util/mempool.o
RMUTILOBJS=rmutil/vector.o rmutil/util.o rmutil/strings.o rmutil/sds.o rmutil/alloc.o
QUERY_PARSE=query_parser/lexer.o query_parser/parser.o 
LIBTRIE=trie/libtrie.a
LIBTRIEMAP=dep/triemap/libtriemap.a
LIBNU=dep/libnu/libnu.a
TESTS=test.o

SRCDIR := $(shell pwd)
DEPS := $(patsubst %, ../%, $(TEXT)  $(VARINT) $(INDEX) $(UTILOBJS) $(RMUTILOBJS) $(QUERY_PARSE) $(REDIS) $(LIBTRIEMAP) $(LIBTRIE) $(LIBNU) )
LDFLAGS :=  -lc -lm -ldl

CC=gcc
.SUFFIXES: .c .so .xo .o

ext_example:
	$(MAKE) -C ./ext-example clean all
.PHONY: ext_example


stemmer: test_stemmer.o
	$(CC) $(CFLAGS)  -o test_stemmer test_stemmer.o  $(DEPS) $(LDFLAGS)
test_stemmer: 
	@(sh -c ./test_stemmer)
.PHONY: test_stemmer

extensions: ext_example test_extensions.o
	$(CC) $(CFLAGS)  -o test_extensions test_extensions.o  $(DEPS) $(LDFLAGS)
test_extensions:
	@(sh -c ./test_extensions)
.PHONY: test_extensions

trie: test_trie.o
	$(CC) $(CFLAGS)  -o test_trie test_trie.o $ $(DEPS) $(LDFLAGS)
test_trie: 	 
	@(sh -c ./test_trie)
.PHONY: test_trie

query: test_query.o
	$(CC) $(CFLAGS) -o test_query test_query.o $ $(DEPS) $(LDFLAGS)

test_query: query 	 
	@(sh -c ./test_query)
	
.PHONY: test_query

index: test_index.o
	$(CC) $(CFLAGS)  -o test_index test_index.o $ $(DEPS) $(LDFLAGS)

test_index: 	
	@(sh -c ./test_index)
.PHONY: test_index

range: test_range.o
	$(CC) $(CFLAGS)  -o test_range test_range.o $ $(DEPS) $(LDFLAGS)

test_range: range
	@(sh -c ./test_range)
.PHONY: test_range

build: stemmer trie index range extensions query

	
test: test_index test_stemmer test_trie	 test_range test_extensions test_query

all: build test

clean:
	-rm -f *.o
.PHONY: clean

rebuild: clean all
