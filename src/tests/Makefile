CFLAGS = -g -fPIC -lc -lm -std=gnu99 -I./ -O3 
VARINT=varint.o buffer.o
INDEX=index.o forward_index.o score_index.o skip_index.o numeric_index.o  
TEXT=tokenize.o stemmer.o dep/snowball/libstemmer.o
REDIS=redis_buffer.o redis_index.o query.o spec.o
UTILOBJS=util/heap.o util/logging.o util/fnv.o
RMUTILOBJS=rmutil/vector.o rmutil/util.o rmutil/strings.o rmutil/sds.o
LIBTRIE=trie/libtrie.a
TESTS=test.o

SRCDIR := $(shell pwd)
DEPS := $(patsubst %, $(SRCDIR)/../%, $(TEXT) $(LIBTRIE) $(VARINT) $(INDEX) $(UTILOBJS) $(RMUTILOBJS)  $(REDIS) )

CC=gcc
.SUFFIXES: .c .so .xo .o


stemmer: test_stemmer.o
	$(CC) $(CFLAGS)  -o test_stemmer test_stemmer.o  $(DEPS) -lc -lm 
test_stemmer: 
	@(sh -c ./test_stemmer)
.PHONY: test_stemmer

trie: test_trie.o
	$(CC) $(CFLAGS)  -o test_trie test_trie.o $ $(DEPS) -lc -lm
test_trie: 	 
	@(sh -c ./test_trie)
.PHONY: test_trie

index: test_index.o
	$(CC) $(CFLAGS)  -o test_index test_index.o $ $(DEPS) -lc -lm

test_index: 	
	@(sh -c ./test_index)
.PHONY: test_index

build: stemmer trie index

	
test: test_index test_stemmer test_trie	

all: build test

rebuild: clean all
