ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

.SILENT:

CFLAGS = -g -O2 -fPIC -std=gnu99 -I./ 
CFLAGS += -I$(RM_INCLUDE_DIR)

# Sources
SOURCEDIR=..
CC_SOURCES = $(wildcard $(SOURCEDIR)/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/query_parser/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/ext/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/util/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/trie/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/dep/thpool/*.c) 

# Convert all sources to .o files
DEP_OBJECTS = $(patsubst %.c, %.o, $(CC_SOURCES) )

# Library dependencies
LIBRMUTIL=../rmutil/librmutil.a
LIBTRIE=../trie/libtrie.a
LIBTRIEMAP=../dep/triemap/libtriemap.a
LIBNU=../dep/libnu/libnu.a
LIBSTEMMER=../dep/snowball/libstemmer.o

DEP_LIBS = $(LIBRMUTIL) $(LIBTRIE) $(LIBTRIEMAP) $(LIBNU) $(LIBSTEMMER)
DEPS = $(DEP_OBJECTS) $(DEP_LIBS)
SRCDIR := $(shell pwd)
#DEPS := $(patsubst %, ../%, $(TEXT)  $(VARINT) $(INDEX) $(UTILOBJS) $(RMUTILOBJS) $(QUERY_PARSE) $(REDIS) $(LIBTRIEMAP) $(LIBTRIE) $(LIBNU) )
LDFLAGS :=  -lc -lm -ldl -lpthread

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

test_index: index	
	@(sh -c ./test_index)
.PHONY: test_index

range: test_range.o
	$(CC) $(CFLAGS)  -o test_range test_range.o $ $(DEPS) $(LDFLAGS)

test_range: range
	@(sh -c ./test_range)
.PHONY: test_range

stopwords: test_stopwords.o
	$(CC) $(CFLAGS)  -o test_stopwords  $^ $(DEPS) $(LDFLAGS)

test_stopwords: stopwords
	@(sh -c ./test_stopwords)
.PHONY: test_stopwords


build: stemmer trie index range extensions query stopwords

	
test: test_index test_stemmer test_trie	 test_range test_extensions test_query test_stopwords

all: build test

clean:
	-rm -f *.o
.PHONY: clean

rebuild: clean all
