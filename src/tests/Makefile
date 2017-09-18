ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS = -g -O2 -fPIC -std=gnu99 -I./ 
CFLAGS += -I$(RM_INCLUDE_DIR)
%.c: %.y

.SILENT:
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

bench-decoder: bench-decoder.o
	$(CC) $(CFLAGS) -o $@ $^ $(DEPS) $(LDFLAGS)

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

qint: test_qint.o $(DEPS)
	$(CC) $(CFLAGS) -o test_qint $^ $ $(LDFLAGS)

test_qint: qint
	./test_qint
.PHONY: test_qint

blkalloc: test_blkalloc.o
	$(CC) $(CFLAGS) -o test_blkalloc $^ $(DEPS) $(LDFLAGS)

test_blkalloc: blkalloc
	./test_blkalloc
.PHONY: test_blkalloc

khtable: test_khtable.o
	$(CC) $(CFLAGS) -o test_khtable $^ $(DEPS) $(LDFLAGS)

test_khtable: khtable
	./test_khtable
.PHONY: test_khtable

test_array: array
	./test_array

array: test_array.o
	$(CC) $(CFLAGS) -o test_array $^ $(DEPS) $(LDFLAGS)

.PHONY: test_array

build: stemmer trie index range extensions query stopwords blkalloc khtable array

	
test: test_index test_stemmer test_trie	 test_range test_extensions test_query test_stopwords \
	test_qint test_blkalloc test_khtable test_array

all: build test bench-decoder

clean:
	-rm -f *.o
.PHONY: clean

rebuild: clean all
