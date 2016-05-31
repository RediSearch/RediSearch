CFLAGS = -g -fPIC -lc -lm -std=gnu99 -I./ -O3 
#VARINT=varint.o buffer.o
#INDEX=index.o forward_index.o score_index.o skip_index.o numeric_index.o
TEXT=tokenize.o stemmer.o dep/snowball/libstemmer.o
#REDIS=redis_buffer.o module.o redis_index.o query.o spec.o
#UTILOBJS=util/heap.o util/logging.o
RMUTILOBJS=rmutil/librmutil.a
TESTS=test.o

SRCDIR := $(shell pwd)
DEPS=$(patsubst %, $(SRCDIR)/../%, $(TEXT) $(RMUTILOBJS))

CC=gcc
.SUFFIXES: .c .so .xo .o


stemmer: test_stemmer.o
	$(CC) $(CFLAGS)  -o test_stemmer test_stemmer.o  $(DEPS) -lc -lm 
	
test: stemmer
	@(sh -c ./test_stemmer)
all: test

#rebuild: clean all
