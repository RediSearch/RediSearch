#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=./
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=../rmutil
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -g -lc -lm -O3 -g -std=gnu99 -fPIC  
CC=gcc
LDFLAGS=-lc -lm -Bsymbolic


OBJS=sparse_vector.o trie.o levenshtein.o

all: module.so 

build_test: $(OBJS) test.o
	$(CC) $(CFLAGS) -o ./test $(OBJS) ./rmutil/vector.o test.o -lc

clean:
	rm -rf *.xo *.so *.o

test: build_test
	@(sh -c ./test)

SRCDIR := $(shell pwd)
MODULE=$(patsubst %, $(SRCDIR)/%, $(OBJS) trie_type.o ./rmutil/vector.o)

module.so: $(MODULE)
	 $(LD) -o $@ $(MODULE) $(SHOBJ_LDFLAGS) $(LDFLAGS)
