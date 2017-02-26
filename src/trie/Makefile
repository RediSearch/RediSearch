# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS ?= -g -fPIC -O3 -std=gnu99 -Wall -Wno-unused-function -Wno-unused-variable
CFLAGS += -I$(RM_INCLUDE_DIR)
CC=gcc
OBJS=levenshtein.o rune_util.o sparse_vector.o trie.o trie_type.o

all: libtrie.a

clean:
	rm -rf *.o *.a

libtrie.a: $(OBJS)
	ar rcs $@ $^

# test_trie: test.o $(OBJS)
# 	$(CC) -Wall -o test test.o $(OBJS) -lc -O0
# 	@(sh -c ./test_vector)

	
