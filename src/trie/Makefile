# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS = -g -fPIC -lc -lm -O3 -std=gnu99 -I$(RM_INCLUDE_DIR) -Wall -Werror -Wno-unused-function -Wno-unused-variable
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

	
