# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS = -g -fPIC -lc -lm -O3 -std=gnu99 -I$(RM_INCLUDE_DIR) -Wall -Wno-unused-function
CC=gcc

OBJS=util.o strings.o sds.o vector.o heap.o priority_queue.o

all: librmutil.a

clean:
	rm -rf *.o *.a

librmutil.a: $(OBJS)
	ar rcs $@ $^

test_vector: test_vector.o vector.o
	$(CC) -Wall -o test_vector vector.o test_vector.o -lc -O0
	@(sh -c ./test_vector)

test_heap: test_heap.o heap.o vector.o
	$(CC) -Wall -o test_heap heap.o vector.o test_heap.o -lc -O0
	@(sh -c ./test_heap)

test_priority_queue: test_priority_queue.o priority_queue.o heap.o vector.o
	$(CC) -Wall -o test_priority_queue priority_queue.o heap.o vector.o test_priority_queue.o -lc -O0
	@(sh -c ./test_heap)
