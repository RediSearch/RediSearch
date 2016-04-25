
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS = -g -fPIC -lc -lm -Og -std=gnu99 -Wall -pedantic

VARINT=varint.o buffer.o
INDEX=index.o tokenize.o forward_index.o
REDIS=redis_buffer.o mdma.o redis_index.o query.o
UTILOBJS=util/pqueue.o util/list.o 
TESTS=test_varint.o
CC=gcc
.SUFFIXES: .c .so .xo .o

UTIL = util

.PHONY: util $(UTIL)

# util: $(UTIL)

$(UTIL):
	$(MAKE) -C $@


all: util mdma.so 

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

#mdma.xo: ./index.h

mdma.so: $(VARINT) $(INDEX) $(REDIS) $(UTILOBJS)
	$(LD) -o $@ $(VARINT) $(INDEX) $(REDIS) $(UTILOBJS) $(SHOBJ_LDFLAGS) $(LIBS) -lc -lm 

clean:
	rm -rf *.xo *.so *.o

test: $(VARINT) $(INDEX) $(TESTS) $(UTILOBJS)
	$(CC) $(CFLAGS)  -o testung  $(INDEX) $(VARINT) $(TESTS) $(LIBS) -lc -lm 
	@(sh -c ./testung)
