
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
CFLAGS = -g -fPIC -lc -lm -Og -std=gnu99

VARINT=varint.o buffer.o
INDEX=index.o tokenize.o forward_index.o
REDIS=redis_buffer.o mdma.o redis_index.o 
TESTS=test_varint.o
CC=gcc
.SUFFIXES: .c .so .xo .o

all: mdma.so

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

#mdma.xo: ./index.h

mdma.so: $(VARINT) $(INDEX) $(REDIS)
	$(LD) -o $@ $(VARINT) $(INDEX) $(REDIS) $(SHOBJ_LDFLAGS) $(LIBS) -lc -lm

clean:
	rm -rf *.xo *.so *.o

test: $(VARINT) $(INDEX) $(TESTS)
	$(CC) $(CFLAGS)  -o testung  $(INDEX) $(VARINT) $(TESTS) $(LIBS) -lc -lm 
	@(sh -c ./testung)
