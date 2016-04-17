
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
CFLAGS =  -g -O3     -fPIC

VARINT=varint.o
INDEX=index.o
TESTS=test_varint.o
CC=gcc
.SUFFIXES: .c .so .xo .o

all: mdma.so

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

#mdma.xo: ./index.h

mdma.so: $(INDEX) $(VARINT)
	$(LD) -o $@ $< $(SHOBJ_LDFLAGS) $(LIBS) -lc

clean:
	rm -rf *.xo *.so *.o

test: $(VARINT) $(INDEX) $(TESTS)
	$(CC) $(CFLAGS) -o testung  $(INDEX) $(VARINT) $(TESTS) $(LIBS) -lc 
	@(sh -c ./testung)
