
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
CFLAGS = -g -fPIC -lc -lm -std=gnu99 -I./ -O3 
VARINT=varint.o buffer.o
INDEX=index.o tokenize.o forward_index.o score_index.o
REDIS=redis_buffer.o module.o redis_index.o query.o spec.o
UTILOBJS=util/heap.o util/logging.o
RMUTILOBJS=rmutil/librmutil.a
TESTS=test.o

SRCDIR := $(shell pwd)
MODULE=$(patsubst %, $(SRCDIR)/%, $(VARINT) $(INDEX) $(REDIS) $(UTILOBJS) $(RMUTILOBJS))

CC=gcc
.SUFFIXES: .c .so .xo .o

UTIL = util

RMUTIL = rmutil

.PHONY: rmutil $(RMUTIL) 

.PHONY: util $(UTIL)

# util: $(UTIL)

all: util rmutil module.so 

$(UTIL):
	$(MAKE) -C $@

$(RMUTIL):
	$(MAKE) -C $@

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

module.so: $(MODULE)
	$(LD) -o $@ $(VARINT) $(INDEX) $(REDIS) $(UTILOBJS) $(RMUTILOBJS) $(SHOBJ_LDFLAGS) $(LIBS) -lc -lm -Bsymbolic

clean:
	find . -type f -name '*.o' -delete -print

test: $(VARINT) $(INDEX) $(TESTS) $(UTILOBJS) $(REDIS) $(RMUTILOBJS)
	$(CC) $(CFLAGS)  -o testung  $(INDEX) $(VARINT) $(TESTS) $(REDIS) $(UTILOBJS) $(RMUTILOBJS) $(LIBS) -lc -lm 
	@(sh -c ./testung)

rebuild: clean all
