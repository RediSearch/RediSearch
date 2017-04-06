#set environment variable RS_INCLUDE_DIR to the location of redismodule.h
ifndef RS_INCLUDE_DIR
	RS_INCLUDE_DIR=../
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
CFLAGS = -I$(RS_INCLUDE_DIR) -Wall -g -fPIC -lc -lm -Og -std=gnu99  
CC=gcc

all: example.so

example.so: example.o
	$(LD) -o $@ example.o $(SHOBJ_LDFLAGS) $(LIBS) -lc 

clean:
	rm -rf *.xo *.so *.o

FORCE: