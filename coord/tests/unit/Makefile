#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR:=$(abspath ../src)
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR:=$(abspath ../src/dep/rmutil)
endif

DEP_DIR := $(abspath ../src/dep)

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
CFLAGS := -I"$(RM_INCLUDE_DIR)" -I"$(DEP_DIR)" -Wall -g -fPIC -O1 -std=gnu99  


# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
SOURCEDIR=$(abspath ../)
CC_SOURCES = ../src/search_cluster.c  ../src/fnv32.c ../src/partition.c $(wildcard ../src/dep/rmr/redise_parser/*.c) 
CC_OBJECTS = $(patsubst %.c, %.o, $(CC_SOURCES))

LIBS= $(abspath ../src/dep/rmr/librmr.a ../src/dep/rmr/hiredis/libhiredis.a  ../src/libuv.a ../src/dep/rmutil/librmutil.a ../src/dep/triemap/libtriemap.a )

test_searchcluster:
	$(CC)  -o ./$@ $@.c $(CFLAGS) $(CC_OBJECTS) $(LIBS) -lc -lpthread -lm
	./$@
.PHONY: test_searchcluster

all: test_searchcluster

clean:
	rm -rf *.xo *.so *.o

