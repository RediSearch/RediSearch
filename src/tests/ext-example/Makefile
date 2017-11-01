#set environment variable RS_INCLUDE_DIR to the location of redismodule.h
ifndef RS_INCLUDE_DIR
	RS_INCLUDE_DIR=../../
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
CFLAGS = -I$(RS_INCLUDE_DIR) -Wall -g -fPIC -O0 -std=gnu99  
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')

# Compile flags for non-osx / osx
ifneq ($(uname_S),Darwin)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic
else
	CFLAGS += -mmacosx-version-min=10.6
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -dylib -exported_symbol _RS_ExtensionInit -macosx_version_min 10.6
endif

all: example.so

example.so: example.o
	$(LD) -o $@ example.o $(SHOBJ_LDFLAGS) $(LIBS) -lc 

clean:
	rm -rf *.xo *.so *.o

FORCE: