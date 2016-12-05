# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -W -Wall -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_CPPFLAGS ?= -W -Wall -fno-common -g -ggdb
else
	SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_CPPFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -O2
endif

SOURCEDIR = .
CC_SOURCES = $(wildcard $(SOURCEDIR)/*.c)
CC_OBJECTS = $(patsubst $(SOURCEDIR)/%.c, $(SOURCEDIR)/%.o, $(CC_SOURCES))

.SUFFIXES: .c .cc .o

all: libnu.a

# $(SOURCEDIR)/%.o: $(SOURCEDIR)/%.c
# 	$(CC) -I. $(SHOBJ_CFLAGS) -fPIC -fpermissive -c $< -o $@

# test1.xo: ../redismodule.h

libnu.a: $(CC_OBJECTS)
	ar rcs $@ $^

clean:
	rm -rf *.xo *.so *.o *.a
