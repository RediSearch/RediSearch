# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
CFLAGS ?= -W -fno-common -g -ggdb -fPIC -std=gnu99 -O3

SOURCEDIR = .
CC_SOURCES = $(wildcard $(SOURCEDIR)/*.c)
CC_OBJECTS = $(sort $(patsubst $(SOURCEDIR)/%.c, $(SOURCEDIR)/%.o, $(CC_SOURCES)))

.SUFFIXES: .c .cc .o

all: libtriemap.a


libtriemap.a: $(CC_OBJECTS)
	ar rcs $@ $^

clean:
	rm -rf *.o *.a
