# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
CFLAGS ?= -fno-common -g -ggdb -fPIC -std=gnu99 -O3
#LDFLAGS = -L../ -ltriemap
SOURCEDIR = .
CC_SOURCES = $(SOURCEDIR)/crc16.c
CC_OBJECTS = $(patsubst $(SOURCEDIR)/%.c, $(SOURCEDIR)/%.o, $(CC_SOURCES))

.SUFFIXES: .c .cc .o


benchmark: $(CC_SOURCES) benchmark.c ../triemap.c
	$(CC) $(CFLAGS) -o benchmark $^ -lprofiler
.PHONY = benchmark

test: $(CC_SOURCES) test.c
	$(CC) $(CFLAGS) -o test $^ $(LDFLAGS)



clean:
	rm -rf *.o *.a benchmark test
