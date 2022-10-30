# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
CFLAGS ?= -fno-common -g -ggdb -fPIC -std=gnu99 -O3 
LDFLAGS ?= -lm
SOURCEDIR = .
CC_SOURCES = $(SOURCEDIR)/crc16.c
CC_OBJECTS = $(sort $(patsubst $(SOURCEDIR)/%.c, $(SOURCEDIR)/%.o, $(CC_SOURCES)))

.SUFFIXES: .c .cc .o

all: test test_valgrind

benchmark: $(CC_SOURCES) benchmark.c ../triemap.c
	$(CC) $(CFLAGS) -o benchmark $^  $(LDFLAGS)
.PHONY = benchmark

build_test: $(CC_SOURCES) test.c ../triemap.c
	$(CC) $(CFLAGS) -o test $^  $(LDFLAGS)

.PHONY: test
test: build_test
	$(sh) ./test

test_valgrind: build_test

	valgrind --error-exitcode=1 --leak-resolution=low --quiet \
            --leak-check=full --show-possibly-lost=no ./test



clean:
	rm -rf *.o *.a benchmark test


