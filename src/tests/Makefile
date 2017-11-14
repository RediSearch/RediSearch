ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS = -g -O3 -fPIC -std=gnu99 -I./ 
CFLAGS += -I$(RM_INCLUDE_DIR)
%.c: %.y

ifndef VERBOSE
.SILENT:
endif

# Sources
SOURCEDIR=$(abspath $(shell pwd -P)/../)
CC_SOURCES = $(wildcard $(SOURCEDIR)/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/query_parser/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/ext/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/util/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/trie/*.c) 
CC_SOURCES += $(wildcard $(SOURCEDIR)/dep/thpool/*.c) 

# Convert all sources to .o files
DEP_OBJECTS = $(patsubst %.c, %.o, $(CC_SOURCES) )
CC_DEPS = $(patsubst %.c, %.d, $(CC_SOURCES) )


TEST_SOURCES = $(abspath $(wildcard test_*.c))
TEST_OBJECTS = $(patsubst %.c, %.o, $(TEST_SOURCES)  )
TEST_EXECUTABLES = $(patsubst %.c, %.run, $(TEST_SOURCES)  )
TEST_DEPS = $(patsubst %.c, %.d,$(TEST_SOURCES))

# Library dependencies
DEP_LIBS = ../rmutil/librmutil.a ../trie/libtrie.a ../dep/triemap/libtriemap.a \
		   ../dep/libnu/libnu.a ../dep/snowball/libstemmer.o ../dep/friso/libfriso.a \
		   ../../cndict/generated/libcndict_data.a ../dep/miniz/libminiz.a
DEPS = $(DEP_OBJECTS) $(DEP_LIBS)
SRCDIR := $(shell pwd)
LDFLAGS :=  -lc -lm -ldl -lpthread

CC=gcc

%.o: %.c
%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) $(TARGET_ARCH) -c $< -o $@ -MMD -MF $(@:.o=.d)


all: build test

# Including this stuff makes sure everything gets built on changes to the source directory
-include $(TEST_DEPS)
-include $(CC_DEPS)

# The exampe extension is a dependency to extension tests
ext_example:
	$(MAKE) -C ./ext-example clean all
.PHONY: ext_example

# Compiling each test runner from its .o file
%.run: %.o
	$(CC) $(CFLAGS) -o $@ $^  $(DEPS) $(LDFLAGS)
# extra rule for test_extensions.run
test_extensions.run: | ext_example


build: $(TEST_OBJECTS) $(TEST_EXECUTABLES) $(DEPS) ext_example

# Test all
test: build $(TEST_EXECUTABLES)
	set -e; \
	for t in test_*.run;\
	 do ./$$t;\
	done

memcheck: build $(TEST_EXECUTABLES)
	set -e; \
	for t in test_*.run;\
	 do valgrind --tool=memcheck --leak-check=full --error-exitcode=1 --show-possibly-lost=no ./$$t;\
	done

# Target for individual tests - make run:{test name}, e.g. "make run:query" will run test_query.run
run\:%: 
	$(MAKE) test_$*.run
	./test_$*.run

clean:
	-rm -f *.o
	-rm -f ext-example/*.o ext-example/*.so 

.PHONY: clean

rebuild: clean all
