

CC  = gcc
CXX = g++

CPPFLAGS = -I../../deps -I.. -I../../deps/VectorSimilarity/src -DREDISMODULE_EXPERIMENTAL_API
CFLAGS   = -Wall -Wextra -Wconversion -Wshadow -Werror -std=c2x   -O2
CXXFLAGS = -std=c++20 -O2
LDFLAGS = -g
LDLIBS = 

ifneq ($(BOOST_ROOT),)
INC = -I$(BOOST_ROOT)
endif

SRCS = $(wildcard *.cpp)
OBJS = $(SRCS:.cpp=.o)
DEPS = $(SRCS:.cpp=.d) test.d

test: test.o $(OBJS)
	$(CXX) $(LDFLAGS) $(INC)-o $@ $^ $(LDLIBS)

$(OBJS): %.o: %.cpp
	$(COMPILE.cpp) $(INC) -o $@ -MMD -MQ $@ $<

test.o: test.c geometry.in
	$(COMPILE.c) $(INC) -o $@ -MMD -MQ $@ $<

clean:
	rm -rf test *.o *.d *.in

geometry.in: generator.cc
	g++ generator.cc -lfmt -o generator.out
	./generator.out
	rm -rf generator.out

-include $(DEPS)

.PHONY: clean
