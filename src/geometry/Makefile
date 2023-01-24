

CC  = gcc
CXX = g++

CFLAGS   = -Wall -Wextra -Wpedantic -Werror -std=c2x   -O3
CXXFLAGS = -Wall -Wextra -Wpedantic -Werror -std=c++20 -O3
LDFLAGS = -g
LDLIBS = 

SRCS = $(wildcard *.cpp)
OBJS = $(SRCS:.cpp=.o)
DEPS = $(SRCS:.cpp=.d) test.d

test: test.o $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(OBJS): %.o: %.cpp
	$(COMPILE.cpp) -o $@ -MMD -MQ $@ $<

test.o: test.c geometry.in
	$(COMPILE.c) -o $@ -MMD -MQ $@ $<

clean:
	rm -rf test *.o *.d

geometry.in: generator.cc
	g++ generator.cc -lfmt -o generator.out
	./generator.out
	rm -rf generator.out

-include $(DEPS)

.PHONY: clean
