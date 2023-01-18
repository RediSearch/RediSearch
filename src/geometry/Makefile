

CC  = gcc
CXX = g++

CFLAGS   = -Wall -Wextra -Wpedantic -Werror -std=c2x   -Og -g
CXXFLAGS = -Wall -Wextra -Wpedantic -Werror -std=c++20 -Og -g
LDFLAGS = -g
LDLIBS = 

SRCS = $(wildcard *.cpp)
OBJS = $(SRCS:.cpp=.o)
DEPS = $(SRCS:.cpp=.d) test.d

test: test.o $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(OBJS): %.o: %.cpp
	$(COMPILE.cpp) -o $@ -MMD -MQ $@ $<

test.o: test.c
	$(COMPILE.c) -o $@ -MMD -MQ $@ $<

clean:
	rm -rf test *.o *.d

-include $(DEPS)

.PHONY: clean
