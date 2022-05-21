CFLAGS = -g -O3 -std=gnu99 -I/usr/local/include -Wall -Wno-unused-function
LDFLAGS= -L/usr/local/lib -lhiredis -lev -lc -lm -static
CC=gcc

benchmark: benchmark.o
	$(CC) -o ./benchmark benchmark.o $(LDFLAGS)

all: benchmark
