
# Quick Start Guide for RediSearch

## Running with Docker

```sh
docker run -p 6379:6379 redislabs/redisearch:latest
```

## Building and running from source

RediSearch uses [CMake](https://cmake.org/) as its build system. CMake is
available for almost every available platform. You can obtain cmake through
your operating system's package manager. RediSearch requires CMake version
3 or greater. If your package repository does not contain CMake3, you can
download a precompiled binary from [CMake downloads](https://cmake.org/download/).

To build using CMake:

```sh
git clone https://github.com/RediSearch/RediSearch.git
cd RediSearch
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
make

redis-server --loadmodule ./redisearch.so
```

The resulting module will be in the current directory.

You can also simply type `make` from the top level directory, this will
take care of running `cmake` with the appropriate arguments, and provide you
with a `redisearch.so` file in the `src` directory:

```sh
git clone https://github.com/RediSearch/RediSearch.git
cd RediSearch
make
redis-server --loadmodule ./src/redisearch.so
```

## Creating an index with fields and weights (default weight is 1.0)

```
127.0.0.1:6379> FT.CREATE myIdx SCHEMA title TEXT WEIGHT 5.0 body TEXT url TEXT
OK 

``` 

## Adding documents to the index
```
127.0.0.1:6379> FT.ADD myIdx doc1 1.0 FIELDS title "hello world" body "lorem ipsum" url "http://redis.io" 
OK
```

## Searching the index

```
127.0.0.1:6379> FT.SEARCH myIdx "hello world" LIMIT 0 10
1) (integer) 1
2) "doc1"
3) 1) "title"
   2) "hello world"
   3) "body"
   4) "lorem ipsum"
   5) "url"
   6) "http://redis.io"
```

!!! note
    Input is expected to be valid utf-8 or ASCII. The engine cannot handle wide character unicode at the moment. 


## Dropping the index

```
127.0.0.1:6379> FT.DROP myIdx
OK
```

## Adding and getting Auto-complete suggestions

```
127.0.0.1:6379> FT.SUGADD autocomplete "hello world" 100
OK

127.0.0.1:6379> FT.SUGGET autocomplete "he"
1) "hello world"

```
