
# Quick Start Guide for RediSearch

## Redis Cloud

RediSearch is available on all Redis Cloud managed services.  Redis Cloud Essentials offers a completely free managed database up to 30MB.

[Get started here](https://redislabs.com/try-free/)

## Running with Docker

```sh
docker run -p 6379:6379 redislabs/redisearch:latest
```

## Download and running binaries

First download the pre-compiled version from [RedisLabs download center](https://redislabs.com/download-center/modules/).

Next, run Redis with RediSearch: 

```sh
$ redis-server --loadmodule /path/to/module/src/redisearch.so
```

## Building and running from source

First, clone the git repo (make sure not to omit the `--recursive` option, to properly clone submodules):

```sh
git clone --recursive https://github.com/RediSearch/RediSearch.git
cd RediSearch
```

Next, install dependencies:

On macOS:
```sh
make setup
```

On Linux:
```sh
sudo make setup
```

Next, build:
```sh
make build
```

Finally, run Redis with RediSearch:
```sh
make run
```

For more elaborate build instructions, see the [Development page](Development.md).

## Creating an index with fields and weights (default weight is 1.0)

```
127.0.0.1:6379> FT.CREATE myIdx ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 5.0 body TEXT url TEXT
OK 

```

## Adding documents to the index
```
127.0.0.1:6379> hset doc:1 title "hello world" body "lorem ipsum" url "http://redis.io" 
(integer) 3
```

## Searching the index

```
127.0.0.1:6379> FT.SEARCH myIdx "hello world" LIMIT 0 10
1) (integer) 1
2) "doc:1"
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
127.0.0.1:6379> FT.DROPINDEX myIdx 
OK
```

## Adding and getting Auto-complete suggestions

```
127.0.0.1:6379> FT.SUGADD autocomplete "hello world" 100
OK

127.0.0.1:6379> FT.SUGGET autocomplete "he"
1) "hello world"

```
