
# Quick Start Guide for RediSearch:

## Building and running:

```sh
git clone https://github.com/RedisLabsModules/RediSearch.git
cd RediSearch/src
make all

# Assuming you have a redis build from the unstable branch:
/path/to/redis-server --loadmodule ./module.so
```

## Creating an index with fields and weights:

```
127.0.0.1:6379> FT.CREATE myIdx title TEXT 5.0 body TEXT 1.0 url 1.0
OK 

``` 

## Adding documents to the index:
```
127.0.0.1:6379> FT.ADD myIdx doc1 1.0 fields title "hello world" body "lorem ipsum" url "http://redis.io" 
OK
```

## Searching the index:

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

  > **NOTE**: Input is expected to be valid utf-8 or ascii. The engine cannot handle wide character unicode at the moment. 


## Dropping the index:

```
127.0.0.1:6379> FT.DROP myIdx
OK
```

## Adding and getting Auto-complete suggestions:

```
127.0.0.1:6379> FT.SUGADD autocomplete "hello world" 100
OK

127.0.0.1:6379> FT.SUGGET autocomplete "he"
1) "hello world"

```
