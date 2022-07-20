---
title: "RediSearch quick start"
linkTitle: "Quick start"
weight: 1
description: >
    Get started with RediSearch
---

## Set up RediSearch

There are several ways to set up a Redis database with the RediSearch module enabled.

### Redis Cloud

RediSearch is available on all Redis Cloud managed services.  Redis Cloud Essentials offers a completely free managed database with up to 30MB.

[Create a free Redis Cloud account here](https://redis.com/try-free/) and select **Redis Stack** as your database type. For more detailed instructions, see the [Redis Stack and modules quick start](https://docs.redis.com/latest/modules/modules-quickstart/#set-up-a-redis-cloud-database).

### Run with Docker

```sh
$ docker run -p 6379:6379 redis/redis-stack-server:latest
```

### Download binaries

1. Download the pre-compiled version from the [Redis download center](https://redis.com/download-center/modules/).

1. Run Redis with RediSearch:

    ```sh
    $ redis-server --loadmodule /path/to/module/src/redisearch.so
    ```

### Build from source

1. Clone the git repository (make sure you include the `--recursive` option to properly clone submodules):

    ```sh
    $ git clone --recursive https://github.com/RediSearch/RediSearch.git
    $ cd RediSearch
    ```

1. Install dependencies:

    On macOS:
    ```sh
    $ make setup
    ```

    On Linux:
    ```sh
    $ sudo make setup
    ```

1. Build:
    ```sh
    $ make build
    ```

1. Run Redis with RediSearch:
    ```sh
    $ make run
    ```

For more elaborate build instructions, see the [Development page](/docs/stack/search/development).

## Create an index

Create an index with fields and weights (default weight is 1.0):

```sh
127.0.0.1:6379> FT.CREATE myIdx ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 5.0 body TEXT url TEXT
OK
```

## Add documents

Add a document to the index:

```sh
127.0.0.1:6379> HSET doc:1 title "hello world" body "lorem ipsum" url "http://redis.io"
(integer) 3
```

## Search the index

```sh
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

{{% alert title="Note" color="info" %}}
Input is expected to be valid UTF-8 or ASCII. The engine cannot handle wide character unicode at the moment.
{{% /alert %}}

## Drop the index

```sh
127.0.0.1:6379> FT.DROPINDEX myIdx
OK
```

## Auto-complete

Add an [auto-complete](/docs/stack/search/design/overview/#auto-completion) suggestion with `FT.SUGADD`:

```sh
127.0.0.1:6379> FT.SUGADD autocomplete "hello world" 100
OK
```

Test auto-complete suggestions with `FT.SUGGET`:

```sh
127.0.0.1:6379> FT.SUGGET autocomplete "he"
1) "hello world"
```
