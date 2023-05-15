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

RediSearch is available on all Redis Cloud managed services. A Redis Cloud Fixed subscription offers a completely free managed database with up to 30MB.

[Create a free Redis Cloud account here](https://redis.com/try-free/) and select **Redis Stack** as your database type. For more detailed instructions, see the [Redis Stack and modules quick start](https://docs.redis.com/latest/modules/modules-quickstart/#set-up-a-redis-cloud-database).

### Run with Docker

To run RediSearch with Docker, use the `redis-stack-server` Docker image:

```sh
$ docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest
```

For more information about running Redis Stack in a Docker container, see [Run Redis Stack on Docker](/docs/stack/get-started/install/docker/).

### Download binaries

To download and run RediSearch from a precompiled binary:

1. Download a precompiled version of RediSearch from the [Redis download center](https://redis.com/download-center/modules/).

1. Run Redis with RediSearch:

    ```sh
    $ redis-server --loadmodule /path/to/module/src/redisearch.so
    ```

### Build from source

To build and run RediSearch from the source code:

1. Requirements: `git` & `make`

1. Clone the [RediSearch repository](https://github.com/RediSearch/RediSearch) (make sure you include the `--recursive` option to properly clone submodules):

    ```sh
    $ git clone --recursive https://github.com/RediSearch/RediSearch.git
    $ cd RediSearch
    ```

1. Install dependencies:

    ```sh
    $ make setup
    ```

1. Build:
    ```sh
    $ make build
    ```

1. Run Redis with RediSearch:
    Requirements: [redis-server](https://redis.io/docs/getting-started/)

    ```sh
    $ make run
    ```

For more elaborate build instructions, see the [Development page](/docs/stack/search/development).

## Create an index

Use the `FT.CREATE` command to create an index with fields and weights (default weight is 1.0):

```sh
127.0.0.1:6379> FT.CREATE myIdx ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 5.0 body TEXT url TEXT
OK
```

Any existing hash documents that have a key prefixed with `doc:` are automatically added to the index at this time.

## Add documents

After you create the index, any new hash documents with the `doc:` prefix are automatically indexed upon creation.

Use the `HSET` command to create a new [hash](/docs/manual/data-types/#hashes) document and add it to the index:

```sh
127.0.0.1:6379> HSET doc:1 title "hello world" body "lorem ipsum" url "http://redis.io"
(integer) 3
```

## Search the index

To search the index for documents that contain specific words, use the `FT.SEARCH` command:

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
`FT.SEARCH` expects valid UTF-8 or ASCII as input. The engine cannot handle wide character unicode.
{{% /alert %}}

{{% alert title="Note" color="info" %}}
When configuring [ACLs](/docs/management/security/acl/), search-related commands such as `FT.SEARCH` consider the index name to be the "key". Therefore, if you use key patterns in an ACL rule allowing or disallowing search commands, include a pattern matching the index name for the specified commands. 

For example, to enable search on an index `myIdx`, the ACL rule should include a pattern for either 'FT.SEARCH` or a category that includes it. Such ACL rules could be `+ft.search ~myIdx`, `+ft.search ~myId*`, `+@all ~myIdx`, and so on.
{{% /alert %}}

## Drop the index

To remove the index without deleting the associated hash documents, run `FT.DROPINDEX` without the `DD` option:

```sh
127.0.0.1:6379> FT.DROPINDEX myIdx
OK
```

To delete the index and all indexed hash documents, add the `DD` option to the command:

```sh
127.0.0.1:6379> FT.DROPINDEX myIdx DD
OK
```

## Auto-complete

Add an [auto-complete](/docs/stack/search/design/overview/#auto-completion) suggestion with `FT.SUGADD`:

```sh
127.0.0.1:6379> FT.SUGADD autocomplete "hello world" 100
(integer) 1
```

Test auto-complete suggestions with `FT.SUGGET`:

```sh
127.0.0.1:6379> FT.SUGGET autocomplete "he"
1) "hello world"
```

## Index JSON documents

In addition to Redis hashes, you can index and search JSON documents if your database has both RediSearch and [RedisJSON](/docs/stack/json) enabled. If you have a Redis Stack database, it automatically includes both modules.

To learn how to use RediSearch with JSON documents, see [Indexing JSON documents](/docs/stack/search/indexing_json).
