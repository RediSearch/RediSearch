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

## Connect to Redis Stack

{{< clients-example search_quickstart connect >}}
> redis-cli -h 127.0.0.1 -p 6379
{{< /clients-example>}}

## Create an index

Use the `FT.CREATE` command to create an index with fields and weights (default weight is 1.0):

{{< clients-example search_quickstart create_index >}}
127.0.0.1:6379> FT.CREATE idx:bicycle ON JSON PREFIX 1 bicycle: SCHEMA $.brand AS brand TEXT $.model AS model TEXT $.description AS description TEXT $.price AS price NUMERIC $.condition AS condition TAG
OK
{{< / clients-example >}}

Any existing JSON documents that have a key prefixed with `bicycle:` are automatically added to the index at this time.

## Add documents

After you create the index, any new JSON documents with the `bicycle:` prefix are automatically indexed upon creation.

Use the `JSON.SET` command to create a new [JSON](/docs/stack/json/) document and add it to the index:

{{< clients-example search_quickstart add_documents "" 2 >}}
127.0.0.1:6379> JSON.SET bicycle:0 $ "{\"brand\":\"Diaz Ltd\",\"model\":\"Dealer Sl\",\"price\":7315.58,\"description\":\"The Diaz Ltd Dealer Sl is a reliable choice for urban cycling. The Diaz Ltd Dealer Sl is a comfortable choice for urban cycling.\",\"condition\":\"used\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:1 $ "{\"brand\":\"Bridges Group\",\"model\":\"Project Pro\",\"price\":3610.82,\"description\":\"This mountain bike is perfect for mountain biking. The Bridges Group Project Pro is a responsive choice for mountain biking.\",\"condition\":\"used\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:2 $ "{\"brand\":\"Vega, Cole and Miller\",\"model\":\"Group Advanced\",\"price\":8961.42,\"description\":\"The Vega, Cole and Miller Group Advanced provides a excellent ride. With its fast carbon frame and 24 gears, this bicycle is perfect for any terrain.\",\"condition\":\"used\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:3 $ "{\"brand\":\"Powell-Montgomery\",\"model\":\"Angle Race\",\"price\":4050.27,\"description\":\"The Powell-Montgomery Angle Race is a smooth choice for road cycling. The Powell-Montgomery Angle Race provides a durable ride.\",\"condition\":\"used\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:4 $ "{\"brand\":\"Gill-Lewis\",\"model\":\"Action Evo\",\"price\":283.68,\"description\":\"The Gill-Lewis Action Evo provides a smooth ride. The Gill-Lewis Action Evo provides a excellent ride.\",\"condition\":\"used\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:5 $ "{\"brand\":\"Rodriguez-Guerrero\",\"model\":\"Drama Comp\",\"price\":4462.55,\"description\":\"This kids bike is perfect for young riders. With its excellent aluminum frame and 12 gears, this bicycle is perfect for any terrain.\",\"condition\":\"new\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:6 $ "{\"brand\":\"Moore PLC\",\"model\":\"Award Race\",\"price\":3790.76,\"description\":\"This olive folding bike features a carbon frame and 27.5 inch wheels. This folding bike is perfect for compact storage and transportation.\",\"condition\":\"new\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:7 $ "{\"brand\":\"Hall, Haley and Hayes\",\"model\":\"Weekend Plus\",\"price\":2008.4,\"description\":\"The Hall, Haley and Hayes Weekend Plus provides a comfortable ride. This blue kids bike features a steel frame and 29.0 inch wheels.\",\"condition\":\"new\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:8 $ "{\"brand\":\"Peck-Carson\",\"model\":\"Sun Hybrid\",\"price\":9874.95,\"description\":\"With its comfortable aluminum frame and 25 gears, this bicycle is perfect for any terrain. The Peck-Carson Sun Hybrid provides a comfortable ride.\",\"condition\":\"new\"}"
OK
127.0.0.1:6379> JSON.SET bicycle:9 $ "{\"brand\":\"Fowler Ltd\",\"model\":\"Weekend Trail\",\"price\":3833.71,\"description\":\"The Fowler Ltd Letter Trail is a comfortable choice for transporting cargo. This cargo bike is perfect for transporting cargo.\",\"condition\":\"refurbished\"}"
OK
{{< / clients-example >}}

## Search the index

Let's find a folding bicycle and filter the results by price range using the `FT.SEARCH` command. 

For more information, see [Query syntax](/docs/stack/search/reference/query_syntax).

{{< clients-example search_quickstart query_single_term_and_num_range >}}
127.0.0.1:6379> FT.SEARCH idx:bicycle "folding @price:[1000 4000]"
1) (integer) 1
2) "bicycle:6"
3) 1) "$"
   2) "{\"brand\":\"Moore PLC\",\"model\":\"Award Race\",\"price\":3790.76,\"description\":\"This olive folding bike features a carbon frame and 27.5 inch wheels. This folding bike is perfect for compact storage and transportation.\",\"condition\":\"new\"}"
{{< / clients-example >}}

Return only the `price` field.

{{< clients-example search_quickstart query_single_term_limit_fields >}}
127.0.0.1:6379> FT.SEARCH idx:bicycle "cargo" RETURN 1 $.price
1) (integer) 1
2) "bicycle:9"
3) 1) "$.price"
   2) "3833.71"
{{< / clients-example >}}

## Aggregate the index

Count all bicycles based on their condition with `FT.AGGREGATE`.

{{< clients-example search_quickstart simple_aggregation >}}
127.0.0.1:6379> FT.AGGREGATE idx:bicycle "*" GROUPBY 1 @condition REDUCE COUNT 0 AS count
1) (integer) 3
2) 1) "condition"
   2) "refurbished"
   3) "count"
   4) "1"
3) 1) "condition"
   2) "used"
   3) "count"
   4) "5"
4) 1) "condition"
   2) "new"
   3) "count"
   4) "4"
{{< / clients-example >}}

To learn how to use RediSearch with JSON documents, see [Indexing JSON documents](/docs/stack/search/indexing_json).
