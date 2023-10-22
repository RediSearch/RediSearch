---
title: "Search and query"
linkTitle: "Search and query"
description: Searching and querying Redis data
type: docs
weight: 10
aliases: 
    - /docs/stack/search
stack: true
highlighted: true
---

[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)
[![Github](https://img.shields.io/static/v1?label=&message=repository&color=5961FF&logo=github)](https://github.com/RediSearch/RediSearch/)

The enhanced Redis experience with Redis Stack provides querying capabilities, secondary indexing, full-text search, vector similarity search, and more. To use these features, you first declare indexes on your Redis data, and then you use the query language to retrieve the data your application requires. All future data will also be indexed automatically.

## Quick links
  - [Quick start guide](/docs/interact/search-and-query/quickstart/)
  - [Source code](https://github.com/RediSearch/RediSearch)
  - [Latest release](https://github.com/RediSearch/RediSearch/releases)
  - [Docker image](https://hub.docker.com/r/redis/redis-stack-server/)

## Primary features

Redis Stack supports the following search and query features:

* Secondary indexing
* Multi-field queries
* [Aggregation](/docs/interact/search-and-query/search/aggregations/)
* Full-text indexing of multiple fields in a document
* Incremental indexing without performance loss
* Document ranking (provided manually by the user at index time)
* Boolean queries with AND, OR, and NOT operators between subqueries
* Optional query clauses
* [Prefix-based searches](/docs/interact/search-and-query/query/#prefix-matching)
* Field weights
* [Auto-complete](/docs/interact/search-and-query/administration/overview/#auto-complete) and fuzzy prefix suggestions
* Exact phrase search and slop-based search
* [Stemming](/docs/interact/search-and-query/advanced-concepts/stemming/)-based query expansion for [many languages](/docs/interact/search-and-query/advanced-concepts/stemming//#supported-languages) using [Snowball](http://snowballstem.org/)
* Support for custom functions for query expansion and scoring (see [Extensions](/docs/interact/search-and-query/administration/extensions/))
* [Numeric filters](/docs/interact/search-and-query/query/#numeric-filters) and ranges
* [Geo-filtering](/docs/interact/search-and-query/query/#geo-filters) using Redis [geo commands](/commands/?group=geo)
* [Vector similarity search](/docs/interact/search-and-query/query/#vector-similarity-search) for semantic search using exact and approximate algorithms
* Unicode support (UTF-8 input required)
* Retrieval of full document contents or only their IDs
* Document deletion and updating with index garbage collection


## JSON documents

When combined with the Redis Stack's [JSON](/docs/data-types/json/) support, Redis can index and query JSON documents.

To learn how to search and query your JSON documents, see [Indexing JSON documents](/docs/interact/search-and-query/indexing/).

## Cluster support

The search and query features of Redis Stack are also available for distributed databases that can scale to billions of documents and hundreds of servers.

## Commercial support

Commercial support for Redis Stack is provided by Redis Ltd. See the [Redis Ltd. website](https://redis.com/redis-enterprise/technology/redis-search/#sds) for more details and contact information.

## Supported platforms
Redis Stack is developed and tested on Linux and macOS on x86_64 CPUs.

Atom CPUs are not supported.

## References
### Videos
1. [RediSearch? - RedisConf 2020](https://youtu.be/9R29LLWquME)
1. [RediSearch Overview - RedisConf 2019](https://youtu.be/AwnEhr9BO74)
1. [RediSearch & CRDT - Redis Day Tel Aviv 2019](https://youtu.be/OGC6Mx9E3jU)


### Course
* [RU203: Querying, Indexing, and Full-Text Search](https://university.redis.com/courses/ru203/) - An online Search and Query course from [Redis University](https://university.redis.com/).

### Blog posts
1. [Introducing RediSearch 2.0](https://redis.com/blog/introducing-redisearch-2-0/)
1. [Getting Started with RediSearch 2.0](https://redis.com/blog/getting-started-with-redisearch-2-0/)
1. [Mastering RediSearch / Part I](https://redis.com/blog/mastering-redisearch-part/)
1. [Mastering RediSearch / Part II](https://redis.com/blog/mastering-redisearch-part-ii/)
1. [Mastering RediSearch / Part III](https://redis.com/blog/mastering-redisearch-part-iii/)
1. [Building Real-Time Full-Text Site Search with RediSearch](https://redis.com/blog/building-real-time-full-text-site-search-with-redisearch/)
1. [RediSearch Version 1.6 Adds Features, Improves Performance](https://redis.com/blog/redisearch-version-1-6-adds-features-improves-performance/)
1. [RediSearch 1.6 Boosts Performance Up to 64%](https://redis.com/blog/redisearch-1-6-boosts-performance-up-to-64/)

## Mailing list / forum

Got questions? Feel free to ask at the [Search and Query forum](https://forum.redis.com/c/modules/redisearch/).

## License

Redis Source Available License 2.0 (RSALv2) or the Server Side Public License v1 (SSPLv1) - see [LICENSE](https://raw.githubusercontent.com/RediSearch/RediSearch/master/LICENSE.txt)
