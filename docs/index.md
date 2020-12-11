<img src="img/logo.svg" alt="logo" width="200"/>

# RediSearch - Redis Secondary Index & Query Engine
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)

RediSearch is a source available Secondary Index, Query Engine and Full-Text Search over Redis, developed by [Redis Labs](http://redislabs.com). 

!!! note "Quick Links:"
    * [Source Code at GitHub](https://github.com/RediSearch/RediSearch).
    * [Latest Release](https://github.com/RediSearch/RediSearch/releases)
    * [Docker Image: redislabs/redisearch](https://hub.docker.com/r/redislabs/redisearch/)
    * [Quick Start Guide](Quick_Start.md)
    * [Mailing list / Forum](https://forum.redislabs.com/c/modules/redisearch)

!!! tip "Supported Platforms"
    RediSearch is developed and tested on Linux and Mac OS, on x86_64 CPUs.

    i386 CPUs should work fine for small data-sets, but are not tested and not recommended. Atom CPUs are not supported currently. 

## Overview

Redisearch implements a secondary index on top of Redis, but unlike other Redis 
indexing libraries, it does not use internal data structures like sorted sets.

This also enables more advanced features, like multi field queries, aggregation and full text search capabilites like
exact phrase matching and numeric filtering for text queries, that are not possible or efficient with traditional Redis indexing approaches.

## Client Libraries

Official and community client libraries in Python, Java, JavaScript, Ruby, Go, C#, and PHP. 

See the [Clients page](Clients.md) for the full list.

## Cluster Support

RediSearch has a distributed cluster version that can scale to billions of documents and hundreds of servers. We also offer official commercial support for RediSearch. See the [Redis Labs Website](https://redislabs.com/redis-enterprise/technology/redis-search/#sds) for more info and contact information. 

## Primary Features

* Secondary Index. 
* Multi field queries.
* Aggregation. 
* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time).
* Complex boolean queries with AND, OR, NOT operators between sub-queries.
* Optional query clauses.
* Prefix based searches.
* Field weights.
* Auto-complete suggestions (with fuzzy prefix suggestions).
* Exact Phrase Search, Slop based search.
* Stemming based query expansion in [many languages](Stemming.md) (using [Snowball](http://snowballstem.org/)).
* Support for custom functions for query expansion and scoring (see [Extensions](Extensions.md)).
* Limiting searches to specific document fields.
* Numeric filters and ranges.
* Geo filtering using Redis' own Geo-commands. 
* Unicode support (UTF-8 input required).
* Retrieve full document content or just ids.
* Document deletion and updating with index garbage collection.
* Partial and conditional document updates.

## References
### Videos
1. [RediSearch? - RedisConf 2020](https://youtu.be/9R29LLWquME)
1. [RediSearch Overview - RedisConf 2019](https://youtu.be/AwnEhr9BO74) 
1. [RediSearch & CRDT - Redis Day Tel Aviv 2019](https://youtu.be/OGC6Mx9E3jU)


### Course
* [RU203: Querying, Indexing, and Full-Text Search](https://university.redislabs.com/courses/ru203/) - An online RediSearch course from [Redis University](https://university.redislabs.com/).

### Blog posts
1. [Introducing RediSearch 2.0](https://redislabs.com/blog/introducing-redisearch-2-0/)
1. [Getting Started with RediSearch 2.0](https://redislabs.com/blog/getting-started-with-redisearch-2-0/)
1. [Mastering RediSearch / Part I](https://redislabs.com/blog/mastering-redisearch-part/)
1. [Mastering RediSearch / Part II](https://redislabs.com/blog/mastering-redisearch-part-ii/)
1. [Mastering RediSearch / Part III](https://redislabs.com/blog/mastering-redisearch-part-iii/)
1. [Building Real-Time Full-Text Site Search with RediSearch](https://redislabs.com/blog/building-real-time-full-text-site-search-with-redisearch/)
1. [Search Benchmarking: RediSearch vs. Elasticsearch](https://redislabs.com/blog/search-benchmarking-redisearch-vs-elasticsearch/)
1. [RediSearch Version 1.6 Adds Features, Improves Performance](https://redislabs.com/blog/redisearch-version-1-6-adds-features-improves-performance/)
1. [RediSearch 1.6 Boosts Performance Up to 64%](https://redislabs.com/blog/redisearch-1-6-boosts-performance-up-to-64/)

## Mailing List / Forum

Got questions? Feel free to ask at the [RediSearch forum](https://forum.redislabs.com/c/modules/redisearch/).

## License

Redis Source Available License Agreement - see [LICENSE](https://raw.githubusercontent.com/RediSearch/RediSearch/master/LICENSE)
