<img src="img/logo.svg" alt="logo" width="200"/>

# RediSearch - Redis Powered Search Engine
[![Mailing List](https://img.shields.io/badge/Mailing%20List-RediSearch-blue)](https://groups.google.com/forum/#!forum/redisearch)
[![Gitter](https://badges.gitter.im/RedisLabs/RediSearch.svg)](https://gitter.im/RedisLabs/RediSearch?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

RediSearch is a source available Full-Text and Secondary Index engine over Redis, developed by [Redis Labs](http://redislabs.com). 

!!! note "Quick Links:"
    * [Source Code at GitHub](https://github.com/RediSearch/RediSearch).
    * [Latest Release](https://github.com/RediSearch/RediSearch/releases)
    * [Docker Image: redislabs/redisearch](https://hub.docker.com/r/redislabs/redisearch/)
    * [Quick Start Guide](Quick_Start.md)
    * [Mailing list / Forum](https://groups.google.com/forum/#!forum/redisearch)

!!! tip "Supported Platforms"
    RediSearch is developed and tested on Linux and Mac OS, on x86_64 CPUs.

    i386 CPUs should work fine for small data-sets, but are not tested and not recommended. Atom CPUs are not supported currently. 

## Overview

Redisearch implements a search engine on top of Redis, but unlike other Redis 
search libraries, it does not use internal data structures like sorted sets.

This also enables more advanced features, like exact phrase matching and numeric filtering for text queries, 
that are not possible or efficient with traditional Redis search approaches.

## Client Libraries

Official and community client libraries in Python, Java, JavaScript, Ruby, Go, C#, and PHP. 

See the [Clients page](Clients.md) for the full list.

## Cluster Support and Commercial Version

RediSearch has a distributed cluster version that can scale to billions of documents and hundreds of servers. However, it is only available as part of Redis Labs Enterprise. We also offer official commercial support for RediSearch. See the [Redis Labs Website](https://redislabs.com/redis-enterprise/technology/redis-search/#sds) for more info and contact information. 

## Primary Features

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
* Retrieve full document content or just ids
* Document deletion and updating with index garbage collection.
* Partial and conditional document updates.

## References
### Videos
1. [RediSearch Overview - RedisConf 2019](https://youtu.be/AwnEhr9BO74) 
1. [RediSearch & CRDT - Redis Day Tel Aviv 2019](https://youtu.be/OGC6Mx9E3jU)

### Blog posts
1. [Mastering RediSearch / Part I](https://redislabs.com/blog/mastering-redisearch-part/)
1. [Mastering RediSearch / Part II](https://redislabs.com/blog/mastering-redisearch-part-ii/)
1. [Mastering RediSearch / Part III](https://redislabs.com/blog/mastering-redisearch-part-iii/)
1. [Search Benchmarking: RediSearch vs. Elasticsearch](https://redislabs.com/blog/search-benchmarking-redisearch-vs-elasticsearch/)

## Mailing List / Forum

Got questions? Feel free to ask at the [RediSearch mailing list](https://groups.google.com/forum/#!forum/redisearch).

## License

Redis Source Available License Agreement - see [LICENSE](https://raw.githubusercontent.com/RediSearch/RediSearch/master/LICENSE)
