<center>![logo.png](logo.png)</center>

# RediSearch - Redis Powered Search Engine

RediSearch is a an open-source Full-Text and Secondary Index engine over Redis, developed by [Redis Labs](http://redislabs.com). 

!!! note "Quick Links:"
    * [Source Code at GitHub](https://github.com/RedisLabsModules/RediSearch).
    * [Latest Release: 0.21.2](https://github.com/RedisLabsModules/RediSearch/releases)
    * [Docker Image: redislabs/redisearch](https://hub.docker.com/r/redislabs/redisearch/)
    * [Quick Start Guide](/Quick_Start)

!!! tip "Supported Platforms"
    RediSearch is developed and tested on Linux and Mac OS, on x86_64 CPUs.

    i386 CPUs should work fine for small data-sets, but are not tested and not recommended. Atom CPUs are not supported currently. 

## Overview

Redisearch implements a search engine on top of Redis, but unlike other Redis 
search libraries, it does not use internal data structures like sorted sets.

This also enables more advanced features, like exact phrase matching and numeric filtering for text queries, 
that are not possible or efficient with traditional redis search approache

## Client Libraries

Official and community client libraries in Python, Java, JavaScript, Ruby, Go, C#, and PHP. 
See [Clients Page](/Clients)

## Cluster Support and Commercial Version

RediSearch has a distributed cluster version that can scale to billions of documents and hundreds of servers. However, it is only available as part of Redis Labs Enterprise. We also offer official commercial suppport for RediSearch. See the [Redis Labs Website](https://redislabs.com/modules/redisearch/) for more info and contact information. 

## Primary Features:

* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time).
* Complex boolean queries with AND, OR, NOT operators between sub-queries.
* Optional query clauses.
* Prefix based searches.
* Field weights.
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Exact Phrase Search, Slop based search.
* Stemming based query expansion in [many languages](/Stemming/) (using [Snowball](http://snowballstem.org/)).
* Support for custom functions for query expansion and scoring (see [Extensions](/Extensions)).
* Limiting searches to specific document fields (**up to 32 fields supported**).
* Numeric filters and ranges.
* Geo filtering using Redis' own Geo-commands. 
* Unicode support (UTF-8 input required)
* Retrieve full document content or just ids
* Document deletion and updating with index garbage collection.


