# RediSearch - Redis Powered Search Engine

RediSearch is a full-text search over Redis, developed by RedisLabs. 

The source code is available at [https://github.com/RedisLabsModules/RediSearch](https://github.com/RedisLabsModules/RediSearch).

### Latest Release: [0.16 (Preview)](https://github.com/RedisLabsModules/RediSearch/releases/tag/v0.16)

## Overview

The Redisearch module implements a search engine on top of Redis, but unlike other Redis 
search libraries, it does not use internal data structures like sorted sets.

It enables more advanced features, like exact phrase matching and numeric filtering for text queries, 
that are not possible or efficient with traditional Redis search approaches.

## Client Libraries

* **Python**: [https://github.com/RedisLabs/redisearch-py](https://github.com/RedisLabs/redisearch-py)

* **Java**: [https://github.com/RedisLabs/JRediSearch](https://github.com/RedisLabs/JRediSearch)

### Community Libraries:

* **PHP**: [https://github.com/ethanhann/redisearch-php](https://github.com/ethanhann/redisearch-php) (By Ethan Hann)

* **Ruby on Rails**: [https://github.com/dmitrypol/redi_search_rails](https://github.com/dmitrypol/redi_search_rails) (By Dmitry Polyakovsky)

* **.Net**: [https://libraries.io/nuget/NRediSearch](https://libraries.io/nuget/NRediSearch) (By Marc Gravell)

## Primary Features:

* Full-text indexing of multiple fields in documents
* Incremental indexing without performance loss
* Document ranking (provided manually by the user at index time)
* Complex Boolean queries with AND, OR, NOT operators between sub-queries
* Optional query clauses
* Prefix-based searches
* Field weights
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Exact phrase search & slop-based search
* Stemming-based query expansion in [many languages](/Stemming/) (using [Snowball](http://snowballstem.org/))
* Custom functions for query expansion and scoring (see [Extensions](/Extensions))
* Limiting searches to specific document fields (up to 8 fields supported)
* Numeric filters and ranges
* Geo filtering using Redis' own geo-commands
* UTF-8 encoded text support
* Retrieves full document content or just IDs
* Automatically indexes existing HASH keys as documents
* Document deletion and index garbage collection


