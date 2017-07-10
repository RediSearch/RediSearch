<center>![logo.png](logo.png)</center>

# RediSearch - Redis Powered Search Engine

RediSearch is a Full-Text search over Redis, developed by RedisLabs. 

The source code is available at [https://github.com/RedisLabsModules/RediSearch](https://github.com/RedisLabsModules/RediSearch).

### Latest Release: [0.19.1 (Preview)](https://github.com/RedisLabsModules/RediSearch/releases/tag/v0.19.1)

## Overview

Redisearch impements a search engine on top of redis, but unlike other redis 
search libraries, it does not use internal data structures like sorted sets.

This also enables more advanced features, like exact phrase matching and numeric filtering for text queries, 
that are not possible or efficient with traditional redis search approache

## Client Libraries

* **Python**: [https://github.com/RedisLabs/redisearch-py](https://github.com/RedisLabs/redisearch-py)

* **Java**: [https://github.com/RedisLabs/JRediSearch](https://github.com/RedisLabs/JRediSearch)

### Community Libraries:

* **PHP**: [https://github.com/ethanhann/redisearch-php](https://github.com/ethanhann/redisearch-php) (By Ethan Hann)

* **Ruby**: [https://github.com/vruizext/redisearch-rb](https://github.com/vruizext/redisearch-rb) (By Victor Ruiz)

* **Ruby on Rails**: [https://github.com/dmitrypol/redi_search_rails](https://github.com/dmitrypol/redi_search_rails) (By Dmitry Polyakovsky)

* **.Net**: [https://libraries.io/nuget/NRediSearch](https://libraries.io/nuget/NRediSearch) (By Marc Gravell)

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
* Limiting searches to specific document fields (up to 8 fields supported).
* Numeric filters and ranges.
* Geo filtering using Redis' own Geo-commands. 
* Supports any utf-8 encoded text.
* Retrieve full document content or just ids
* Automatically index existing HASH keys as documents.
* Document deletion and updating with index garbage collection.


