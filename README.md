[![Build Status](https://travis-ci.org/RedisLabsModules/RediSearch.svg?branch=master)](https://travis-ci.org/RedisLabsModules/RediSearch)

# RediSearch 

### Full-text search over Redis by RedisLabs

### See full documentation at [http://redisearch.io](http://redisearch.io)

### Latest Release: [0.16 (Preview)](https://github.com/RedisLabsModules/RediSearch/releases/tag/v0.16)

# Client Libraries

* **Python**: [https://github.com/RedisLabs/redisearch-py](https://github.com/RedisLabs/redisearch-py)

* **Java**: [https://github.com/RedisLabs/JRediSearch](https://github.com/RedisLabs/JRediSearch)

## Community Libraries:

* **PHP**: [https://github.com/ethanhann/redisearch-php](https://github.com/ethanhann/redisearch-php) (By Ethan Hann)

* **Ruby on Rails**: [https://github.com/dmitrypol/redi_search_rails](https://github.com/dmitrypol/redi_search_rails) (By Dmitry Polyakovsky)

* **.Net**: [https://libraries.io/nuget/NRediSearch](https://libraries.io/nuget/NRediSearch) (By Marc Gravell)

# Overview

The RediSearch module implements a search engine on top of Redis, but unlike other Redis 
search libraries, it does not use internal data structures like sorted sets.

Inverted indexes are stored as a special compressed data type that allows for fast
indexing and search speed, as well as a low memory footprint. 

It enables more advanced features, like exact phrase matching and numeric filtering for text queries, 
that are not possible or efficient with traditional Redis search approaches. 

## Primary Features:

* Full-text indexing of multiple fields in documents
* Incremental indexing without performance loss
* Document ranking (provided manually by the user at index time)
* Field weights.
* Complex Boolean queries with AND, OR, NOT operators between sub-queries
* Prefix matching in full-text queries
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Exact phrase search
* Stemming-based query expansion in [many languages](http://redisearch.io/Stemming/) (using [Snowball](http://snowballstem.org/))
* Limiting searches to specific document fields (up to 32 fields supported)
* Numeric filters and ranges
* Geographical search utlizing Redis' own GEO commands
* UTF-8 encoded text support
* Retrieves full document content or IDs only
* Automatically indexes existing HASH keys as documents
* Document deletion (updates can be done through deletion and then re-insertion)
* Sortable properties (i.e. sorting users by age or name)

### Not *yet* supported:

* Spelling correction
* Aggregations

### License: AGPL

You can freely use this module for your own projects without "virality" to your code, as long as you're not modifying the module itself. See [This Blog Post](https://redislabs.com/blog/why-redis-labs-modules-are-agpl/) for more details.

### Note About Stability

RediSearch is still under development and considered alpha stage. We've tested it extensively with big data sets and very high workloads, and it is very stable, although the API itself may change. You are welcome to use it, but keep in mind that future versions might change things.
 
