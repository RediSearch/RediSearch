# Welcome to RediSearch 

RediSearch is a Full-Text search over Redis, developed by RedisLabs. 

The source code is available at [https://github.com/RedisLabsModules/RediSearch](https://github.com/RedisLabsModules/RediSearch).

### Latest Release: [0.12 (Preview)](https://github.com/RedisLabsModules/RediSearch/releases/tag/v0.11)

## Overview

Redisearch impements a search engine on top of redis, but unlike other redis 
search libraries, it does not use internal data structures like sorted sets.

Inverted indexes are stored on top of Redis strings using binary encoding,
and not mapped to existing data structures (see [DESIGN.md](https://github.com/RedisLabsModules/RediSearch/blob/master/docs/DESIGN.md)). 

This allows much faster performance, significantly less memory consumption, and
more advanced features like exact phrase matching, that are not possible with 
traditional redis search approaches. 

## Client Libraries

* **Python**: [https://github.com/RedisLabs/redisearch-py](https://github.com/RedisLabs/redisearch-py)

* **Java**: [https://github.com/RedisLabs/JRediSearch](https://github.com/RedisLabs/JRediSearch)

## Primary Features:

* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time).
* Field weights.
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Exact Phrase Search of up to 8 words.
* Stemming based query expansion in [many languages](/Stemming/) (using [Snowball](http://snowballstem.org/)).
* Support for custom functions for query expansion and scoring (see [Extensions](/Extensions)).
* Limiting searches to specific document fields (up to 8 fields supported).
* Numeric filters and ranges.
* Geo filtering using Redis' own Geo-commands. 
* Supports any utf-8 encoded text.
* Retrieve full document content or just ids
* Automatically index existing HASH keys as documents.
* Document deletion and updating with index garbage collection.


