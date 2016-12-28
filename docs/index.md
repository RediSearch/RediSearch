# Welcome to RediSearch 

RediSearch is a Full-Text search over Redis, developed by RedisLabs. 

The source code is available at [github.com/RedisLabsModules/RediSearch](github.com/RedisLabsModules/RediSearch).

## Overview

Redisearch impements a search engine on top of redis, but unlike other redis 
search libraries, it does not use internal data structures like sorted sets.

Inverted indexes are stored on top of Redis strings using binary encoding,
and not mapped to existing data structures (see [DESIGN.md](DESIGN.md)). 

This allows much faster performance, significantly less memory consumption, and
more advanced features like exact phrase matching, that are not possible with 
traditional redis search approaches. 

## Primary Features:

* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time).
* Field weights.
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Exact Phrase Search of up to 8 words.
* Stemming based query expansion in [many languages](#stemming-support) (using [Snowball](http://snowballstem.org/)).
* Limiting searches to specific document fields (up to 8 fields supported).
* Numeric filters and ranges.
* Supports any utf-8 encoded text.
* Retrieve full document content or just ids
* Automatically index existing HASH keys as documents.

