# RediSearch 

### Full-Text search over redis by RedisLabs

### See Full Documentation at [http://redislabsmodules.github.io/RediSearch/](http://redislabsmodules.github.io/RediSearch/)

# Overview

Redisearch impements a search engine on top of redis, but unlike other redis 
search libraries, it does not use internal data structures like sorted sets.

Inverted indexes are stored on top of Redis strings using binary encoding,
and not mapped to existing data structures (see [docs/DESIGN.md](DESIGN.md)). 

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

### Not *yet* supported:

* Geo filters.
* NOT queries (foo -bar).
* Spelling correction
* Aggregations
* Deletion and Updating (without full index rebuild)

### License: AGPL

Which basically means you can freely use this for your own projects without "virality" to your code,
as long as you're not modifying the module itself.

### Note About Stability

RediSearch is still under development and can be considered Alpha. While we've tested it extensively with big data-sets and very high workloads, and it is very stable - the API itself may change. You are welcome to use it, but keep in mind future versions might change things.
 
