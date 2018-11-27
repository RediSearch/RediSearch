[![CircleCI](https://circleci.com/gh/RedisLabsModules/RediSearch/tree/master.svg?style=svg)](https://circleci.com/gh/RedisLabsModules/RediSearch/tree/master)

# RediSearch

### Full-Text search over Redis by RedisLabs
![logo.png](docs/logo.png)

### See Full Documentation at [https://oss.redislabs.com/redisearch/](https://oss.redislabs.com/redisearch/)

### Latest Release: [1.4.2](https://github.com/RedisLabsModules/RediSearch/releases)

# Overview

RediSearch implements a search engine on top of Redis, but unlike other Redis
search libraries, it does not use internal data structures like Sorted Sets.

Inverted indexes are stored as a special compressed data type that allows for fast indexing and search speed, and low memory footprint.

This also enables more advanced features, like exact phrase matching and numeric filtering for text queries, that are not possible or efficient with traditional Redis search approaches.

# Docker Image

[https://hub.docker.com/r/redislabs/redisearch/](https://hub.docker.com/r/redislabs/redisearch/)

```sh
$ docker run -p 6379:6379 redislabs/redisearch:latest
```
# Mailing List / Forum

Got questions? Feel free to ask at the [RediSearch mailing list](https://groups.google.com/forum/#!forum/redisearch).

# Client Libraries

Official (Redis Labs) and community Clients:

| Language | Library | Author | License | Comments |
|---|---|---|---|---|
|Python | [redisearch-py](https://github.com/RedisLabs/redisearch-py) | Redis Labs | BSD | Usually the most up-to-date client library |
| Java | [JRediSearch](https://github.com/RedisLabs/JRediSearch) | Redis Labs | BSD | |
| Go | [redisearch-go](https://github.com/RedisLabs/redisearch-go) | Redis Labs | BSD | Incomplete API |
| JavaScript | [RedRediSearch](https://github.com/stockholmux/redredisearch) | Kyle J. Davis | MIT | Partial API, compatible with [Reds](https://github.com/tj/reds) |
| C# | [NRediSearch](https://libraries.io/nuget/NRediSearch) | Marc Gravell | MIT | Part of StackExchange.Redis |
| PHP | [redisearch-php](https://github.com/ethanhann/redisearch-php) | Ethan Hann | MIT |
| Ruby on Rails | [redi_search_rails](https://github.com/dmitrypol/redi_search_rails)  | Dmitry Polyakovsky | MIT | |
| Ruby | [redisearch-rb](https://github.com/vruizext/redisearch-rb) | Victor Ruiz | MIT | |

## Primary Features:

* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time).
* Field weights.
* Complex boolean queries with AND, OR, NOT operators between sub-queries.
* Prefix matching, fuzzy matching and exact phrase search in full-text queries.
* Auto-complete suggestions (with fuzzy prefix suggestions).
* Stemming based query expansion in [many languages](https://oss.redislabs.com/redisearch/Stemming/) (using [Snowball](http://snowballstem.org/)).
* Support for logographic (Chinese, etc.) tokenization and querying (using [Friso](https://github.com/lionsoul2014/friso))
* Limiting searches to specific document fields (up to 128 fields supported).
* Numeric filters and ranges.
* Geographical search utilizing Redis' own GEO commands.
* A powerfull aggregations engine.
* Supports any utf-8 encoded text.
* Retrieve full document content or just ids.
* Automatically index existing HASH keys as documents.
* Document deletion.
* Sortable properties (i.e. sorting users by age or name).

## Cluster Support

RediSearch has a distributed cluster version that can scale to billions of documents and hundreds of servers. However, at the moment it is only available as part of Redis Labs Enterprise. See the [Redis Labs Website](https://redislabs.com/modules/redisearch/) for more info and contact information.

### Not *yet* supported:

* Spelling correction

### License

Apache 2.0 with Commons Clause - see [LICENSE](LICENSE)
