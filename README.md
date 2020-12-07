[![Release](https://img.shields.io/github/v/release/redisearch/redisearch.svg?sort=semver)](https://github.com/RediSearch/RediSearch/releases)
[![CircleCI](https://circleci.com/gh/RediSearch/RediSearch/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/RediSearch/tree/master)
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/redislabs/redisearch.svg)](https://hub.docker.com/r/redislabs/redisearch/builds/)
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)
[![Codecov](https://codecov.io/gh/RediSearch/RediSearch/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/RediSearch)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/RediSearch/RediSearch.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RediSearch/RediSearch/alerts/)

# RediSearch

## Secondary Index, Query Engine & Full-Text search over Redis by RedisLabs
<img src="docs/img/logo.svg" alt="logo" width="300"/>

### See Full Documentation at [https://oss.redislabs.com/redisearch/](https://oss.redislabs.com/redisearch/)

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

Got questions? Feel free to ask at the [RediSearch Discussion Forum](http://forum.redislabs.com/c/modules/redisearch).

# Client Libraries

Official (Redis Labs) and community Clients:

| Language | Library | Author | License | Stars |
|----------|---------|--------|---------|-------|
|Python | [redisearch-py][redisearch-py-url] | [Redis Labs][redisearch-py-author] | BSD | [![redisearch-py-stars]][redisearch-py-url] |
| Java | [JRediSearch][JRediSearch-url] | [Redis Labs][JRediSearch-author] | BSD | [![JRediSearch-stars]][JRediSearch-url]|
| Java | [lettusearch][lettusearch-url] | [Redis Labs][lettusearch-author] | Apache-2.0 | [![lettusearch-stars]][lettusearch-url]|
| Java | [spring-redisearch][spring-redisearch-url] | [Redis Labs][spring-redisearch-author] | Apache-2.0 | [![spring-redisearch-stars]][spring-redisearch-url]|
| Java | [redis-modules-java][redis-modules-java-url] | [dengliming][redis-modules-java-author] | Apache-2.0 | [![redis-modules-java-stars]][redis-modules-java-url]|
| Go | [redisearch-go][redisearch-go-url] | [Redis Labs][redisearch-go-author] | BSD | [![redisearch-go-stars]][redisearch-go-url] | 
| JavaScript | [RedRediSearch][RedRediSearch-url] | [Kyle J. Davis][RedRediSearch-author] | MIT |[![RedRediSearch-stars]][RedRediSearch-url]|
| JavaScript | [redis-redisearch][redis-redisearch-url] | [Kyle J. Davis][redis-redisearch-author] | MIT | [![redis-redisearch-stars]][redis-redisearch-url]|
| C# | [NRediSearch][NRediSearch-url] | [Marc Gravell][NRediSearch-author] | MIT | [![NRediSearch-stars]][NRediSearch-url] |
| PHP | [redisearch-php][redisearch-php-url] | [Ethan Hann][redisearch-php-author] | MIT | [![redisearch-php-stars]][redisearch-php-url] |
| Ruby on Rails | [redi_search_rails][redi_search_rails-url] | [Dmitry Polyakovsky][redi_search_rails-author] | MIT | [![redi_search_rails-stars]][redi_search_rails-url]|
| Ruby | [redisearch-rb][redisearch-rb-url] | [Victor Ruiz][redisearch-rb-author] | MIT | [![redisearch-rb-stars]][redisearch-rb-url]|
| Ruby | [redi_search][redi_search-url] | [Nick Pezza][redi_search-author] | MIT | [![redi_search-stars]][redi_search-url] |

[redisearch-py-url]: https://github.com/RediSearch/redisearch-py
[redisearch-py-author]: https://redislabs.com
[redisearch-py-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-py.svg?style=social&amp;label=Star&amp;maxAge=2592000

[JRediSearch-url]: https://github.com/RediSearch/JRediSearch
[JRediSearch-author]: https://redislabs.com
[JRediSearch-stars]: https://img.shields.io/github/stars/RediSearch/JRediSearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[lettusearch-url]: https://github.com/RediSearch/lettusearch
[lettusearch-author]: https://redislabs.com
[lettusearch-stars]: https://img.shields.io/github/stars/RediSearch/lettusearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[spring-redisearch-url]: https://github.com/RediSearch/spring-redisearch
[spring-redisearch-author]: https://redislabs.com
[spring-redisearch-stars]: https://img.shields.io/github/stars/RediSearch/spring-redisearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-modules-java-url]: https://github.com/dengliming/redis-modules-java
[redis-modules-java-author]: https://github.com/dengliming
[redis-modules-java-stars]: https://img.shields.io/github/stars/dengliming/redis-modules-java.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redisearch-go-url]: https://github.com/RediSearch/redisearch-go
[redisearch-go-author]: https://redislabs.com
[redisearch-go-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-go.svg?style=social&amp;label=Star&amp;maxAge=2592000

[RedRediSearch-url]: https://github.com/stockholmux/redredisearch
[RedRediSearch-author]: https://github.com/stockholmux
[RedRediSearch-stars]: https://img.shields.io/github/stars/stockholmux/redredisearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-redisearch-url]: https://github.com/stockholmux/node_redis-redisearch
[redis-redisearch-author]: https://github.com/stockholmux
[redis-redisearch-stars]: https://img.shields.io/github/stars/stockholmux/node_redis-redisearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[NRediSearch-url]: https://libraries.io/nuget/NRediSearch
[NRediSearch-author]: https://github.com/StackExchange/
[NRediSearch-stars]: https://img.shields.io/github/stars/StackExchange/StackExchange.Redis.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redisearch-php-url]: https://github.com/ethanhann/redisearch-php
[redisearch-php-author]: https://github.com/ethanhann
[redisearch-php-stars]: https://img.shields.io/github/stars/ethanhann/redisearch-php.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redi_search_rails-url]: https://github.com/dmitrypol/redi_search_rails
[redi_search_rails-author]: https://github.com/dmitrypol
[redi_search_rails-stars]: https://img.shields.io/github/stars/dmitrypol/redi_search_rails.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redisearch-rb-url]: https://github.com/vruizext/redisearch-rb
[redisearch-rb-author]: https://github.com/vruizext
[redisearch-rb-stars]: https://img.shields.io/github/stars/vruizext/redisearch-rb.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redi_search-url]: https://github.com/npezza93/redi_search
[redi_search-author]: https://github.com/npezza93
[redi_search-stars]: https://img.shields.io/github/stars/npezza93/redi_search.svg?style=social&amp;label=Star&amp;maxAge=2592000

## Other available Libraries

| Language | Library | Author | License | Stars | Comments |
|----------|---------|--------|---------|-------|----------|
| Rust | [redisearch-api-rs][redisearch-api-rs-url] | [Redis Labs][redisearch-api-rs-author] | BSD | [![redisearch-api-rs-stars]][redisearch-api-rs-url] | API for Redis Modules written in Rust |


[redisearch-api-rs-url]: https://github.com/RediSearch/redisearch-api-rs
[redisearch-api-rs-author]: https://redislabs.com
[redisearch-api-rs-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-api-rs.svg?style=social&amp;label=Star&amp;maxAge=2592000

## Features:

* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time).
* Field weights.
* Complex boolean queries with AND, OR, NOT operators between sub-queries.
* Prefix matching, fuzzy matching and exact phrase search in full-text queries.
* Support for DM phonetic matching
* Auto-complete suggestions (with fuzzy prefix suggestions).
* Stemming based query expansion in [many languages](https://oss.redislabs.com/redisearch/Stemming/) (using [Snowball](http://snowballstem.org/)).
* Support for logographic (Chinese, etc.) tokenization and querying (using [Friso](https://github.com/lionsoul2014/friso))
* Limiting searches to specific document fields (up to 128 fields supported).
* Numeric filters and ranges.
* Lightweight tag fields for exact-match boolean queries
* Geographical search utilizing Redis' own GEO commands.
* A powerful aggregations engine.
* Supports any utf-8 encoded text.
* Retrieve full document content or just ids.
* Automatically index existing HASH keys as documents.
* Document deletion.
* Sortable properties (i.e. sorting users by age or name).

## Cluster Support

RediSearch has a distributed cluster version that can scale to billions of documents and hundreds of servers. However, at the moment it is only available as part of Redis Labs Enterprise. See the [Redis Labs Website](https://redislabs.com/modules/redisearch/) for more info and contact information.

### License

 Redis Source Available License Agreement - see [LICENSE](LICENSE)
