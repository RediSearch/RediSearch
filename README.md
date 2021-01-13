[![Release](https://img.shields.io/github/v/release/redisearch/redisearch.svg?sort=semver)](https://github.com/RediSearch/RediSearch/releases)
[![CircleCI](https://circleci.com/gh/RediSearch/RediSearch/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/RediSearch/tree/master)
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/redislabs/redisearch.svg)](https://hub.docker.com/r/redislabs/redisearch/builds/)
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)
[![Codecov](https://codecov.io/gh/RediSearch/RediSearch/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/RediSearch)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/RediSearch/RediSearch.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RediSearch/RediSearch/alerts/)

# RediSearch

## Querying, secondary indexing, and full-text search for Redis
<img src="https://oss.redislabs.com/redisearch/img/logo.svg" alt="logo" width="300"/>

## Overview

RediSearch is a [Redis module](https://redis.io/modules) that provides querying, secondary indexing, and full-text search for Redis. To use RediSearch, you first declare indexes on your Redis data. You can then use the RediSearch query language to query that data.

RediSearch uses compressed, inverted indexes for fast indexing with a low memory footprint.

RediSearch indexes enhance Redis by providing exact-phrase matching, fuzzy search, and numeric filtering, among many other features.

## Getting started

If you're just getting started with RediSearch, check out the [official RediSearch tutorial](https://github.com/RediSearch/redisearch-getting-started). Also, consider viewing our [RediSearch video explainer](https://www.youtube.com/watch?v=B10nHEdW3NA).

The fastest way to get up and running with RediSearch is by using the [RediSearch Docker image](https://hub.docker.com/r/redislabs/redisearch/).

## Trying RediSearch

To try RediSearch, either use the RediSearch Docker image, or [create a free Redis Cloud Essentials account](https://redislabs.com/try-free/) to get a RediSearch instance in the cloud.

### Docker image

The [RediSearch Docker image](https://hub.docker.com/r/redislabs/redisearch/) makes it easy to try RediSearch.

To create a local RediSearch container, run:

```sh
$ docker run -p 6379:6379 redislabs/redisearch:latest
```

To connect to this instance, run:

```sh
$ redis-cli
```

## Documentation

The [RediSearch documentation](https://oss.redislabs.com/redisearch/) provides a complete overview of RediSearch. Helpful sections include:

* The [RediSearch quick start](https://oss.redislabs.com/redisearch/Quick_Start/) 
* The [RediSearch command reference](https://oss.redislabs.com/redisearch/Commands/)
* References on features such as [aggregations](https://oss.redislabs.com/redisearch/Aggregations/), [higlights](https://oss.redislabs.com/redisearch/Highlight/), [stemming](https://oss.redislabs.com/redisearch/Stemming/), and [spelling correction](https://oss.redislabs.com/redisearch/Spellcheck/).

## Mailing list and forum

Got questions? Join us in [#redisearch on the Redis Discord](https://discord.gg/knMsnYmwXu) server. 

If you have a more detailed question, drop us a line on the [RediSearch Discussion Forum](http://forum.redislabs.com/c/modules/redisearch).

## Client libraries

You can use any standard Redis client library to run RediSearch commands, but it's easiest to use a library that wraps the RediSearch API. 

| Language | Library | Author | License | Stars |
|----------|---------|--------|---------|-------|
|Python | [redisearch-py][redisearch-py-url] | [Redis Labs][redisearch-py-author] | BSD | [![redisearch-py-stars]][redisearch-py-url] |
| Java (Jedis client library) | [JRediSearch][JRediSearch-url] | [Redis Labs][JRediSearch-author] | BSD | [![JRediSearch-stars]][JRediSearch-url]|
| Java (Lettuce client library) | [lettusearch][lettusearch-url] | [Redis Labs][lettusearch-author] | Apache-2.0 | [![lettusearch-stars]][lettusearch-url]|
| Java | [spring-redisearch][spring-redisearch-url] | [Redis Labs][spring-redisearch-author] | Apache-2.0 | [![spring-redisearch-stars]][spring-redisearch-url]|
| Java | [redis-modules-java][redis-modules-java-url] | [dengliming][redis-modules-java-author] | Apache-2.0 | [![redis-modules-java-stars]][redis-modules-java-url]|
| Go | [redisearch-go][redisearch-go-url] | [Redis Labs][redisearch-go-author] | BSD | [![redisearch-go-stars]][redisearch-go-url] | 
| JavaScript | [RedRediSearch][RedRediSearch-url] | [Kyle J. Davis][RedRediSearch-author] | MIT |[![RedRediSearch-stars]][RedRediSearch-url]|
| JavaScript | [redis-redisearch][redis-redisearch-url] | [Kyle J. Davis][redis-redisearch-author] | MIT | [![redis-redisearch-stars]][redis-redisearch-url]|
| TypeScript | [redis-modules-sdk][redis-modules-sdk-url] | [Dani Tseitlin][redis-modules-sdk-author] | BSD-3-Clause | [![redis-modules-sdk-stars]][redis-modules-sdk-url]|
| C# | [NRediSearch][NRediSearch-url] | [Marc Gravell][NRediSearch-author] | MIT | [![NRediSearch-stars]][NRediSearch-url] |
| PHP | [redisearch-php][redisearch-php-url] | [Ethan Hann][redisearch-php-author] | MIT | [![redisearch-php-stars]][redisearch-php-url] |
| Rust | [redisearch-api-rs][redisearch-api-rs-url] | [Redis Labs][redisearch-api-rs-author] | BSD | [![redisearch-api-rs-stars]][redisearch-api-rs-url] | API for Redis Modules written in Rust |
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

[redis-modules-sdk-url]: https://github.com/danitseitlin/redis-modules-sdk
[redis-modules-sdk-author]: https://github.com/danitseitlin
[redis-modules-sdk-stars]: https://img.shields.io/github/stars/danitseitlin/redis-modules-sdk.svg?style=social&amp;label=Star&amp;maxAge=2592000

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

[redisearch-api-rs-url]: https://github.com/RediSearch/redisearch-api-rs
[redisearch-api-rs-author]: https://redislabs.com
[redisearch-api-rs-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-api-rs.svg?style=social&amp;label=Star&amp;maxAge=2592000

## RediSearch features

* Full-Text indexing of multiple fields in Redis hashes
* Incremental indexing without performance loss
* Document ranking (using [tf-idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf), with optional user-provided weights)
* Field weighting
* Complex boolean queries with AND, OR, and NOT operators
* Prefix matching, fuzzy matching, and exact-phrase queries
* Support for [double-metaphone phonetic matching](https://oss.redislabs.com/redisearch/Phonetic_Matching/)
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Stemming-based query expansion in [many languages](https://oss.redislabs.com/redisearch/Stemming/) (using [Snowball](http://snowballstem.org/))
* Support for Chinese-language tokenization and querying (using [Friso](https://github.com/lionsoul2014/friso))
* Numeric filters and ranges
* Geospatial searches using [Redis geospatial indexing](https://redis.io/commands/georadius)
* A powerful aggregations engine
* Supports for all utf-8 encoded text
* Retrieve full documents, selected fields, or only the document IDs
* Sorting results (for example, by creation date)

## Cluster support

RediSearch has a distributed cluster version that scales to billions of documents across hundreds of servers. At the moment, distributed RediSearch is available as part of [Redis Enterprise Cloud](https://redislabs.com/redis-enterprise-cloud/overview/) and [Redis Enterprise Software](https://redislabs.com/redis-enterprise-software/overview/).

See [RediSearch on Redis Enterprise](https://redislabs.com/modules/redisearch/) for more information.

### License

 RediSearch is licensed under the [Redis Source Available License Agreement](LICENSE).
