[![Release](https://img.shields.io/github/v/release/redisearch/redisearch.svg?sort=semver)](https://github.com/RediSearch/RediSearch/releases)
[![CircleCI](https://circleci.com/gh/RediSearch/RediSearch/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/RediSearch/tree/master)
[![Dockerhub](https://img.shields.io/docker/pulls/redis/redis-stack-server?label=redis-stack-server)](https://hub.docker.com/r/redis/redis-stack-server/)
[![Codecov](https://codecov.io/gh/RediSearch/RediSearch/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/RediSearch)

# RediSearch
## Querying, secondary indexing, and full-text search for Redis
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redis.com/c/modules/redisearch/)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)

<img src="https://redis.io/docs/stack/search/img/logo.svg" alt="logo" width="300"/>

## Overview

RediSearch is a [Redis module](https://redis.io/modules) that provides querying, secondary indexing, and full-text search for Redis. To use RediSearch, you first declare indexes on your Redis data. You can then use the RediSearch query language to query that data.

RediSearch uses compressed, inverted indexes for fast indexing with a low memory footprint.

RediSearch indexes enhance Redis by providing exact-phrase matching, fuzzy search, and numeric filtering, among many other features.

## Getting started

If you're just getting started with RediSearch, check out the [official RediSearch tutorial](https://github.com/RediSearch/redisearch-getting-started). Also, consider viewing our [RediSearch video explainer](https://www.youtube.com/watch?v=B10nHEdW3NA).

The fastest way to get up and running with RediSearch is by using the [Redis Stack Docker image](https://hub.docker.com/r/redis/redis-stack/).

## Trying RediSearch

To try RediSearch, either use the RediSearch Docker image, or [create a free Redis Cloud Essentials account](https://redis.com/try-free/?utm_source=redisio&utm_medium=referral&utm_campaign=2023-09-try_free&utm_content=cu-redis_cloud_users) to get a RediSearch instance in the cloud.

### Docker image

The [Redis Stack Docker image](https://hub.docker.com/r/redis/redis-stack-server/) makes it easy to try RediSearch.

To create a local RediSearch container, run:

```sh
$ docker run -p 6379:6379 redis/redis-stack-server:latest
```

To connect to this instance, run:

```sh
$ redis-cli
```

## Documentation

The [RediSearch documentation](https://redis.io/docs/interact/search-and-query/) provides a complete overview of RediSearch. Helpful sections include:

* The [RediSearch quick start](https://redis.io/docs/interact/search-and-query/quickstart/)
* The [RediSearch command reference](https://redis.io/commands/?group=search)
* References on features such as [aggregations](https://redis.io/docs/interact/search-and-query/search/aggregations/), [highlights](https://redis.io/docs/interact/search-and-query/advanced-concepts/highlight/), [stemming](https://redis.io/docs/interact/search-and-query/advanced-concepts/stemming/), and [spelling correction](https://redis.io/docs/interact/search-and-query/advanced-concepts/spellcheck/).

## Mailing list and forum

Got questions? Join us in [#redisearch on the Redis Discord](https://discord.gg/knMsnYmwXu) server.

If you have a more detailed question, drop us a line on the [RediSearch Discussion Forum](http://forum.redis.com/c/modules/redisearch).

## Client libraries

### Official clients
| [<img width="75" src="https://user-images.githubusercontent.com/1655867/228534778-d0b41ce8-3ce4-4340-bd32-754f01ebed43.svg" />][dotnet-quickstart]  | [<img width="75" src="https://raw.githubusercontent.com/devicons/devicon/master/icons/java/java-plain-wordmark.svg" />][java-quickstart] | [<img width="75" src="https://raw.githubusercontent.com/devicons/devicon/master/icons/nodejs/nodejs-original-wordmark.svg" />][nodejs-quickstart]   | [<img width="75" src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original-wordmark.svg" />][python-quickstart]  |
|---|------------------------------------------------------------------------------------------------------------------------------------------|---|---|
|  [NRedisStack][dotnet-quickstart] | [Jedis][java-quickstart]                                                                                                                 | [node-redis][nodejs-quickstart]  |  [redis-py][python-quickstart] |
|  [Redis.OM][dotnet-om] | [Redis OM Spring][java-om]                                                                                                               | [redis-om-node][nodejs-om]  |  [redis-om][python-om] |

[dotnet-quickstart]: https://redis.io/docs/clients/dotnet/
[dotnet-om]: https://github.com/redis/redis-om-dotnet

[java-quickstart]: https://redis.io/docs/clients/java/
[java-om]: https://github.com/redis/redis-om-spring

[nodejs-quickstart]: https://redis.io/docs/clients/nodejs/
[nodejs-om]: https://github.com/redis/redis-om-node

[python-quickstart]: https://redis.io/docs/clients/python/
[python-om]: https://github.com/redis/redis-om-python

### Community-maintained clients

| Project | Language | License | Author                                         | Stars |
|----------|---------|--------|------------------------------------------------|-------|
| [redisson][redisson-url] | Java | MIT | [Redisson][redisson-url] | ![Stars][redisson-stars] | |
| [redisearch-go][redisearch-go-url] | Go | BSD | [Redis][redisearch-go-author]                  | [![redisearch-go-stars]][redisearch-go-url] |
| [rueidis][rueidis-url] | Go | Apache 2.0 | [Rueian][rueidis-author]                       | [![rueidis-stars]][rueidis-url] |
| [redisearch-php][redisearch-php-url] | PHP | MIT | [Ethan Hann][redisearch-php-author]            | [![redisearch-php-stars]][redisearch-php-url] |
| [php-redisearch][php-redisearch-url] | PHP | MIT | [MacFJA][php-redisearch-author]                | [![php-redisearch-stars]][php-redisearch-url] |
| [redisearch-api-rs][redisearch-api-rs-url] | Rust | BSD | [Redis][redisearch-api-rs-author]              | [![redisearch-api-rs-stars]][redisearch-api-rs-url] |
| [redi_search_rails][redi_search_rails-url] | Ruby | MIT | [Dmitry Polyakovsky][redi_search_rails-author] | [![redi_search_rails-stars]][redi_search_rails-url]|
| [redisearch-rb][redisearch-rb-url] | Ruby | MIT | [Victor Ruiz][redisearch-rb-author] | [![redisearch-rb-stars]][redisearch-rb-url]|
| [redi_search][redi_search-url] | Ruby | MIT | [Nick Pezza][redi_search-author] | [![redi_search-stars]][redi_search-url] |
| [coredis][coredis-url] | Python | MIT | [Ali-Akber Saifee][coredis-author] | [![coredis-stars]][coredis-url] | [Documentation][coredis-documentation]

[redis-url]: https://redis.com

[redisson-url]: https://github.com/redisson/redisson
[redisson-stars]: https://img.shields.io/github/stars/redisson/redisson.svg?style=social&amp;label=Star&amp;maxAge=2592000
[redisson-package]: https://central.sonatype.com/artifact/org.redisson/redisson

[redisearch-go-url]: https://github.com/RediSearch/redisearch-go
[redisearch-go-author]: https://redis.com
[redisearch-go-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-go.svg?style=social&amp;label=Star&amp;maxAge=2592000

[rueidis-url]: https://github.com/rueian/rueidis
[rueidis-author]: https://github.com/rueian
[rueidis-stars]: https://img.shields.io/github/stars/rueian/rueidis.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redisearch-php-url]: https://github.com/ethanhann/redisearch-php
[redisearch-php-author]: https://github.com/ethanhann
[redisearch-php-stars]: https://img.shields.io/github/stars/ethanhann/redisearch-php.svg?style=social&amp;label=Star&amp;maxAge=2592000

[php-redisearch-url]: https://github.com/MacFJA/php-redisearch
[php-redisearch-author]: https://github.com/MacFJA
[php-redisearch-stars]: https://img.shields.io/github/stars/MacFJA/php-redisearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

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
[redisearch-api-rs-author]: https://redis.com
[redisearch-api-rs-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-api-rs.svg?style=social&amp;label=Star&amp;maxAge=2592000

[coredis-url]: https://github.com/alisaifee/coredis
[coredis-author]: https://github.com/alisaifee
[coredis-stars]: https://img.shields.io/github/stars/alisaifee/coredis.svg?style=social&amp;label=Star&amp;maxAge=2592000
[coredis-documentation]: https://coredis.readthedocs.io/en/stable/handbook/modules.html#redisearch

## RediSearch features

* Full-Text indexing of multiple fields in Redis hashes
* Incremental indexing without performance loss
* Document ranking (using [tf-idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf), with optional user-provided weights)
* Field weighting
* Complex boolean queries with AND, OR, and NOT operators
* Prefix matching, fuzzy matching, and exact-phrase queries
* Support for [double-metaphone phonetic matching](https://redis.io/docs/interact/search-and-query/advanced-concepts/phonetic_matching/)
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Stemming-based query expansion in [many languages](https://redis.io/docs/stack/search/reference/stemming/) (using [Snowball](http://snowballstem.org/))
* Support for Chinese-language tokenization and querying (using [Friso](https://github.com/lionsoul2014/friso))
* Numeric filters and ranges
* Geospatial searches using [Redis geospatial indexing](/commands/georadius)
* A powerful aggregations engine
* Supports for all utf-8 encoded text
* Retrieve full documents, selected fields, or only the document IDs
* Sorting results (for example, by creation date)

## Cluster support

RediSearch has a distributed cluster version that scales to billions of documents across hundreds of servers. At the moment, distributed RediSearch is available as part of [Redis Enterprise Cloud](https://redis.com/redis-enterprise-cloud/overview/) and [Redis Enterprise Software](https://redis.com/redis-enterprise-software/overview/).

See [RediSearch on Redis Enterprise](https://redis.com/modules/redisearch/) for more information.

### License

RediSearch is licensed under the [Redis Source Available License 2.0 (RSALv2)](https://redis.com/legal/rsalv2-agreement) or the [Server Side Public License v1 (SSPLv1)](https://www.mongodb.com/licensing/server-side-public-license).
