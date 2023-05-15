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

To try RediSearch, either use the RediSearch Docker image, or [create a free Redis Cloud Essentials account](https://redis.com/try-free/) to get a RediSearch instance in the cloud.

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

The [RediSearch documentation](https://redis.io/docs/stack/search/) provides a complete overview of RediSearch. Helpful sections include:

* The [RediSearch quick start](https://redis.io/docs/stack/search/Quick_Start/)
* The [RediSearch command reference](https://redis.io/docs/stack/search/Commands/)
* References on features such as [aggregations](https://redis.io/docs/stack/search/reference/aggregations), [highlights](https://redis.io/docs/stack/search/reference/highlight/), [stemming](https://redis.io/docs/stack/search/reference/stemming/), and [spelling correction](https://redis.io/docs/stack/search/reference/spellcheck/).

## Mailing list and forum

Got questions? Join us in [#redisearch on the Redis Discord](https://discord.gg/knMsnYmwXu) server.

If you have a more detailed question, drop us a line on the [RediSearch Discussion Forum](http://forum.redis.com/c/modules/redisearch).

## Client libraries

You can use any standard Redis client library to run RediSearch commands, but it's easiest to use a library that wraps the RediSearch API.

| Project | Language | License | Author | Stars | Comment |
|----------|---------|--------|---------|-------|------- |
| [jedis][jedis-url] | Java | MIT | [Redis][redis-url] | ![Stars][jedis-stars] | |
| [redis-py][redis-py-url] | Python | MIT | [Redis][redis-url] | ![Stars][redis-py-stars] | |
| [node-redis][node-redis-url] | Node.js | MIT | [Redis][redis-url] | ![Stars][node-redis-stars] | |
| [nredisstack][nredisstack-url] | .NET | MIT | [Redis][redis-url] | ![Stars][nredisstack-stars] | |
| [redis-om][redis-om-python-url] | Python | BSD | [Redis][redis-om-python-author] | [![redis-om-python-stars]][redis-om-python-url] |
| [lettusearch][lettusearch-url] | Java | Apache 2.0 | [Redis][lettusearch-author] | [![lettusearch-stars]][lettusearch-url] |
| [spring-redisearch][spring-redisearch-url] | Java | Apache 2.0 | [Redis][spring-redisearch-author] | [![spring-redisearch-stars]][spring-redisearch-url] |
| [redis-om-spring][redis-om-spring-url] | Java | BSD | [Redis][redis-om-spring-author] | [![redis-om-spring-stars]][redis-om-spring-url] |
| [redisearch-go][redisearch-go-url] | Go | BSD | [Redis][redisearch-go-author] | [![redisearch-go-stars]][redisearch-go-url] |
| [rueidis][rueidis-url] | Go | Apache 2.0 | [Rueian][rueidis-author] | [![rueidis-stars]][rueidis-url] |
| [Redis-om][redis-om-node-url] | JavaScript | BSD | [Redis][redis-om-node-author] | [![redis-om-node-stars]][redis-om-node-url] |
| [Redis.OM][redis-om-dotnet-url] | .NET | BSD | [Redis][redis-om-dotnet-author] | [![redis-om-dotnet-stars]][redis-om-dotnet-url] |
| [redisearch-php][redisearch-php-url] | PHP | MIT | [Ethan Hann][redisearch-php-author] | [![redisearch-php-stars]][redisearch-php-url] |
| [php-redisearch][php-redisearch-url] | PHP | MIT [MacFJA][php-redisearch-author] | MIT | [![php-redisearch-stars]][php-redisearch-url] |
| [redisearch-api-rs][redisearch-api-rs-url] | Rust | BSD | [Redis][redisearch-api-rs-author] | [![redisearch-api-rs-stars]][redisearch-api-rs-url] |
| [redi_search_rails][redi_search_rails-url] | Ruby | MIT | [Dmitry Polyakovsky][redi_search_rails-author] | [![redi_search_rails-stars]][redi_search_rails-url]|
| [redisearch-rb][redisearch-rb-url] | Ruby | MIT | [Victor Ruiz][redisearch-rb-author] | [![redisearch-rb-stars]][redisearch-rb-url]|
| [redi_search][redi_search-url] | Ruby | MIT | [Nick Pezza][redi_search-author] | [![redi_search-stars]][redi_search-url] |

[redis-url]: https://redis.com

[redis-py-url]: https://github.com/redis/redis-py
[redis-py-stars]: https://img.shields.io/github/stars/redis/redis-py.svg?style=social&amp;label=Star&amp;maxAge=2592000
[redis-py-package]: https://pypi.python.org/pypi/redis

[jedis-url]: https://github.com/redis/jedis
[jedis-stars]: https://img.shields.io/github/stars/redis/jedis.svg?style=social&amp;label=Star&amp;maxAge=2592000
[Jedis-package]: https://search.maven.org/artifact/redis.clients/jedis

[nredisstack-url]: https://github.com/redis/nredisstack
[nredisstack-stars]: https://img.shields.io/github/stars/redis/nredisstack.svg?style=social&amp;label=Star&amp;maxAge=2592000
[nredisstack-package]: https://www.nuget.org/packages/nredisstack/

[node-redis-url]: https://github.com/redis/node-redis
[node-redis-stars]: https://img.shields.io/github/stars/redis/node-redis.svg?style=social&amp;label=Star&amp;maxAge=2592000
[node-redis-package]: https://www.npmjs.com/package/redis


[redis-om-python-url]: https://github.com/redis/redis-om-python
[redis-om-python-author]: https://redis.com
[redis-om-python-stars]: https://img.shields.io/github/stars/redis/redis-om-python.svg?style=social&amp;label=Star&amp;maxAge=2592000

[lettusearch-url]: https://github.com/RediSearch/lettusearch
[lettusearch-author]: https://redis.com
[lettusearch-stars]: https://img.shields.io/github/stars/RediSearch/lettusearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[spring-redisearch-url]: https://github.com/RediSearch/spring-redisearch
[spring-redisearch-author]: https://redis.com
[spring-redisearch-stars]: https://img.shields.io/github/stars/RediSearch/spring-redisearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-om-spring-url]: https://github.com/redis/redis-om-spring
[redis-om-spring-author]: https://redis.com
[redis-om-spring-stars]: https://img.shields.io/github/stars/redis/redis-om-spring.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redisearch-go-url]: https://github.com/RediSearch/redisearch-go
[redisearch-go-author]: https://redis.com
[redisearch-go-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-go.svg?style=social&amp;label=Star&amp;maxAge=2592000

[rueidis-url]: https://github.com/rueian/rueidis
[rueidis-author]: https://github.com/rueian
[rueidis-stars]: https://img.shields.io/github/stars/rueian/rueidis.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-om-node-url]: https://github.com/redis/redis-om-node
[redis-om-node-author]: https://redis.com
[redis-om-node-stars]: https://img.shields.io/github/stars/redis/redis-om-node.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-om-dotnet-url]: https://github.com/redis/redis-om-dotnet
[redis-om-dotnet-author]: https://redis.com
[redis-om-dotnet-stars]: https://img.shields.io/github/stars/redis/redis-om-dotnet.svg?style=social&amp;label=Star&amp;maxAge=2592000

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

## RediSearch features

* Full-Text indexing of multiple fields in Redis hashes
* Incremental indexing without performance loss
* Document ranking (using [tf-idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf), with optional user-provided weights)
* Field weighting
* Complex boolean queries with AND, OR, and NOT operators
* Prefix matching, fuzzy matching, and exact-phrase queries
* Support for [double-metaphone phonetic matching](https://redis.io/docs/stack/search/reference/phonetic_matching/)
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
