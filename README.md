[![Discord](https://img.shields.io/discord/697882427875393627)](https://discord.gg/xTbqgTB)

| Total Coverage | Unit Tests | Flow Tests |
|----------------|------------|------------|
|[![codecov](https://codecov.io/gh/RediSearch/RediSearch/graph/badge.svg?token=bfZ02W6x3K)](https://codecov.io/gh/RediSearch/RediSearch)|[![codecov](https://codecov.io/gh/RediSearch/RediSearch/graph/badge.svg?token=bfZ02W6x3K&flag=unit)](https://codecov.io/gh/RediSearch/RediSearch?flags[0]=unit)|[![codecov](https://codecov.io/gh/RediSearch/RediSearch/graph/badge.svg?token=bfZ02W6x3K&flag=flow)](https://codecov.io/gh/RediSearch/RediSearch?flags[0]=flow)|

[![Latest Release](https://img.shields.io/github/v/release/RediSearch/RediSearch?label=latest%20release)](https://github.com/RediSearch/RediSearch/releases/latest)

[![Latest 2.8](https://img.shields.io/github/v/release/RediSearch/RediSearch?filter=v2.8*&label=latest%20maintenance%20release%20for%202.8)](https://github.com/RediSearch/RediSearch/releases?q=tag:v2.8%20draft:false)
[![Latest 2.6](https://img.shields.io/github/v/release/RediSearch/RediSearch?filter=v2.6*&label=latest%20maintenance%20release%20for%202.6)](https://github.com/RediSearch/RediSearch/releases?q=tag:v2.6%20draft:false)

# RediSearch

<img src="https://redis.io/docs/interact/search-and-query/img/logo.svg" title="RediSearch's Logo" width="300">

> [!NOTE]
> Starting with Redis 8, Redis Query Engine (RediSearch) is integral to Redis. You don't need to install this module separately.
>
> Therefore, we no longer release standalone versions of RediSearch.
>
> See https://github.com/redis/redis

## Overview

RediSearch is a [Redis module](https://redis.io/modules) that provides querying, secondary indexing, and full-text search for Redis. To use RediSearch, you first declare indexes on your Redis data. You can then use the RediSearch query language to query that data.

RediSearch uses compressed, inverted indexes for fast indexing with a low memory footprint.

RediSearch indexes enhance Redis by providing exact-phrase matching, fuzzy search, and numeric filtering, among many other features.

## Getting started

If you're just getting started with RediSearch, check out the [official RediSearch tutorial](https://github.com/RediSearch/redisearch-getting-started). Also, consider viewing our [RediSearch video explainer](https://www.youtube.com/watch?v=B10nHEdW3NA).

## Documentation

The [RediSearch documentation](https://redis.io/docs/interact/search-and-query/) provides a complete overview of RediSearch. Helpful sections include:

* The [RediSearch quick start](https://redis.io/docs/latest/develop/get-started/document-database/)
* The [RediSearch command reference](https://redis.io/commands/?group=search)
* References on features such as [aggregations](https://redis.io/docs/interact/search-and-query/search/aggregations/), [highlights](https://redis.io/docs/interact/search-and-query/advanced-concepts/highlight/), [stemming](https://redis.io/docs/interact/search-and-query/advanced-concepts/stemming/), and [spelling correction](https://redis.io/docs/interact/search-and-query/advanced-concepts/spellcheck/).
* [Vector search] (https://redis.io/docs/latest/develop/interact/search-and-query/query/vector-search/)

## Questions?

Got questions? Join us in [#redisearch on the Redis Discord](https://discord.gg/knMsnYmwXu) server.

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
* Geoshape indexing
* Vector similarity search - KNN, filtered KNN and range query

## Cluster support

RediSearch has a distributed cluster version that scales to billions of documents across hundreds of servers. At the moment, distributed RediSearch is available as part of [Redis Cloud](https://redis.com/redis-enterprise-cloud/overview/) and [Redis Enterprise Software](https://redis.com/redis-enterprise-software/overview/).

See [RediSearch on Redis Enterprise](https://redis.com/modules/redisearch/) for more information.

## License

Starting with Redis 8, RediSearch is licensed under your choice of: (i) Redis Source Available License 2.0 (RSALv2); (ii) the Server Side Public License v1 (SSPLv1); or (iii) the GNU Affero General Public License version 3 (AGPLv3). Please review the license folder for the full license terms and conditions. Prior versions remain subject to (i) and (ii).

## Code contributions

By contributing code to this Redis module in any form, including sending a pull request via GitHub, a code fragment or patch via private email or public discussion groups, you agree to release your code under the terms of the Redis Software Grant and Contributor License Agreement. Please see the CONTRIBUTING.md file in this source distribution for more information. For security bugs and vulnerabilities, please see SECURITY.md. 
