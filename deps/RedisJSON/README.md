[![GitHub issues](https://img.shields.io/github/release/RedisJSON/RedisJSON.svg)](https://github.com/RedisJSON/RedisJSON/releases/latest)
[![CircleCI](https://circleci.com/gh/RedisJSON/RedisJSON/tree/master.svg?style=svg)](https://circleci.com/gh/RedisJSON/RedisJSON/tree/master)
[![macos](https://github.com/RedisJSON/RedisJSON/workflows/macos/badge.svg)](https://github.com/RedisJSON/RedisJSON/actions?query=workflow%3Amacos)
[![Dockerhub](https://img.shields.io/docker/pulls/redis/redis-stack-server?label=redis-stack-server)](https://hub.docker.com/r/redis/redis-stack-server/)
[![Codecov](https://codecov.io/gh/RedisJSON/RedisJSON/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisJSON/RedisJSON)

# RedisJSON

[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/QUkjSsk)

<img src="docs/docs/images/logo.svg" alt="logo" width="300"/>

> [!NOTE]
> Starting with Redis 8, the JSON data structure is integral to Redis. You don't need to install this module separately.
>
> We no longer release standalone versions of RedisJSON.
>
> See https://github.com/redis/redis

> [!NOTE]
> 32 bit systems are not supported.

## Overview

RedisJSON is a [Redis](https://redis.io/) module that implements [ECMA-404 The JSON Data Interchange Standard](https://json.org/) as a native data type. It allows storing, updating, and fetching JSON values from Redis keys (documents).

## Primary features

* Full support of the JSON standard
* [JSONPath](https://goessner.net/articles/JsonPath/) syntax for selecting elements inside documents
* Documents are stored as binary data in a tree structure, allowing fast access to sub-elements
* Typed atomic operations for all JSON value types
* Secondary index support when combined with [RediSearch](https://redis.io/docs/latest/develop/interact/search-and-query/)

## Documentation

Read the docs at <https://redis.io/docs/latest/develop/data-types/json/>

## License

Starting with Redis 8, RedisJSON is licensed under your choice of: (i) Redis Source Available License 2.0 (RSALv2); (ii) the Server Side Public License v1 (SSPLv1); or (iii) the GNU Affero General Public License version 3 (AGPLv3). Please review the license folder for the full license terms and conditions. Prior versions remain subject to (i) and (ii).

## Code contributions

By contributing code to this Redis module in any form, including sending a pull request via GitHub, a code fragment or patch via private email or public discussion groups, you agree to release your code under the terms of the Redis Software Grant and Contributor License Agreement. Please see the CONTRIBUTING.md file in this source distribution for more information. For security bugs and vulnerabilities, please see SECURITY.md. 
