---
title: "Client Libraries"
linkTitle: "Clients"
weight: 4
description: >
    List of RediSearch client libraries
---

RediSearch has several client libraries, written by the module authors and community members - abstracting the API in different programming languages.

While it is possible and simple to use the raw Redis commands API, in most cases it's easier to just use a client library abstracting it.

## Currently available Libraries

| Project | Language | License | Author | Stars |
|----------|---------|--------|---------|-------|
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