# RediSearch Client Libraries

RediSearch has several client libraries, written by the module authors and community members - abstracting the API in different programming languages. 

While it is possible and simple to use the raw Redis commands API, in most cases it's easier to just use a client library abstracting it. 

## Currently available Libraries

| Language | Library | Author | License | Stars |
|----------|---------|--------|---------|-------|
|Python | [redisearch-py][redisearch-py-url] | [Redis Inc][redisearch-py-author] | BSD | [![redisearch-py-stars]][redisearch-py-url] |
| Python | [redis-om][redis-om-python-url] | [Redis][redis-om-python-author] | BSD-3-Clause | [![redis-om-python-stars]][redis-om-python-url] |
| Java (Jedis client library) | [JRediSearch][JRediSearch-url] | [Redis Inc][JRediSearch-author] | BSD | [![JRediSearch-stars]][JRediSearch-url]|
| Java | [redis-om-spring][redis-om-spring-url] | [Redis][redis-om-spring-author] | BSD-3-Clause | [![redis-om-spring-stars]][redis-om-spring-url] |
| Java (Lettuce client library) | [LettuceMod][lettucemod-url] | [Redis Inc][lettucemod-author] | Apache-2.0 | [![lettucemod-stars]][lettucemod-url]|
| Java | [Spring LettuceMod][lettucemod-url] | [Redis Labs][lettucemod-author] | Apache-2.0 | [![lettucemod-stars]][lettucemod-url]|
| Java | [redis-modules-java][redis-modules-java-url] | [dengliming][redis-modules-java-author] | Apache-2.0 | [![redis-modules-java-stars]][redis-modules-java-url]|
| Go | [redisearch-go][redisearch-go-url] | [Redis Inc][redisearch-go-author] | BSD | [![redisearch-go-stars]][redisearch-go-url] | 
| JavaScript | [RedRediSearch][RedRediSearch-url] | [Kyle J. Davis][RedRediSearch-author] | MIT |[![RedRediSearch-stars]][RedRediSearch-url]|
| JavaScript | [redis-redisearch][redis-redisearch-url] | [Kyle J. Davis][redis-redisearch-author] | MIT | [![redis-redisearch-stars]][redis-redisearch-url]|
| JavaScript | [Redis-om][redis-om-node-url] | [Redis][redis-om-node-author] | BSD-3-Clause | [![redis-om-node-stars]][redis-om-node-url] |
| TypeScript | [redis-modules-sdk][redis-modules-sdk-url] | [Dani Tseitlin][redis-modules-sdk-author] | BSD-3-Clause | [![redis-modules-sdk-stars]][redis-modules-sdk-url]|
| C# | [NRediSearch][NRediSearch-url] | [Marc Gravell][NRediSearch-author] | MIT | [![NRediSearch-stars]][NRediSearch-url] |
| C# | [RediSearchClient][RediSearchClient-url] | [Tom Hanks][RediSearchClient-author] | MIT | [![RediSearchClient-stars]][RediSearchClient-url] |
| C# | [Redis.OM][redis-om-dotnet-url] | [Redis][redis-om-dotnet-author] | BSD-3-Clause | [![redis-om-dotnet-stars]][redis-om-dotnet-url] |
| PHP | [php-redisearch][php-redisearch-url] | [MacFJA][php-redisearch-author] | MIT | [![php-redisearch-stars]][php-redisearch-url] |
| PHP | [redisearch-php][redisearch-php-url] (for RediSearch v1)| [Ethan Hann][redisearch-php-author] | MIT | [![redisearch-php-stars]][redisearch-php-url] |
| PHP | [Redisearch][front-redisearch-url] (for RediSearch v2)| [Front][front-redisearch-author] | MIT | [![front-redisearch-stars]][front-redisearch-url] |
| Ruby on Rails | [redi_search_rails][redi_search_rails-url] | [Dmitry Polyakovsky][redi_search_rails-author] | MIT | [![redi_search_rails-stars]][redi_search_rails-url]|
| Ruby | [redisearch-rb][redisearch-rb-url] | [Victor Ruiz][redisearch-rb-author] | MIT | [![redisearch-rb-stars]][redisearch-rb-url]|
| Ruby | [redi_search][redi_search-url] | [Nick Pezza][redi_search-author] | MIT | [![redi_search-stars]][redi_search-url] |

[redisearch-py-url]: https://github.com/RediSearch/redisearch-py
[redisearch-py-author]: https://redis.com
[redisearch-py-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-py.svg?style=social&amp;label=Star&amp;maxAge=2592000

[JRediSearch-url]: https://github.com/RediSearch/JRediSearch
[JRediSearch-author]: https://redis.com
[JRediSearch-stars]: https://img.shields.io/github/stars/RediSearch/JRediSearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[lettucemod-url]: https://github.com/redis-developer/lettucemod
[lettucemod-author]: https://redis.com
[lettucemod-stars]: https://img.shields.io/github/stars/redis-developer/lettucemod.svg?style=social&amp;label=Star&amp;maxAge=2592000

[spring-redisearch-url]: https://github.com/RediSearch/spring-redisearch
[spring-redisearch-author]: https://redis.com
[spring-redisearch-stars]: https://img.shields.io/github/stars/RediSearch/spring-redisearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-modules-java-url]: https://github.com/dengliming/redis-modules-java
[redis-modules-java-author]: https://github.com/dengliming
[redis-modules-java-stars]: https://img.shields.io/github/stars/dengliming/redis-modules-java.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redisearch-go-url]: https://github.com/RediSearch/redisearch-go
[redisearch-go-author]: https://redis.com
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
[NRediSearch-stars]: https://img.shields.io/github/stars/StackExchange/NRediSearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[RediSearchClient-url]: https://www.nuget.org/packages/RediSearchClient
[RediSearchClient-author]: https://github.com/tombatron
[RediSearchClient-stars]: https://img.shields.io/github/stars/tombatron/RediSearchClient.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redisearch-php-url]: https://github.com/ethanhann/redisearch-php
[redisearch-php-author]: https://github.com/ethanhann
[redisearch-php-stars]: https://img.shields.io/github/stars/ethanhann/redisearch-php.svg?style=social&amp;label=Star&amp;maxAge=2592000

[php-redisearch-url]: https://github.com/MacFJA/php-redisearch
[php-redisearch-author]: https://github.com/MacFJA
[php-redisearch-stars]: https://img.shields.io/github/stars/MacFJA/php-redisearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[front-redisearch-url]: https://github.com/front/redisearch
[front-redisearch-author]: https://github.com/front/
[front-redisearch-stars]: https://img.shields.io/github/stars/front/redisearch.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redi_search_rails-url]: https://github.com/dmitrypol/redi_search_rails
[redi_search_rails-author]: https://github.com/dmitrypol
[redi_search_rails-stars]: https://img.shields.io/github/stars/dmitrypol/redi_search_rails.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redisearch-rb-url]: https://github.com/vruizext/redisearch-rb
[redisearch-rb-author]: https://github.com/vruizext
[redisearch-rb-stars]: https://img.shields.io/github/stars/vruizext/redisearch-rb.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redi_search-url]: https://github.com/npezza93/redi_search
[redi_search-author]: https://github.com/npezza93
[redi_search-stars]: https://img.shields.io/github/stars/npezza93/redi_search.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-om-python-url]: https://github.com/redis/redis-om-python
[redis-om-python-author]: https://redis.com
[redis-om-python-stars]: https://img.shields.io/github/stars/redis/redis-om-python.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-om-spring-url]: https://github.com/redis/redis-om-spring
[redis-om-spring-author]: https://redis.com
[redis-om-spring-stars]: https://img.shields.io/github/stars/redis/redis-om-spring.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-om-node-url]: https://github.com/redis/redis-om-node
[redis-om-node-author]: https://redis.com
[redis-om-node-stars]: https://img.shields.io/github/stars/redis/redis-om-node.svg?style=social&amp;label=Star&amp;maxAge=2592000

[redis-om-dotnet-url]: https://github.com/redis/redis-om-dotnet
[redis-om-dotnet-author]: htts://redis.com
[redis-om-dotnet-stars]: https://img.shields.io/github/stars/redis/redis-om-dotnet.svg?style=social&amp;label=Star&amp;maxAge=2592000

## Other available Libraries

| Language | Library | Author | License | Stars | Comments |
|----------|---------|--------|---------|-------|----------|
| Rust | [redisearch-api-rs][redisearch-api-rs-url] | [Redis Inc][redisearch-api-rs-author] | BSD | [![redisearch-api-rs-stars]][redisearch-api-rs-url] | API for Redis Modules written in Rust |

[redisearch-api-rs-url]: https://github.com/RediSearch/redisearch-api-rs
[redisearch-api-rs-author]: https://redis.com
[redisearch-api-rs-stars]: https://img.shields.io/github/stars/RediSearch/redisearch-api-rs.svg?style=social&amp;label=Star&amp;maxAge=2592000
