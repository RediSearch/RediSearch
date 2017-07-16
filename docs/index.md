<center>![logo.png](logo.png)</center>

# RediSearch - Redis Powered Search Engine
# RediSearch - Redis 技术支持的搜索引擎

>:RediSearch is a Full-Text search over Redis, developed by RedisLabs. 
>:RediSearch 是一个建立在Redis上的全文搜索引擎, 由RedisLabs开发.

>:The source code is available at [https://github.com/RedisLabsModules/RediSearch](https://github.com/RedisLabsModules/RediSearch).
>:源代码托管在: [https://github.com/RedisLabsModules/RediSearch](https://github.com/RedisLabsModules/RediSearch).

### Latest Release: [0.18 (Preview)](https://github.com/RedisLabsModules/RediSearch/releases/tag/v0.18)
### 最新的发布版本: [0.18 (Preview)](https://github.com/RedisLabsModules/RediSearch/releases/tag/v0.18)

## Overview
## 概述

>:Redisearch impements a search engine on top of redis, but unlike other redis 
>:search libraries, it does not use internal data structures like sorted sets.
>:Redisearch是基于redis所实现的一个搜索引擎, 但是区别于其他redis搜索类库, 
>:它不是使用redis自带的内部数据结构， 比如sorted set

>:This also enables more advanced features, like exact phrase matching and numeric filtering for text queries, 
>:that are not possible or efficient with traditional redis search approache
>:正是因为如此redisearch带来了更多高级的功能, 比如准确的短语匹配和文本查询时的数字过滤，<br>这些也是传统redis搜索所不能或者高效达成的功能

## Client Libraries
## 客户端类库

* **Python**: [https://github.com/RedisLabs/redisearch-py](https://github.com/RedisLabs/redisearch-py)

* **Java**: [https://github.com/RedisLabs/JRediSearch](https://github.com/RedisLabs/JRediSearch)

### Community Libraries:
### 社区版类库:

* **PHP**: [https://github.com/ethanhann/redisearch-php](https://github.com/ethanhann/redisearch-php) (By Ethan Hann)

* **Ruby**: [https://github.com/vruizext/redisearch-rb](https://github.com/vruizext/redisearch-rb) (By Victor Ruiz)

* **Ruby on Rails**: [https://github.com/dmitrypol/redi_search_rails](https://github.com/dmitrypol/redi_search_rails) (By Dmitry Polyakovsky)

* **.Net**: [https://libraries.io/nuget/NRediSearch](https://libraries.io/nuget/NRediSearch) (By Marc Gravell)

## Primary Features:
## 主要特征:

* Full-Text indexing of multiple fields in documents.
* 多文档、多字段的全文索引.

* Incremental indexing without performance loss.
* 无性能损失的增量式索引.

* Document ranking (provided manually by the user at index time).
* 文档排序 (在添加索引的时候需要人工的指定).

* Complex boolean queries with AND, OR, NOT operators between sub-queries.
* 在子查询中带AND，OR，NOT操作符的复杂的boolean查询.

* Optional query clauses.
* 可选的查询子语句.

* Prefix based searches.
* 基于前缀搜索.

* Field weights.
* 字段权重支持.

* Auto-complete suggestions (with fuzzy prefix suggestions)
* 自动补全建议 (模糊前缀建议).

* Exact Phrase Search, Slop based search.
* 精确短语搜索，基于Slop的搜索.

* Stemming based query expansion in [many languages](/Stemming/) (using [Snowball](http://snowballstem.org/)).
* [多语言](/Stemming/)的查询扩展是基于分词的 (使用 [Snowball](http://snowballstem.org/)).

* Support for custom functions for query expansion and scoring (see [Extensions](/Extensions)).
* 支持自定义的查询扩展和评分方法 (参考 [扩展](/Extensions)).

* Limiting searches to specific document fields (up to 8 fields supported).
* 支持对执行文档字段的查询(最多支持8个).

* Numeric filters and ranges.
* 支持数字过滤和范围查询.

* Geo filtering using Redis' own Geo-commands. 
* 使用redis自己的地理位置命令支持地理位置过滤.

* Supports any utf-8 encoded text.
* 支持utf-8.

* Retrieve full document content or just ids
* 支持检索全部文档内容或者仅仅是id搜索.

* Automatically index existing HASH keys as documents.
* 支持将已经存在的hash key当做文档自动索引.

* Document deletion and updating with index garbage collection.
* 支持使用索引垃圾回收对文档删除，更新.


