# RediSearch Administration Guide

RediSearch doesn't require any configuration to work, but there are a few things worth noting when running RediSearch on top of Redis.

## Persistence

RediSearch supports both RDB and AOF based persistence. For a pure RDB set-up, nothing special is needed beyond the standard Redis RDB configuration.

### AOF Persistence

While RediSearch supports working with AOF based persistence, as of version 1.1.0 it **does not support** "classic AOF" mode, which uses AOF rewriting. Instead, it only supports AOF with RDB preamble mode. In this mode, rewriting the AOF log just creates an RDB file, which is appended to. 

To enable AOF persistence with RediSearch, add the two following lines to your redis.conf:

```
appendonly yes
aof-use-rdb-preamble yes
``` 

## Master/Slave Replication

RediSearch supports replication inherently, and using a master/slave set-up, you can use slaves for high availability. On top of that, slaves can be used for searching, to load-balance read traffic. 

## Cluster Support

RediSearch will not work correctly on a cluster. The enterprise version of RediSearch, which is commercially available from Redis Labs, does support a cluster set up and scales to hundreds of nodes, billions of documents and terabytes of data. See the [Redis Labs Website](https://redislabs.com/redis-enterprise-documentation/developing/modules/redisearch/) for more details. 