---
title: "General Administration"
linkTitle: "General Administration"
weight: 1
description: >
    General Administration of the Search and Query module
---

# Search and Query Administration Guide

Search and Query doesn't require any configuration to work, but there are a few things worth noting when running it on top of Redis.

## Persistence

Search and Query supports both RDB and AOF based persistence. For a pure RDB set-up, nothing special is needed beyond the standard Redis RDB configuration.

### AOF Persistence

While Search and Query supports working with AOF based persistence, as of version 1.1.0 it **does not support** "classic AOF" mode, which uses AOF rewriting. Instead, it only supports AOF with RDB preamble mode. In this mode, rewriting the AOF log just creates an RDB file, which is appended to. 

To enable AOF persistence with Search and Query, add the two following lines to your redis.conf:

```
appendonly yes
aof-use-rdb-preamble yes
``` 

## Master/Slave Replication

Search and Query supports replication inherently, and using a master/replica set-up, you can use replicas for high availability. On top of that, replicas can be used for searching to load-balance read traffic. 

## Cluster Support

Search and Query will not work correctly on a cluster. The enterprise version of Search and Query, which is commercially available from Redis Inc., supports a cluster set up and scales to hundreds of nodes, billions of documents and terabytes of data. See the [Redis Labs Website](https://docs.redis.com/latest/modules/redisearch/) for more details. 
