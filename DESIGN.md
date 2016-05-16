# RediSearch internal design

RediSearch implements inverted indexes on top of redis, but unlike previous implementations of redis inverted indexes,
it uses custom data encoding, that allows more memory and CPU efficient searches, and more advanced search features.

This document details some of the design choices and how these features are implemented.

## Intro: Redis String DMA

The main feature that this module takes advantage of, is Redis Modules Strings DMA, or Direct Memory Access.

This features is simple yet very powerful. It basically allows modules to allocate data on Redis string keys,
then get a direct pointer to the data allocated by this key, without copying or serializing it. 

This allows very fast access to huge amounts of memory, and since from the module's perspective, the string
value is exposed simply as `char *`, it can be cast to any data structure. 

You simply call `RedisModule_StringTruncate` to resize a memory chunk to the size needed, and `RedisModule_StringDMA` 
to get direct access to the memory in that key. 

We use this API in the module mainly to encode inverted indexes, but for other auxiliary data structures used. 

A generic "Buffer" implementation using DMA strings can be found in `redis_buffer.c`. It automatically resizes
the redis string it uses as raw memory, when the capacity needs to grow.
 
## Inverted index encoding



## Document and result ranking

## Document data saving

## Query Execution Engine

