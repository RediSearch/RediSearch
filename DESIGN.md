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
See [https://github.com/RedisLabs/RedisModulesSDK/blob/master/FUNCTIONS.md#redismodule_stringdma](https://github.com/RedisLabs/RedisModulesSDK/blob/master/FUNCTIONS.md#redismodule_stringdma)

We use this API in the module mainly to encode inverted indexes, and for other auxiliary data structures besides that. 

A generic "Buffer" implementation using DMA strings can be found in [redis_buffer.c](src/redis_buffer.c). It automatically resizes
the redis string it uses as raw memory, when the capacity needs to grow.
 
## Inverted index encoding

An [Inverted Index](https://en.wikipedia.org/wiki/Inverted_index) is the data structure at the heart of all search engines. The idea is simple - per each
word or search term, we save a list of all the documents it appears in, and other data, such as term frequency,
the offsets where the term appeared in the document, and more. Offsets are used for "exact match" type searches,
or for ranking of results.

When a search is performed, we need to either traverse such an index, or intersect or union two or more indexes. 
Classic redis implementations of search engines use sorted sets as inverted indexes. This works, but has great memory
overhead, and also does not allow for encoding of offsets, as explained above.

RediSearch uses String DMA (see above) to efficiently encode inverted indexes. 
It combines [Delta Encoding](https://en.wikipedia.org/wiki/Delta_encoding) and 
[Varint Encoding](https://developers.google.com/protocol-buffers/docs/encoding#varints) to encode entries, 
minimizing space used for indexes, while keeping decompression and traversal efficient. 

For each "hit" (document/word entry), we encode:

* The document Id as a delta from the previous document.
* The term frequency, factored by the document's rank (see below)
* Flags, that can be used to filter only specific fields or other user defined properties.
* An Offset Vector, of all the document offsets of the word.

> Note: document ids as entered by the user are converted to internal incremental document ids, that allow 
> delta encoding to be efficient, and let the inverted indexes be sorted by document id.

This allows for a single index hit entry to be encoded in as little as 6 bytes 
(Note that this is the best case. depending on the number of occurrences of the word in the document, this can get much higher).

To optimize searches, we keep two additional auxiliary data structures in different DMA string keys:
 
1. **Skip Index**: We keep a table of the index offset of 1/50 of the index entries. This allows faster lookup when intersecting inverted indexes, as not the entire list must be traversed.
2. **Score Index**: In simple single-word searches, there is no real need to traverse all the results, just the top N results the user is intersted in. 
So we keep an auxiliary index of the top 20 or so entries for each term, and use them when applicable. 

## Document and result ranking

Each document entered to the engine using `FT.ADD`, has a user assigned rank, between 0 and 1.0. This is used in
combination with [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) scoring of each word, to rank the results. 

As an optimization, each inverted index hit is encoded with TF*Document_rank as its score, and only IDF is applied
during searches. This may change in the future.

On top of that, in the case of intersection queries, we take the minimal distance between the terms in the query,
and factor that into the ranking. The closest the terms are to each other, the better the result.

When searching, we keep a priority queue of the top N results requested, and eventually return them, sorted by rank. 

## Index Specs and field weights

When creating an "index" using `FT.CREATE`, the user specifies the fields to be indexed, and their respective weights. 
This can be used to give some document fields, like a title, more weight in ranking results. 

For example: 
```
FT.CREATE my_index title 10.0 body 1.0 url 2.0
```

Will create an index on fields named title, body and url, with scores of 10, 1 and 2 respectively.
 

When documents are indexed, the weights are taken from the saved *Index Spec*, that is stored in a special redis key,
and only fields that are specified in this spec are indexed.

## Document data storage

It is not mandatory to save the document data when indexing a document (specifying `NOSAVE` for `FT.ADD` will cause
the document to be indexed but not saved). 

If the user does save the document, we simply create a HASH key in redis, containing all fields (including ones not indexed), 
and upon search, we simply perform an `HGETALL` query on each retrieved document, returning its entire data. 

**TODO**: Document snippets should be implemented down the road,

## Query Execution Engine

We use a chained-iterator based approach to query execution, similar to [Python generators](https://wiki.python.org/moin/Generators) in concept.

We simply chain iterators that yield index hits. Those can be:

1. **Read Iterators**, reading hits one by one from an inverted index. i.e. `hello`
2. **Intersect Iterators**, aggregating two or more iterators, yielding only their intersection points. i.e. `hello AND world`
3. **Exact Intersect Iterators** - same as above, but yielding results only if the intersection is an exact phrase. i.e. `hello NEAR world`
4. **Union Iterators** - combining two or more iterators, and yielding a union of their hits. i.e. `hello OR world`

These are combined based on the query as an execution plan that is evaluated lazily. For example:

```
hello ==> read("hello")

hello world ==> intersect( read("hello"), read("world") ) 

"hello world" ==> exact_intersect( read("hello"), read("world") )

"hello world" foo ==> intersect( 
                            exact_intersect(
                                read("hello"), 
                                read("world")
                            ), 
                            read("foo")
                      ) 
```

All these iterators are lazy evaluated, entry by entry, with constant memory overhead. 

The "root" iterator is read by the query execution engine, and filtered for the top N results in it.