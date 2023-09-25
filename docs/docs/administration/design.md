---
title: "Internal design"
linkTitle: "Internal design"
weight: 1
description: >
    Details about design choices and implementations
aliases:    
    - /docs/stack/search/design/design/    
---

# Internal design

Redis Stack implements inverted indexes on top of Redis, but unlike previous implementations of Redis inverted indexes, it uses a custom data encoding that allows more memory and CPU efficient searches, and more advanced search features.

This document details some of the design choices and how these features are implemented.

## Intro: Redis String DMA

The main feature that this module takes advantage of is Redis Modules Strings Direct Memory Access (DMA).

This feature is simple, yet very powerful. It allows modules to allocate data on Redis string keys, and then get direct pointers to the data allocated by the keys without copying or serializing.

This allows very fast access to huge amounts of memory. From the module's perspective, the string value is exposed simply as `char *`, meaning it can be cast to any data structure.

You simply call `RedisModule_StringTruncate` to resize a memory chunk to the size needed. Then you call `RedisModule_StringDMA` to get direct access to the memory in that key. See [https://github.com/RedisLabs/RedisModulesSDK/blob/master/FUNCTIONS.md#redismodule_stringdma](https://github.com/RedisLabs/RedisModulesSDK/blob/master/FUNCTIONS.md#redismodule_stringdma)

This API is used in the module mainly to encode inverted indexes, and also for other auxiliary data structures.

A generic "Buffer" implementation using DMA strings can be found in [buffer.c](https://github.com/RediSearch/RediSearch/blob/master/src/buffer.c). It automatically resizes the Redis string it uses as raw memory when the capacity needs to grow.

## Inverted index encoding

An [inverted index](https://en.wikipedia.org/wiki/Inverted_index) is the data structure at the heart of all search engines. The idea is simple. For each word or search term, a list of all the documents it appears in is kept. Other data is kept as well, such as term frequency, and the offsets where a term appeared in the document. Offsets are used for exact match type searches, or for ranking of results.

When a search is performed, either a single index is traversed, or the intersection or union of two or more indexes is traversed. Classic Redis implementations of search engines use sorted sets as inverted indexes. This works but has significant memory overhead, and it also does not allow for encoding of offsets, as explained above.

Redis Stack uses string DMA (see above) to efficiently encode inverted indexes. It combines [delta encoding](https://en.wikipedia.org/wiki/Delta_encoding) and [varint encoding](https://developers.google.com/protocol-buffers/docs/encoding#varints) to encode entries, minimizing space used for indexes, while keeping decompression and traversal efficient.

For each hit (document/word entry), the following items are encoded:

* The document ID as a delta from the previous document.
* The term frequency, factored by the document's rank (see below).
* Flags, that can be used to filter only specific fields or other user-defined properties.
* An offset vector of all the document offsets of the word.

{{% alert title="Note" color="info" %}}
Document IDs as entered by the user are converted to internal incremental document IDs, that allow delta encoding to be efficient and let the inverted indexes be sorted by document ID.
{{% /alert %}}

This allows for a single index hit entry to be encoded in as little as 6 bytes. Note: this is the best case. Depending on the number of occurrences of the word in the document, this can get much higher.

To optimize searches, two additional auxiliary data structures are kept in different DMA string keys:

1. **Skip index**: a table of the index offset of 1/50th of the index entries. This allows faster lookup when intersecting inverted indexes, as the entire list doesn't need to be traversed.
2. **Score index**: In simple single-word searches, there is no real need to traverse all the results, just the top N results the user is interested in. So an auxiliary index of the top 20 or so entries is stored for each term, which are used when applicable.

## Document and result ranking

Each document entered to the engine using `FT.ADD` has a user assigned rank between 0.0 and 1.0. This is used in combination with a [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) scoring of each word to rank the results.

As an optimization, each inverted index hit is encoded with `TF * Document_rank` as its score, and only IDF is applied during searches. This may change in the future.

On top of that, in the case of intersection queries, the minimal distance between the terms in the query is factored into the ranking. The closest the terms are to each other, the better the result.

When searching, priority queue of the top N results requested is maintained, which are eventually returned, sorted by rank.

## Index ppecs and field weights

When creating an "index" using `FT.CREATE`, the user specifies the fields to be indexed and their respective weights. This can be used to give some document fields, like a title, more weight in ranking results.

For example:

```
FT.CREATE my_index title 10.0 body 1.0 url 2.0
```

will create an index on fields named title, body, and url, with scores of 10, 1, and 2 respectively.

When documents are indexed, the weights are taken from the saved *index Spec* that is stored in a special Redis key, and only fields that appear in this spec are indexed.

## Document data storage

It is not mandatory to save the document data when indexing a document. Specifying `NOSAVE` for `FT.ADD` will cause the document to be indexed but not saved.

If the user does save the document, a HASH key is created in Redis that contains all fields (including ones not indexed), and upon search, perform an `HGETALL` query on each retrieved document to retrieve all of its data.

**TODO**: Document snippets should be implemented down the road.

## Query execution engine

A chained-iterator based approach is used as part of query execution, which is similar to [Python generators](https://wiki.python.org/moin/Generators) in concept.

Iterators that yield index hits are chained together. Those can be:

1. **Read Iterators**, reading hits one by one from an inverted index. For example, `hello`.
2. **Intersect Iterators**, aggregating two or more iterators, yielding only their intersection points. For example, `hello AND world`.
3. **Exact Intersect Iterators** - same as above, but yielding results only if the intersection is an exact phrase. For example,  `hello NEAR world`.
4. **Union Iterators** - combining two or more iterators, and yielding a union of their hits. For example,  `hello OR world`.

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

All these iterators are lazily evaluated, entry by entry, with constant memory overhead.

The root iterator is read by the query execution engine and filtered for the top N results contained in it.

## Numeric filters

It's possible to define a field in the index schema as `NUMERIC`, meaning you will be able to limit search results only to those where the given value falls within a specific range. Filtering is done by adding `FILTER` predicates (more than one is supported) to your query. For example:

```
FT.SEARCH products "hd tv" FILTER price 100 (300
```

The filter syntax follows the ZRANGEBYSCORE semantics of Redis, meaning `-inf` and `+inf` are supported, and prepending `(` to a number means an exclusive range.

As of release 0.6, the implementation uses a multi-level range tree, saving ranges at multiple resolutions to allow efficient range scanning. Adding numeric filters can accelerate slow queries if the numeric range is small relative to the entire span of the filtered field. For example, a filter on dates focusing on a few days out of years of data can speed a heavy query by an order of magnitude.

## Auto-complete and fuzzy suggestions

Another important feature for searching and querying is auto-completion or suggestion. It allows you to create dictionaries of weighted terms, and then query them for completion suggestions to a given user prefix.  For example, if you put the term “lcd tv” into a dictionary, sending the prefix “lc” will return it as a result. The dictionary is modeled as a compressed trie (prefix tree) with weights, that is traversed to find the top suffixes of a prefix.

Redis Stack also allows for fuzzy suggestions, meaning you can get suggestions to user prefixes even if the user has a typo in the prefix. This is enabled using a Levenshtein automaton, allowing efficient searching of a dictionary for all terms within a maximal Levenshtein distance of a term or prefix. Suggestions are weighted based on both their original score and their distance from a prefix typed by the user. Only suggestions where the prefix is up to one Levenshtein distance away from the typed prefix are supported for performance reasons.

However, since searching for fuzzy prefixes, especially very short ones, will traverse an enormous amount of suggestions (in fact, fuzzy suggestions for any single letter will traverse the entire dictionary!), it is recommended that you use this feature carefully, and only when considering the performance penalty it incurs. Since Redis is single threaded, blocking it for any amount of time means no other queries can be processed at that time.

To support unicode fuzzy matching, 16-bit runes are used inside the trie and not bytes. This increases memory consumption if the text is purely ASCII, but allows completion with the same level of support to all modern languages. This is done in the following manner:

1. Assume all input to `FT.SUG*` commands is valid UTF-8.
2. Input strings are converted to 32-bit Unicode, optionally normalizing, case-folding, and removing accents on the way. If the conversion fails it's because the input is not valid UTF-8.
3. The 32-bit runes are trimmed to 16-bit runes using the lower 16 bits. These can be used for insertion, deletion, and search.
4. The output of searches is converted back to UTF-8.