# RediSearch 

### Full-Text search over redis by RedisLabs

# Overview

Redisearch impements a search engine on top of redis, but unlike other redis 
search libraries, it does not use internal data structures like sorted sets.

Inverted indexes are stored on top of Redis strings using binary encoding,
and not mapped to existing data structures (see [DESIGN.md](DESIGN.md)). 

This allows much faster performance, significantly less memory consumption, and
more advanced features like exact phrase matching, that are not possible with 
traditional redis search approaches. 

## Primary Features:

* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time).
* Field weights.
* Exact Phrase Search of up to 8 words.
* Limiting searches to specific document fields (up to 8 fields supported)
* Numeric filters and ranges.
* Supports any utf-8 encoded text.
* Retrieve full document content or just ids

### Not *yet* supported:

* Geo filters
* NOT queries (foo -bar)
* Spelling correction
* Auto-complete
* Query expansion and stemming
* Full boolean query syntax
* Deletion and Updating (without full index rebuild)

### License: AGPL

Which basically means you can freely use this for your own projects without "virality" to your code,
as long as you're not modifying the module itself.

```

Warning:

RediSearch is under development and missing a lot of features.

The API may change, the internal implementation is evolving, 
and the Redis modules API itself is still unstable.
``` 
 
## Internal Design

See [DESIGN.md](DESIGN.md) for technical details about the internal design of the module. 

## Building and running:

```sh
git clone https://github.com/RedisLabsModules/RediSearch.git
cd RediSearch/src
make all

# Assuming you have a redis build from the unstable branch:
/path/to/redis-server --loadmodule ./module.so
```

## Quick Guide:

* Creating an index with fields and weights:
```
127.0.0.1:6379> FT.CREATE myIdx title 5.0 body 1.0 url 1.0
OK 
``` 

* Adding documents to the index:
```
127.0.0.1:6379> FT.ADD myIdx doc1 1.0 fields title "hello world" body "lorem ipsum" url "http://redis.io"
OK
```

* Searching the index:
```
127.0.0.1:6379> FT.SEARCH myIdx "hello world" LIMIT 0 10
1) (integer) 1
2) "doc1"
3) 1) "title"
   2) "hello world"
   3) "body"
   4) "lorem ipsum"
   5) "url"
   6) "http://redis.io"
```

  > **NOTE**: Input is expected to be valid utf-8 or ascii. The engine cannot handle wide character unicode at the moment. 


* Dropping the index:
```
127.0.0.1:6379> FT.DROP myIdx
OK
```


---- 

# Command details

## FT.CREATE index field1 weight1 [field2 weight2 ...]

Creates an index with the given spec. The index name will be used in all the key names
so keep it short!

### Parameters:

    - index: the index name to create. If it exists the old spec will be overwritten
    
    - field / weight pairs: pairs of field name and relative weight in scoring. 
    The weight is a double, but does not need to be normalized.

### Returns:
> OK or an error

----

## FT.ADD index docId score [NOSAVE] FIELDS <field> <text> ....

Add a documet to the index.

### Parameters:

    - index: The Fulltext index name. The index must be first created with FT.CREATE

    - docId: The document's id that will be returned from searches. 
      Note that the same docId cannot be added twice to the same index

    - score: The document's rank based on the user's ranking. This must be between 0.0 and 1.0. 
      If you don't have a score just set it to 1

    - NOSAVE: If set to true, we will not save the actual document in the index and only index it.
    
    - FIELDS: Following the FIELDS specifier, we are looking for pairs of <field> <text> to be indexed.
    
      Each field will be scored based on the index spec given in FT.CREATE. 
      Passing fields that are not in the index spec will make them be stored as part of the document, or ignored if NOSAVE is set 
    
### Returns
> OK on success, or an error if something went wrong.

----

## FT.SEARCH index query [NOCONTENT] [LIMIT offset num] [INFIELDS <num> <field> ...]
Seach the index with a textual query, returning either documents or just ids.

### Parameters:
    - index: The Fulltext index name. The index must be first created with FT.CREATE

    - query: the text query to search. If it's more than a single word, put it in quotes.
    Basic syntax like quotes for exact matching is supported.

    - NOCONTENT: If it appears after the query, we only return the document ids and not 
    the content. This is useful if rediseach is only an index on an external document collection

    - LIMIT fist num: If the parameters appear after the query, we limit the results to 
    the offset and number of results given. The default is 0 10

    - INFIELDS num field1 field2 ...: If set, filter the results to ones appearing only in specific
    fields of the document, like title or url. num is the number of specified field arguments
    
### Returns:

> Array reply, where the first element is the total number of results, and then pairs of
> document id, and a nested array of field/value, unless NOCONTENT was given
   
----


## FT.DROP <index>
Deletes all the keys associated with the index. 

If no other data is on the redis instance, this is equivalent to FLUSHDB, apart from the fact
that the index specification is not deleted.

### Returns:

> Simple String reply - OK on success.

---

## FT.OPTIMIZE <index>
After the index is built (and doesn't need to be updated again withuot a complete rebuild)
we can optimize memory consumption by trimming all index buffers to their actual size.

  **Warning 1**: Do not run it if you intend to update your index afterward.
  
  **Warning 2**: This blocks redis for a long time. Do not run it on production instances

### Returns:

> Integer Reply - the number of index entries optimized.

# TODO
See [TODO](TODO.md)