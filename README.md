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
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Exact Phrase Search of up to 8 words.
* Stemming based query expansion in [many languages](#stemming-support) (using [Snowball](http://snowballstem.org/)).
* Limiting searches to specific document fields (up to 8 fields supported).
* Numeric filters and ranges.
* Supports any utf-8 encoded text.
* Retrieve full document content or just ids

### Not *yet* supported:

* Geo filters.
* NOT queries (foo -bar).
* Spelling correction
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
127.0.0.1:6379> FT.CREATE myIdx title TEXT 5.0 body TEXT 1.0 url 1.0
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

* Adding and getting Auto-complete suggestions:
```
127.0.0.1:6379> FT.SUGADD autocomplete "hello world" 100
OK

127.0.0.1:6379> FT.SUGGET autocomplete "he"
1) "hello world"

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

## FT.ADD index docId score [LANGUAGE lang] [NOSAVE] FIELDS <field> <text> ....

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

    - LANGUAGE lang: If set, we use a stemmer for the supplied langauge during indexing. Defaults to English. 
      If an unsupported language is sent, the command returns an error. 
      The supported languages are:
  
      > "arabic",  "danish",    "dutch",   "english",   "finnish",    "french",
      > "german",  "hungarian", "italian", "norwegian", "portuguese", "romanian",
      > "russian", "spanish",   "swedish", "tamil",     "turkish"

    
### Returns
> OK on success, or an error if something went wrong.

----

## FT.SEARCH index query [NOCONTENT] [VERBATIM] [LANGUAGE lang] [LIMIT offset num] [INFIELDS num field ...] [FILTER numeric_field min max]
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
    
    - FILTER numeric_field min max: If set, and numeric_field is defined as a numeric field in 
    FT.CREATE, we will limit results to those having numeric values ranging between min and max.
    min and max follow ZRANGE syntax, and can be -inf, +inf and use `(` for exclusive ranges.
      
    - VERBATIM if set, we do not try to use stemming for query expansion but search the query terms verbatim.
    
    - LANGUAGE lang: If set, we use a stemmer for the supplied langauge during search for query expansion. 
      Defaults to English. If an unsupported language is sent, the command returns an error.
       
      The supported languages are:
  
      > "arabic",  "danish",    "dutch",   "english",   "finnish",    "french",
      > "german",  "hungarian", "italian", "norwegian", "portuguese", "romanian",
      > "russian", "spanish",   "swedish", "tamil",     "turkish"
    
    
### Returns:

> Array reply, where the first element is the total number of results, and then pairs of
> document id, and a nested array of field/value, unless NOCONTENT was given
   
----


## FT.DROP index
Deletes all the keys associated with the index. 

If no other data is on the redis instance, this is equivalent to FLUSHDB, apart from the fact
that the index specification is not deleted.

### Returns:

> Simple String reply - OK on success.

---

## FT.OPTIMIZE index
After the index is built (and doesn't need to be updated again withuot a complete rebuild)
we can optimize memory consumption by trimming all index buffers to their actual size.

  **Warning 1**: Do not run it if you intend to update your index afterward.
  
  **Warning 2**: This blocks redis for a long time. Do not run it on production instances

### Returns:

> Integer Reply - the number of index entries optimized.

---

## FT.SUGGADD key string score [INCR]

Add a suggestion string to an auto-complete suggestion dictionary. This is disconnected from the
index definitions, and leaves creating and updating suggestino dictionaries to the user.

### Parameters:

   - **key**: the suggestion dictionary key.

   - **string**: the suggestion string we index

   - **score**: a floating point number of the suggestion string's weight

   - **INCR**: if set, we increment the existing entry of the suggestion by the given score, instead of
    replacing the score. This is useful for updating the dictionary based on user queries in real
    time

### Returns:

> Integer reply: the current size of the suggestion dictionary.

---

## FT.SUGLEN key

Get the size of an autoc-complete suggestion dictionary

### Parameters:

   - **key**: the suggestion dictionary key.

### Returns:

> Integer reply: the current size of the suggestion dictionary.

---

## FT.SUGGET key prefix [FUZZY] [MAX num]

Get completion suggestions for a prefix

### Parameters:

   - **key**: the suggestion dictionary key.

   - **prefix**: the prefix to complete on

   - **FUZZY**: if set,we do a fuzzy prefix search, including prefixes at levenshtein distance of 1 from
    the prefix sent

   - **MAX num**: If set, we limit the results to a maximum of `num`. (**Note**: The default is 5, and the number
   cannot be greater than 10).

### Returns:

> Array reply: a list of the top suggestions matching the prefix



# TODO

See [TODO](TODO.md)

# Stemming Support

RediSearch supports stemming - that is adding the base form of a word to the index. This allows 
the query for "going" to also return results for "go" and "gone", for example. 

The current stemming support is based on the Snowball stemmer library, which supports most European
languages, as well as Arabic and other. We hope to include more languages soon (if you need a specicif
langauge support, please open an issue). 

For further details see the [Snoball Stemmer website](http://snowballstem.org/). 

### The following languages are supported arguments to search and indexing commands:

* arabic
* danish
* dutch
* english
* finnish
* french
* german
* hungarian
* italian
* norwegian
* portuguese
* romanian
* russian
* spanish
* swedish
* tamil
* turkish