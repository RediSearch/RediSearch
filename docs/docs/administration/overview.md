---
title: "Technical overview"
linkTitle: "Technical overview"
weight: 1
description: >
    Technical details of the internal design of RediSearch
aliases:
  - /docs/stack/search/overview
  - /docs/stack/search/design/overview/
---

# Technical overview of the search and query features of Redis Stack

## Abstract

RediSearch is a powerful text search and secondary indexing engine that is built on top of Redis as a Redis module. 

Unlike other Redis search libraries, it does not use the internal data structures of Redis such as sorted sets. Using its own highly optimized data structures and algorithms, it allows for advanced search features, high performance, and a low memory footprint. It can perform simple text searches, as well as complex structured queries, filtering by numeric properties and geographical distances.

RediSearch supports continuous indexing with no performance degradation, maintaining concurrent loads of querying and indexing. This makes it ideal for searching frequently updated databases without the need for batch indexing and service interrupts. 

The Enterprise version of RediSearch supports scaling the search engine across many servers, allowing it to easily grow to billions of documents on hundreds of servers. 

All of this is done while taking advantage of Redis's robust architecture and infrastructure. Using Redis's protocol, replication, persistence, and clustering, RediSearch delivers a powerful yet simple to manage and maintain search and indexing engine that can be used as a standalone database, or to augment existing Redis databases with advanced powerful indexing capabilities.

---

## Main features

* Full-Text indexing of multiple fields in a document, including:
    * Exact phrase matching.
    * Stemming in many languages.
    * Chinese tokenization support.
    * Prefix queries.
    * Optional, negative, and union queries.
* Distributed search on billions of documents.
* Numeric property indexing.
* Geographical indexing and radius filters.
* Incremental indexing without performance loss.
* A structured query language for advanced queries:
    * Unions and intersections
    * Optional and negative queries
    * Tag filtering
    * Prefix matching
* A powerful auto-complete engine with fuzzy matching.
* Multiple scoring models and sorting by values.
* Concurrent, low-latency insertion and updates of documents.
* Concurrent searches allowing long-running queries without blocking Redis.
* An extension mechanism allowing custom scoring models and query extension.
* Support for indexing existing Hash objects in Redis databases.

---

# Indexing documents

Redis Stack needs to know how to index documents in order to search effectively. A document may have several fields, each with its own weight. For example, a title is usually more important than the text itself. The engine can also use numeric or geographical fields for filtering. Hence, the first step is to create the index definition, which tells Redis Stack how to treat the documents that will be added. For example, to define an index of products, indexing their title, description, brand, and price fields, the index creation would look like:

```
FT.CREATE my_index SCHEMA 
    title TEXT WEIGHT 5
    description TEXT 
    brand TEXT 
    PRICE numeric
```

When a document is added to this index, as in the following example, each field of the document is broken into its terms (tokenization), and indexed by marking the index for each of the terms in the index. As a result, the product is added immediately to the index and can now be found in future searches.

```
FT.ADD my_index doc1 1.0 FIELDS
    title "Acme 42 inch LCD TV"
    description "42 inch brand new Full-HD tv with smart tv capabilities"
    brand "Acme"
    price 300
```

This tells Redis Stack to take the document, break each field into its terms (tokenization), and index it by marking the index for each of the terms in the index as contained in this document. Thus, the product is added immediately to the index and can now be found in future searches.


## Searching

Now that the products have been added to our index, searching is very simple:

```
FT.SEARCH products "full hd tv"
```

This will tell Redis Stack to intersect the lists of documents for each term and return all documents containing the three terms. Of course, more complex queries can be performed, and the full syntax of the query language is detailed below. 

## Data structures

Redis Stack uses its own custom data structures and uses Redis' native structures only for storing the actual document content (using Hash objects).

Using specialized data structures allows faster searches and more memory effective storage of index records, utilizing compression techniques like delta encoding. 

These are the data structures Redis Stack uses under the hood:

### Index and document metadata

For each search _index_, there is a root data structure containing the schema, statistics, etc., but most importantly, compact metadata about each document indexed. 

Internally, inside the index, Redis Stack uses delta encoded lists of numeric, incremental, 32-bit document ids. This means that the user given keys or ids for documents, need to be replaced with the internal ids on indexing, and back to the original ids on search. 

For that, Redis Stack saves two tables, mapping the two kinds of ids in two ways (one table uses a compact trie, the other is simply an array where the internal document ID is the array index). On top of that, for each document, its user given presumptive score is stored, along with some status bits and any optional payload attached to the document by the user. 

Accessing the document metadata table is an order of magnitude faster than accessing the hash object where the document is actually saved, so scoring functions that need to access metadata about the document can operate fast enough.

### Inverted index

For each term appearing in at least one document, an inverted index is kept, which is basically a list of all the documents where this term appears. The list is compressed using delta coding, and the document ids are always incrementing. 

For example, when a user indexes the documents "foo", "bar", and "baz", they are assigned incrementing ids, e.g., `1025, 1045, 1080`. When encoding them into the index,  only the first ID is encoded, followed by the deltas between each entry and the previous one, e.g., `1025, 20, 35`. 

Using variable-width encoding, one byte is used to express numbers under 255, two bytes for numbers between 256 and 16,383, and so on. This can compress the index by up to 75%. 

On top of the IDs, the frequency of each term in each document, a bit mask representing the fields in which the term appeared in the document, and a list of the positions in which the term appeared is saved.

The structure of the default search record is as follows. Usually, all the entries are one byte long:

```
+----------------+------------+------------------+-------------+------------------------+
|  docId_delta   |  frequency | field mask       | offsets len | offset, offset, ....   |
|  (1-4 bytes)   | (1-2 bytes)| (1-16 bytes)     |  (1-2 bytes)| (1-2 bytes per offset) |
+----------------+------------+------------------+-------------+------------------------+
```

Optionally, you can choose not to save any one of those attributes besides the ID, degrading the features available to the engine. 

### Numeric index

Numeric properties are indexed in a special data structure that enables filtering by numeric ranges in an efficient way. One could view a numeric value as a term operating just like an inverted index. For example, all the products with the price $100 are in a specific list, which is intersected with the rest of the query. See [query execution engine](/docs/interact/search-and-query/administration/design/#query-execution-engine) for more information. 

However, in order to filter by a range of prices, you would have to intersect the query with all the distinct prices within that range, or perform a union query. If the range has many values in it, this becomes highly inefficient. 

To avoid this, numeric entries are grouped, with close values together, in a single range node. These nodes are stored in a binary range tree, which allows the engine to select the relevant nodes and union them together. Each entry in a range node contains a document Id and the actual numeric value for that document. To further optimize, the tree uses an adaptive algorithm to try to merge as many nodes as possible within the same range node. 

### Tag index

Tag indexes are similar to full-text indexes, but use simpler tokenization and encoding in the index. The values in these fields cannot be accessed by general field-less search and can be used only with a special syntax.

The main differences between tag fields and full-text fields are:

1. The tokenization is simpler. The user can determine a separator (defaults to a comma) for multiple tags. Whitespace trimming is done only at the end of tags. Thus, tags can contain spaces, punctuation marks, accents, etc. The only two transformations that are performed are lower-casing (for latin languages only as of now) and whitespace trimming.

2. Tags cannot be found from a general full-text search. If a document has a field called *tags* with the values *foo* and *bar*, searching for foo or bar without a special tag modifier (see below) will not return this document.

3. The index is much simpler and more compressed. Only the document IDs are stored in the index, usually resulting in 1-2 bytes per index entry.

### Geo index

Geo indexes utilize Redis's own geo-indexing capabilities. At query time, the geographical part of the query (a radius filter) is sent to Redis, returning only the ids of documents that are within that radius. Longitude and latitude should be passed as a string `lon,lat`. For example, `1.23,4.56`.

### Auto-complete

The auto-complete engine (see below for a fuller description) utilizes a compact trie or prefix tree to encode terms and search them by prefix.

## Query language

Simple syntax is supported for complex queries that can be combined together to express complex filtering and matching rules. The query is a text string in the `FT.SEARCH` request that is parsed using a complex query processor.

* Multi-word phrases are lists of tokens, e.g., `foo bar baz`, and imply intersection (logical AND) of the terms.
* Exact phrases are wrapped in quotes, e.g `"hello world"`.
* OR unions (e.g., `word1 OR word2`), are expressed with a pipe (`|`) character. For example, `hello|hallo|shalom|hola`.
* NOT negation (e.g., `word1 NOT word2`) of expressions or sub-queries use the dash (`-`) character. For example, `hello -world`. 
* Prefix matches (all terms starting with a prefix) are expressed with a `*` following a 2-letter or longer prefix.
* Selection of specific fields using the syntax `@field:hello world`.
* Numeric Range matches on numeric fields with the syntax `@field:[{min} {max}]`.
* Geo radius matches on geo fields with the syntax `@field:[{lon} {lat} {radius} {m|km|mi|ft}]`
* Tag field filters with the syntax `@field:{tag | tag | ...}`. See the [full documentation on tag fields](/docs/interact/search-and-query/query/#tag-filters).
* Optional terms or clauses: `foo ~bar` means bar is optional but documents with bar in them will rank higher. 

### Complex query examples

Expressions can be combined together to express complex rules. For example, given a database of products, where each entity has the fields `title`, `brand`, `tags` and `price`, expressing a generic search would be simply:

```
lcd tv
```

This would return documents containing these terms in any field. Limiting the search to specific fields (title only in this case) is expressed as:

```
@title:(lcd tv)
```

Numeric filters can be combined to filter by price within a given price range:

```
    @title:(lcd tv) 
    @price:[100 500.2]
```

Multiple text fields can be accessed in different query clauses. For example, to select products of multiple brands:

```
    @title:(lcd tv)
    @brand:(sony | samsung | lg)
    @price:[100 500.2]
```

Tag fields can be used to index multi-term properties without actual full-text tokenization:

```
    @title:(lcd tv) 
    @brand:(sony | samsung | lg) 
    @tags:{42 inch | smart tv} 
    @price:[100 500.2]
```

And negative clauses can also be added to filter out plasma and CRT TVs:

```
    @title:(lcd tv) 
    @brand:(sony | samsung | lg) 
    @tags:{42 inch | smart tv} 
    @price:[100 500.2]

    -@tags:{plasma | crt}
```

## Scoring model

Redis Stack comes with a few very basic scoring functions to evaluate document relevance. They are all based on document scores and term frequency. This is regardless of the ability to use sortable fields (see below). Scoring functions are specified by adding the `SCORER {scorer_name}` argument to a search request.

If you prefer a custom scoring function, it is possible to add more functions using the [extension API](/docs/interact/search-and-query/administration/extensions/).

These are the pre-bundled scoring functions available in Redis Stack:

* **TFIDF** (default)

    Basic [TF-IDF scoring](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) with document score and proximity boosting factored in.

* **TFIDF.DOCNORM**
* 
    Identical to the default TFIDF scorer, with one important distinction:

* **BM25**

    A variation on the basic TF-IDF scorer. See [this Wikipedia article for more information](https://en.wikipedia.org/wiki/Okapi_BM25).

* **DISMAX**

    A simple scorer that sums up the frequencies of the matched terms. In the case of union clauses, it will give the maximum value of those matches.

* **DOCSCORE**

    A scoring function that just returns the presumptive score of the document without applying any calculations to it. Since document scores can be updated, this can be useful if you'd like to use an external score and nothing further.


## Sortable fields

It is possible to bypass the scoring function mechanism and order search results by the value of different document properties (fields) directly, even if the sorting field is not used by the query. For example, you can search for first name and sort by the last name. 

When creating the index with `FT.CREATE`, you can declare `TEXT`, `TAG`, `NUMERIC`, and `GEO` properties as `SORTABLE`. When a property is sortable, you can later decide to order the results by its values with relatively low latency. When a property is not sortable, it can still be sorted by its values, but may increase latency. For example, the following schema:

```
FT.CREATE users SCHEMA first_name TEXT last_name TEXT SORTABLE age NUMERIC SORTABLE
```

Would allow the following query:

```
FT.SEARCH users "john lennon" SORTBY age DESC
```

## Result highlighting and summarization

Redis Stack uses advanced algorithms for highlighting and summarizing, which enable only the relevant portions of a document to appear in response to a search query. This feature allows users to immediately understand the relevance of a document to their search criteria, typically highlighting the matching terms in bold text. The syntax is as follows:

```
FT.SEARCH ...
    SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}] [SEPARATOR {separator}]
    HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]

```

Summarization will fragment the text into smaller sized snippets. Each snippet will contain the found term(s) and some additional surrounding context.

Highlighting will highlight the found term and its variants with a user-defined tag. This may be used to display the matched text in a different typeface using a markup language, or to otherwise make the text appear differently.

## Auto-completion

Another important feature for Redis Stack is its auto-complete engine. This allows users to create dictionaries of weighted terms, and then query them for completion suggestions to a given user prefix. Completions can have payloads, which are user-provided pieces of data that can be used for display. For example, completing the names of users, it is possible to add extra metadata about users to be displayed.

For example, if a user starts to put the term “lcd tv” into a dictionary, sending the prefix “lc” will return the full term as a result. The dictionary is modeled as a compact trie (prefix tree) with weights, which is traversed to find the top suffixes of a prefix.

Redis Stack also allows fuzzy suggestions, meaning you can get suggestions to prefixes even if the user makes a typo in their prefix. This is enabled using a Levenshtein automaton, allowing efficient searching of the dictionary for all terms within a maximal Levenshtein distance of a term or prefix. Suggestions are then weighted based on both their original score and their distance from the prefix typed by the user. 

However, searching for fuzzy prefixes (especially very short ones) will traverse an enormous number of suggestions. In fact, fuzzy suggestions for any single letter will traverse the entire dictionary, so the recommendation is to use this feature carefully and in full consideration of the performance penalty it incurs. 

Redis Stack's auto-completer supports Unicode, allowing for fuzzy matches in non-latin languages as well.

## Search engine internals

### The Redis module API

RediSearch is implemented using the [Redis module API](https://redis.io/topics/modules-intro) and is loaded into Redis as an extension module at start-up.

Redis modules make it possible to extend Redis's core functionality, implementing new Redis commands, data structures, and capabilities with similar performance to native core Redis itself. Redis modules are dynamic libraries that can be loaded into Redis at start-up or loaded at run-time using the `MODULE LOAD` command. Redis exports a C API, in the form of a single C header file called `redismodule.h`. 

While the logic of RediSearch and its algorithms are mostly independent, and it could be ported quite easily to run as a stand-alone server, it still takes advantage of Redis as a robust infrastructure for a database server. Building on top of Redis means that, by default, modules are afforded:

* A high performance network protocol server
* Robust replication
* Highly durable persistence as snapshots of transaction logs
* Cluster mode

### Query execution engine

Redis Stack uses a high-performance flexible query processing engine that can evaluate very complex queries in real time. 

The above query language is compiled into an execution plan that consists of a tree of index iterators or filters. These can be any of:

* Numeric filter
* Tag filter
* Text filter
* Geo filter
* Intersection operation (combining 2 or more filters)
* Union operation (combining 2 or more filters)
* NOT operation (negating the results of an underlying filter)
* Optional operation (wrapping an underlying filter in an optional matching filter)

The query parser generates a tree of these filters. For example, a multi-word search would be resolved into an intersect operation of multiple text filters, each traversing an inverted index of a different term. Simple optimizations such as removing redundant layers in the tree are applied.

Each of the filters in the resulting tree evaluates one match at a time. This means that at any given moment, the query processor is busy evaluating and scoring one matching document. This means that very little memory allocation is done at run-time, resulting in higher performance. 

The resulting matching documents are then fed to a post-processing chain of result processors that are responsible for scoring them, extracting the top-N results, loading the documents from storage, and sending them to the client. That chain is dynamic as well, which adapts based on the attributes of the query. For example, a query that only needs to return document ids will not include a stage for loading documents from storage. 

### Concurrent updates and searches

While RediSearch is extremely fast and uses highly optimized data structures and algorithms, it was facing the same problem with regards to concurrency. Depending on the size of your data set and the cardinality of search queries, queries can take anywhere between a few microseconds to hundreds of milliseconds, or even seconds in extreme cases. When that happens, the entire Redis server process is blocked. 

Think, for example, of a full-text query intersecting the terms "hello" and "world", each with a million entries, and a half-million common intersection points. To perform that query in a millisecond, Redis would have to scan, intersect, and rank each result in one nanosecond, [which is impossible with current hardware](https://gist.github.com/jboner/2841832). The same goes for indexing a 1,000 word document. It blocks Redis entirely for the duration of the query.

RediSearch uses the Redis Module API's concurrency features to avoid stalling the server for long periods of time. The idea is simple - while Redis itself is single-threaded, a module can run many threads, and any one of those threads can acquire the **Global Lock** when it needs to access Redis data, operate on it, and release it. 

Redis cannot be queried in parallel, as only one thread can acquire the lock, including the Redis main thread, but care is taken to make sure that a long-running query will give other queries time to run by yielding this lock from time to time.

The following design principles were adopted to allow concurrency:

1. RediSearch has a thread pool for running concurrent search queries. 

2. When a search request arrives, it is passed to the handler, parsed on the main thread, and then a request object is passed to the thread pool via a queue.

3. The thread pool runs a query processing function in its own thread.

4. The function locks the Redis Global lock and starts executing the query.

5. Since the search execution is basically an iterator running in a cycle, the elapsed time is sampled every several iterations (sampling on each iteration would slow things down as it has a cost of its own).

6. If enough time has elapsed, the query processor releases the Global Lock and immediately tries to acquire it again. When the lock is released, the kernel will schedule another thread to run - be it Redis's main thread, or another query thread.

7. When the lock is acquired again, all Redis resources that were held before releasing the lock are re-opened (keys might have been deleted while the thread has been sleeping) and work resumes from the previous state. 

Thus the operating system's scheduler makes sure all query threads get CPU time to run. While one is running the rest wait idly, but since execution is yielded about 5,000 times a second, it creates the effect of concurrency. Fast queries will finish in one go without yielding execution, slow ones will take many iterations to finish, but will allow other queries to run concurrently. 

### Index garbage collection

RediSearch is optimized for high write, update, and delete throughput. One of the main design choices dictated by this goal is that deleting and updating documents do not actually delete anything from the index: 

1. Deletion simply marks the document deleted in a global document metadata table using a single bit. 
2. Updating, on the other hand, marks a document as deleted, assigns it a new incremental document ID, and re-indexes the document under a new ID, without performing a comparison of the change. 

What this means, is that index entries belonging to deleted documents are not removed from the index, and can be seen as garbage. Over time, an index with many deletes and updates will contain mostly garbage, both slowing things down and consuming unnecessary memory. 

To overcome this, RediSearch employs a background garbage collection (GC) mechanism. During normal operation of the index, a special thread randomly samples indexes, traverses them, and looks for garbage. Index sections containing garbage are cleaned and memory is reclaimed. This is done in a non- intrusive way, operating on very small amounts of data per scan, and utilizing Redis's concurrency mechanism (see above) to avoid interrupting searches and indexing. The algorithm also tries to adapt to the state of the index, increasing the GC's frequency if the index contains a lot of garbage, and decreasing it if it doesn't, to the point of hardly scanning if the index does not contain garbage. 

### Extension model

RedisSearch supports an extension mechanism, much like Redis supports modules. The API is very minimal at the moment, and it does not yet support dynamic loading of extensions at run-time. Instead, extensions must be written in C (or a language that has an interface with C) and compiled into dynamic libraries that will be loaded at start-up.

There are two kinds of extension APIs at the moment: 

1. **Query expanders**, whose role is to expand query tokens (i.e., stemmers).
2. **Scoring functions**, whose role is to rank search results at query time.

Extensions are compiled into dynamic libraries and loaded into RediSearch on initialization of the module. The mechanism is based on the code of Redis's own module system, albeit far simpler.

---

# Scalable distributed search

While RediSearch is very fast and memory efficient, if an index is big enough, at some point it will be too slow or consume too much memory. It must then be scaled out and partitioned over several machines, each of which will hold a small part of the complete search index.

Traditional clusters map different keys to different shards to achieve this. However, with search indexes this approach is not practical. If each word’s index was mapped to a different shard, it would be necessary to intersect records from different servers for multi-term queries. 

The way to address this challenge is to employ a technique called index partitioning, which is very simple at its core:

* The index is split across many machines/partitions by document ID.
* Every partition has a complete index of all the documents mapped to it.
* All shards are queried concurrently and the results from each shard are merged into a single result.

To facilitate this, a new component called a coordinator is added to the cluster. When searching for documents, the coordinator receives the query and sends it to N partitions, each holding a sub-index of 1/N documents. Since we’re only interested in the top K results of all partitions, each partition returns just its own top K results. Then, the N lists of K elements are merged and the top K elements are extracted from the merged list.
