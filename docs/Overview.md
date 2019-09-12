# RediSearch Technical Overview 

## Abstract

RediSearch is a powerful text search and secondary indexing engine, built on top of Redis as a Redis Module. 

Unlike other Redis search libraries, it does not use the internal data structures of Redis like sorted sets. Using its own highly optimized data structures and algorithms, it allows for advanced search features, high performance, and low memory footprint. It can perform simple text searches, as well as complex structured queries, filtering by numeric properties and geographical distances.

RediSearch supports continuous indexing with no performance degradation, maintaining concurrent loads of querying and indexing. This makes it ideal for searching frequently updated databases, without the need for batch indexing and service interrupts. 

RediSearch's Enterprise version supports scaling the search engine across many servers, allowing it to easily grow to billions of documents on hundreds of servers. 

All of this is done while taking advantage of Redis' robust architecture and infrastructure. Utilizing Redis' protocol, replication, persistence, clustering - RediSearch delivers a powerful yet simple to manage and maintain search and indexing engine, that can be used as a standalone database, or to augment existing Redis databases with advanced powerful indexing capabilities.

---

## Main features

* Full-Text indexing of multiple fields in a document, including:
    * Exact phrase matching.
    * Stemming in many languages.
    * Chinese tokenization support.
    * Prefix queries.
    * Optional, negative and union queries.
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
* Concurrent low-latency insertion and updates of documents.
* Concurrent searches allowing long-running queries without blocking Redis.
* An extension mechanism allowing custom scoring models and query extension.
* Support for indexing existing Hash objects in Redis databases.

---

# Indexing documents

In order to search effectively, RediSearch needs to know how to index documents. A document may have several fields, each with its own weight (e.g. a title is usually more important than the text itself. The engine can also use numeric or geographical fields for filtering. Hence, the first step is to create the index definition, which tells RediSearch how to treat the documents we will add. For example, to define an index of products, indexing their title, description, brand, and price, the index creation would look like:

```
FT.CREATE my_index SCHEMA 
    title TEXT WEIGHT 5
    description TEXT 
    brand TEXT 
    PRICE numeric
```

When we will add a document to this index, for example:

```
FT.ADD my_index doc1 1.0 FIELDS
    title "Acme 42 inch LCD TV"
    description "42 inch brand new Full-HD tv with smart tv capabilities"
    brand "Acme"
    price 300
```

This tells RediSearch to take the document, break each field into its terms ("tokenization") and index it, by marking the index for each of the terms in the index as contained in this document. Thus, the product is added immediately to the index and can now be found in future searches


## Searching

Now that we have added products to our index, searching is very simple:

```
FT.SEARCH products "full hd tv"
```

This will tell RediSearch to intersect the lists of documents for each term and return all documents containing the three terms. Of course, more complex queries can be performed, and the full syntax of the query language is detailed below. 

## Data structures

RediSearch uses its own custom data structures and uses Redis' native structures only for storing the actual document content (using  Hash objects).

Using specialized data structures allows faster searches and more memory effective storage of index records, utilizing compression techniques like delta encoding. 

These are the data structures RediSearch uses under the hood:
### Index and document metadata

For each search _index_, there is a root data structure containing the schema, statistics, etc - but most importantly, little compact metadata about each document indexed. 

Internally, inside the index, RediSearch uses delta encoded lists of numeric, incremental, 32-bit document ids. This means that the user given keys or ids for documents, need to be replaced with the internal ids on indexing, and back to the original ids on search. 

For that, RediSearch saves two tables, mapping the two kinds of ids in two ways (one table uses a compact trie, the other is simply an array where the internal document ID is the array index). On top of that, for each document, we store its user given a priory score, some status bits, and an optional "payload" attached to the document by the user. 

Accessing the document metadata table is an order of magnitude faster than accessing the hash object where the document is actually saved, so scoring functions that need to access metadata about the document can operate fast enough.

### Inverted index

For each term appearing in at least one document, we keep an inverted index, basically a list of all the documents where this term appears. The list is compressed using delta coding, and the document ids are always incrementing. 

When the user indexes the documents "foo", "bar" and "baz" for example, they are assigned incrementing ids, For example `1025, 1045, 1080`. When encoding them into the index, we only encode the first ID, followed by the deltas between each entry and the previous one, in this case, `1025, 20, 35`. 

Using variable-width encoding, we can use one byte to express numbers under 255, two bytes for numbers between 256 and 16383 and so on. This can compress the index by up to 75%. 

On top of the ids, we save the frequency of each term in each document, a bit mask representing the fields in which the term appeared in the document, and a list of the positions in which the term appeared.

The structure of the default search record is as follows. Usually, all the entries are one byte long:

```
+----------------+------------+------------------+-------------+------------------------+
|  docId_delta   |  frequency | field mask       | offsets len | offset, offset, ....   |
|  (1-4 bytes)   | (1-2 bytes)| (1-16 bytes)     |  (1-2 bytes)| (1-2 bytes per offset) |
+----------------+------------+------------------+-------------+------------------------+
```

Optionally, we can choose not to save any one of those attributes besides the ID, degrading the features available to the engine. 

### Numeric index

Numeric properties are indexed in a special data structure that enables filtering by numeric ranges in an efficient way. One could view a numeric value as a term operating just like an inverted index. For example, all the products with the price $100 are in a specific list, that is intersected with the rest of the query (see Query Execution Engine). 

However, in order to filer by a range of prices, we would have to intersect the query with all the distinct prices within that range - or perform a union query. If the range has many values in it, this becomes highly inefficient. 

To avoid that, we group numeric entries with close values together, in a single "range node". These nodes are stored in binary range tree, that allows the engine to select the relevant nodes and union them together. Each entry in a range node contains a document Id, and the actual numeric value for that document. To further optimize, the tree uses an adaptive algorithm to try to merge as many nodes as possible within the same range node. 

### Tag index

Tag indexes are similar to full-text indexes, but use simpler tokenization and encoding in the index. The values in these fields cannot be accessed by general field-less search and can be used only with a special syntax.

The main differences between tag fields and full-text fields are:

1. An entire tag field index resides in a single Redis key and doesn't have a key per term as the full-text one.

2. The tokenization is simpler: The user can determine a separator (defaults to a comma) for multiple tags, and we only do whitespace trimming at the end of tags. Thus, tags can contain spaces, punctuation marks, accents, etc. The only two transformations we perform are lower-casing (for latin languages only as of now), and whitespace trimming.

3. Tags cannot be found from a general full-text search. If a document has a field called "tags" with the values "foo" and "bar", searching for foo or bar without a special tag modifier (see below) will not return this document.

4. The index is much simpler and more compressed: we only store document ids in the index, usually resulting in 1-2 bytes per index entry.

### Geo index

Geo indexes utilize Redis' own geo-indexing capabilities. In query time, the geographical part of the query (a radius filter) is sent to Redis, returning only the ids of documents that are within that radius. 

### Auto-complete

The auto-complete engine (see below for a fuller description) utilizes a compact trie or prefix tree, to encode terms and search them by prefix.

## Query language

We support a simple syntax for complex queries, that can be combined together to express complex filtering and matching rules. The query is combined as a text string in the `FT.SEARCH` request and is parsed using a complex query parser.

* Multi-word phrases simply a list of tokens, e.g. `foo bar baz`, and imply intersection (AND) of the terms.
* Exact phrases are wrapped in quotes, e.g `"hello world"`.
* OR Unions (i.e `word1 OR word2`), are expressed with a pipe (`|`), e.g. `hello|hallo|shalom|hola`.
* NOT negation (i.e. `word1 NOT word2`) of expressions or sub-queries. e.g. `hello -world`. 
* Prefix matches (all terms starting with a prefix) are expressed with a `*` following a 2-letter or longer prefix.
* Selection of specific fields using the syntax `@field:hello world`.
* Numeric Range matches on numeric fields with the syntax `@field:[{min} {max}]`.
* Geo radius matches on geo fields with the syntax `@field:[{lon} {lat} {radius} {m|km|mi|ft}]`
* Tag field filters with the syntax `@field:{tag | tag | ...}`. See the full documentation on tag fields.
* Optional terms or clauses: `foo ~bar` means bar is optional but documents with bar in them will rank higher. 

### Complex queries example

Expressions can be combined together to express complex rules. For example, let's assume we have a database of products, where each entity has the fields `title`, `brand`, `tags` and `price`. 

Expressing a generic search would be simply:
```
lcd tv
```

This would return documents containing these terms in any field. Limiting the search to specific fields (title only in this case) is expressed as:

```
@title:(lcd tv)
```

Numeric filters can be combined to filter price within a price range:

```
    @title:(lcd tv) 
    @price:[100 500.2]
```

Multiple text fields can be accessed in different query clauses, for example, to select products of multiple brands:

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

And negative clauses can also be added, in this example to filter out plasma and CRT TVs:

```
    @title:(lcd tv) 
    @brand:(sony | samsung | lg) 
    @tags:{42 inch | smart tv} 
    @price:[100 500.2]

    -@tags:{plasma | crt}
```

## Scoring model

RediSearch comes with a few very basic scoring functions to evaluate document relevance. They are all based on document scores and term frequency. This is regardless of the ability to use sortable fields (see below). Scoring functions are specified by adding the `SCORER {scorer_name}` argument to a search request.

If you prefer a custom scoring function, it is possible to add more functions using the [Extension API](Extensions.md).

These are the pre-bundled scoring functions available in RediSearch:

* **TFIDF (Default)**

    Basic [TF-IDF scoring](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) with document score and proximity boosting factored in.

* **TFIDF.DOCNORM**
    Identical to the default TFIDF scorer, with one important distinction:

* **BM25**

    A variation on the basic TF-IDF scorer, see [this Wikipedia article for more info](https://en.wikipedia.org/wiki/Okapi_BM25).

* **DISMAX**

    A simple scorer that sums up the frequencies of the matched terms; in the case of union clauses, it will give the maximum value of those matches.

* **DOCSCORE**

    A scoring function that just returns the priory score of the document without applying any calculations to it. Since document scores can be updated, this can be useful if you'd like to use an external score and nothing further.


## Sortable fields

It is possible to bypass the scoring function mechanism, and order search results by the value of different document properties (fields) directly - even if the sorting field is not used by the query. For example, you can search for first name and sort by the last name. 

When creating the index with FT.CREATE, you can declare `TEXT` and `NUMERIC` properties to be `SORTABLE`. When a property is sortable, we can later decide to order the results by its values. For example, in the following schema:

```
> FT.CREATE users SCHEMA first_name TEXT last_name TEXT SORTABLE age NUMERIC SORTABLE
```

Would allow the following query:

```
FT.SEARCH users "john lennon" SORTBY age DESC
```


## Result highlighting and summarisation

Highlighting allows users to only the relevant portions of document matching a search query returned as a result. This allows users to quickly see how a document relates to their query, with the search terms highlighted, usually in bold letters.

RediSearch implements high performance highlighting and summarization algorithms, with the following API: 

```
FT.SEARCH ...
    SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}] [SEPARATOR {separator}]
    HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]

```

Summarisation will fragment the text into smaller sized snippets; each snippet will contain the found term(s) and some additional surrounding context.

Highlighting will highlight the found term (and its variants) with a user-defined tag. This may be used to display the matched text in a different typeface using a markup language, or to otherwise make the text appear differently.

## Auto-completion

Another important feature for RediSearch is its auto-complete engine. This allows users to create dictionaries of weighted terms, and then query them for completion suggestions to a given user prefix. Completions can have "payloads" - a user-provided piece of data that can be used for display. For example, completing the names of users, it is possible to add extra metadata about users to be displayed al

For example, if a user starts to put the term “lcd tv” into a dictionary, sending the prefix “lc” will return the full term as a result. The dictionary is modeled as a compact trie (prefix tree) with weights, which is traversed to find the top suffixes of a prefix.

RediSearch also allows for Fuzzy Suggestions, meaning you can get suggestions to prefixes even if the user makes a typo in their prefix. This is enabled using a Levenshtein Automaton, allowing efficient searching of the dictionary for all terms within a maximal Levenshtein Distance of a term or prefix. Then suggestions are weighted based on both their original score and their distance from the prefix typed by the user. 

However, searching for fuzzy prefixes (especially very short ones) will traverse an enormous number of suggestions. In fact, fuzzy suggestions for any single letter will traverse the entire dictionary, so we recommend using this feature carefully, in consideration of the performance penalty it incurs. 

RediSearch's auto-completer supports Unicode, allowing for fuzzy matches in non-latin languages as well.

## Search engine internals

### The Redis module API

RediSearch utilizes the [Redis Module API](https://redis.io/topics/modules-intro) and is loaded into Redis as an extension module. 

Redis modules make possible to extend Redis functionality, implementing new Redis commands, data structures and capabilities with similar performance to native core Redis itself. Redis modules are dynamic libraries, that can be loaded into Redis at startup or using the MODULE LOAD command. Redis exports a C API, in the form of a single C header file called redismodule.h. 

This means that while the logic of RediSearch and its algorithms are mostly independent, and it could, in theory, be ported quite easily to run as a stand-alone server - it still "stands on the shoulders" of giants and takes advantage of Redis as a robust infrastructure for a database server. Building on top of Redis means that by default the module operates:
  * A high performance network protocol server.
  * Robust replication. 
  * Highly durable persistence as snapshots of transaction logs.
  * Cluster mode.
  * etc.

### Query execution engine

RediSearch uses a high-performance flexible query processing engine, that can evaluate very complex queries in real time. 

The above query language is compiled into an execution plan that consists of a tree of "index iterators" or "filters". These can be any of:

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

The resulting matching documents are then fed to a post-processing chain of "result processors", responsible for scoring them, extracting the top-N results, loading the documents from storage and sending them to the client. That chain is dynamic as well, and changes based on the attributes of the query. For example, a query that only needs to return document ids, will not include a stage for loading documents from storage. 

### Concurrent updates and searches

While it is extremely fast and uses highly optimized data structures and algorithms, it was facing the same problem with regards to concurrency: Depending on the size of your data-set and the cardinality of search queries, they can take internally anywhere between a few microseconds, to hundreds of milliseconds to seconds in extreme cases. And when that happens - the entire Redis server that the engine is running on - is blocked. 

Think, for example, of a full-text query intersecting the terms "hello" and "world", each with, let's say, a million entries, and half a million common intersection points. To do that in a millisecond, you would have to scan, intersect and rank each result in one nanosecond, [which is impossible with current hardware](https://gist.github.com/jboner/2841832). The same goes for indexing a 1000 word document. It blocks Redis entirely for that duration.

RediSearch utilizes the Redis Module API's concurrency features to avoid stalling the server for long periods of time. The idea is simple - while Redis in itself still remains single-threaded, a module can run many threads - and any one of them can acquire the **Global Lock** when it needs to access Redis data, operate on it, and release it. 

We still cannot really query Redis in parallel - only one thread can acquire the lock, including the Redis main thread - but we can make sure that a long-running query will give other queries time to properly run by yielding this lock from time to time.

To allow concurrency, we adopted the following design:

1. RediSearch has a thread pool for running concurrent search queries. 

2. When a search request arrives, it gets to the handler, gets parsed on the main thread, and a request object is passed to the thread pool via a queue.

3. The thread pool runs a query processing function in its own thread.

4. The function locks the Redis Global lock and starts executing the query.

5. Since the search execution is basically an iterator running in a cycle, we simply sample the elapsed time every several iterations (sampling on each iteration would slow things down as it has a cost of its own).

6. If enough time has elapsed, the query processor releases the Global Lock, and immediately tries to acquire it again. When the lock is released, the kernel will schedule another thread to run - be it Redis' main thread, or another query thread.

7. When the lock is acquired again - we reopen all Redis resources we were holding before releasing the lock (keys might have been deleted while the thread has been "sleeping") and continue work from the previous state. 

Thus the operating system's scheduler makes sure all query threads get CPU time to run. While one is running the rest wait idly, but since execution is yielded about 5,000 times a second, it creates the effect of concurrency. Fast queries will finish in one go without yielding execution, slow ones will take many iterations to finish, but will allow other queries to run concurrently. 

### Index garbage collection
RediSearch is optimized for high write, update and delete throughput. One of the main design choices dictated by this goal is that deleting and updating documents do not actually delete anything from the index: 

1. Deletion simply marks the document deleted in a global document metadata table, using a single bit. 
2. Updating, on the other hand, marks the document as deleted, assigns it a new incremental document ID, and re-indexes the document under a new ID, without performing a comparison of the change. 

What this means, is that index entries belonging to deleted documents are not removed from the index, and can be seen as "garbage". Over time, an index with many deletes and updates will contain mostly garbage - both slowing things down and consuming unnecessary memory. 

To overcome this, RediSearch employs a background Garbage Collection mechanism: during normal operation of the index, a special thread randomly samples indexes, traverses them and looks for garbage. Index sections containing garbage are "cleaned" and memory is reclaimed. This is done in a none intrusive way, operating on very small amounts of data per scan, and utilizing Redis' concurrency mechanism (see above) to avoid interrupting the searches and indexing. The algorithm also tries to adapt to the state of the index, increasing the garbage collector's frequency if the index contains a lot of garbage, and decreasing it if it doesn't, to the point of hardly scanning if the index does not contain garbage. 

### Extension model

RediSearch supports an extension mechanism, much like Redis supports modules. The API is very minimal at the moment, and it does not yet support dynamic loading of extensions in run-time. Instead, extensions must be written in C (or a language that has an interface with C) and compiled into dynamic libraries that will be loaded at run-time.

There are two kinds of extension APIs at the moment: 

1. **Query Expanders**, whose role is to expand query tokens (i.e. stemmers).
2. **Scoring Functions**, whose role is to rank search results in query time.

Extensions are compiled into dynamic libraries and loaded into RediSearch on initialization of the module. In fact, the mechanism is based on the code of Redis' own module system, albeit far simpler.

---

# Scalable Distributed Search

While RediSearch is very fast and memory efficient, if an index is big enough, at some point it will be too slow or consume too much memory. Then, it will have to be scaled out and partitioned over several machines - meaning every machine will hold a small part of the complete search index.

Traditional clusters map different keys to different “shards” to achieve this. However, in search indexes, this approach is not practical. If we mapped each word’s index to a different shard, we would end up needing to intersect records from different servers for multi-term queries. 

The way to address this challenge is to employ a technique called Index Partitioning, which is very simple at its core:

* The index is split across many machines/partitions by document ID.
* Every such partition has a complete index of all the documents mapped to it.
* We query all shards concurrently and merge the results from all of them into a single result.

To enable that, a new component is added to the cluster, called a Coordinator. When searching for documents, the coordinator receves the query, and sends it to N partitions, each holding a sub-index of 1/N documents. Since we’re only interested in the top K results of all partitions, each partition returns just its own top K results. We then merge the N lists of K elements and extract the top K elements from the merged list.

This feature is currently available only with the Enterprise version of RediSearch.
