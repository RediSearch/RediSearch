# RediSeach Full Command Documentation

## FT.CREATE 

### Format:
```
  FT.CREATE {index} 
    [NOOFFSETS] [NOFIELDS] [NOSCOREIDX]
    [STOPWORDS {num} {stopword} ...]
    SCHEMA {field} [TEXT [WEIGHT {weight}] | NUMERIC | GEO] [SORTABLE] ...
```

### Description:
Creates an index with the given spec. The index name will be used in all the key names
so keep it short!

### Parameters:

* **index**: the index name to create. If it exists the old spec will be overwritten

* **NOOFFSETS**: If set, we do not store term offsets for documents (saves memory, does not allow exact searches)

* **NOFIELDS**: If set, we do not store field bits for each term. Saves memory, does not allow filtering by specific fields.

* **NOSCOREIDX**: If set, we avoid saving the top results for single words. Saves a lot of memory, slows down searches for common single word queries.

* **STOPWORDS**: If set, we set the index with a custom stopword list, to be ignored during indexing and search time. {num} is the number of stopwords, followed by a list of stopword arguments exactly the length of {num}. 

    If not set, we take the default list of stopwords. 

    If **{num}** is set to 0, the index will not have stopwords.

* **SCHEMA {field} {options...}**: After the SCHEMA keyword we define the index fields. 
They can be numeric, textual or geographical. For textual fields we optionally specify a weight. The default weight is 1.0.

    Numeric or text field can have the optional SORTABLE argument that allows the user to later [sort the results by the value of this field](/Sorting) (this adds memory overhead so do not declare it on large text fields).

### Complexity
O(1)

### Returns:
OK or an error

---


## FT.ADD 

### Format:

```
FT.ADD {index} {docId} {score} 
  [NOSAVE]
  [REPLACE]
  [LANGUAGE {language}] 
  [PAYLOAD {payload}]
  FIELDS {field} {value} [{field} {value}...]
```

### Description

Add a documet to the index.

### Parameters:

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

- **docId**: The document's id that will be returned from searches. 
  Note that the same docId cannot be added twice to the same index

- **score**: The document's rank based on the user's ranking. This must be between 0.0 and 1.0. 
  If you don't have a score just set it to 1

- **NOSAVE**: If set to true, we will not save the actual document in the index and only index it.

- **REPLACE**: If set, we will do an UPSERT style insertion - and delete an older version of the document if it exists.

- **FIELDS**: Following the FIELDS specifier, we are looking for pairs of  `{field} {value}` to be indexed.

  Each field will be scored based on the index spec given in FT.CREATE. 
  Passing fields that are not in the index spec will make them be stored as part of the document, or ignored if NOSAVE is set 

- **PAYLOAD {payload}**: Optionally set a binary safe payload string to the document, 
  that can be evaluated at query time by a custom scoring function, or retrieved to the client.

- **LANGUAGE language**: If set, we use a stemmer for the supplied langauge during indexing. Defaults to English. 
  If an unsupported language is sent, the command returns an error. 
  The supported languages are:

  > "arabic",  "danish",    "dutch",   "english",   "finnish",    "french",
  > "german",  "hungarian", "italian", "norwegian", "portuguese", "romanian",
  > "russian", "spanish",   "swedish", "tamil",     "turkish"

### Complexity

O(n), where n is the number of tokens in the document

### Returns

OK on success, or an error if something went wrong.

----

## FT.ADDHASH

### Format

```
 FT.ADDHASH {index} {docId} {score} [LANGUAGE language] [REPLACE]
```

### Description

Add a documet to the index from an existing HASH key in Redis.

### Parameters:

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

-  **docId**: The document's id. This has to be an existing HASH key in redis that will hold the fields 
    the index needs.

- **score**: The document's rank based on the user's ranking. This must be between 0.0 and 1.0. 
  If you don't have a score just set it to 1

- **REPLACE**: If set, we will do an UPSERT style insertion - and delete an older version of the document if it exists.

- **LANGUAGE language**: If set, we use a stemmer for the supplied langauge during indexing. Defaults to English. 
  If an unsupported language is sent, the command returns an error. 
  The supported languages are:

  > "arabic",  "danish",    "dutch",   "english",   "finnish",    "french",
  > "german",  "hungarian", "italian", "norwegian", "portuguese", "romanian",
  > "russian", "spanish",   "swedish", "tamil",     "turkish"

### Complexity

O(n), where n is the number of tokens in the document

### Returns

OK on success, or an error if something went wrong.

----

## FT.INFO

### Format

```
FT.INFO {index} 
```

### Description

Return information and statistics on the index. Returned values include:

* Number of documents.
* Number of distinct terms.
* Average bytes per record.
* Size and capacity of the index buffers.

Example:

```
127.0.0.1:6379> ft.info wik{0}
 1) index_name
 2) wikipedia
 3) fields
 4) 1) 1) title
       2) type
       3) FULLTEXT
       4) weight
       5) "1"
    2) 1) body
       2) type
       3) FULLTEXT
       4) weight
       5) "1"
 5) num_docs
 6) "502694"
 7) num_terms
 8) "439158"
 9) num_records
10) "8098583"
11) inverted_sz_mb
12) "45.58
13) inverted_cap_mb
14) "56.61
15) inverted_cap_ovh
16) "0.19
17) offset_vectors_sz_mb
18) "9.27
19) skip_index_size_mb
20) "7.35
21) score_index_size_mb
22) "30.8
23) records_per_doc_avg
24) "16.1
25) bytes_per_record_avg
26) "5.90
27) offsets_per_term_avg
28) "1.20
29) offset_bits_per_record_avg
30) "8.00
```

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

### Complexity

O(1)

### Returns

Array Response. A nested array of keys and values.

---

## FT.SEARCH 

### Format

```
FT.SEARCH {index} {query} [NOCONTENT] [VERBATIM] [NOSTOPWORDS] [WITHSCORES] [WITHPAYLOADS] [WITHSORTKEYS]
  [FILTER {numeric_field} {min} {max}] ...
  [GEOFILTER {geo_field} {lon} {lat} {raius} m|km|mi|ft]
  [INKEYS {num} {key} ... ]
  [INFIELDS {num} {field} ... ]
  [RETURN {num} {field} ... ]
  [SLOP {slop}] [INORDER]
  [LANGUAGE {language}]
  [EXPANDER {expander}]
  [SCORER {scorer}]
  [PAYLOAD {payload}]
  [SORTBY {field} [ASC|DESC]]
  [LIMIT offset num]
```

### Description

Search the index with a textual query, returning either documents or just ids.

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **query**: the text query to search. If it's more than a single word, put it in quotes.
  See below for documentation on query syntax. 
- **NOCONTENT**: If it appears after the query, we only return the document ids and not 
  the content. This is useful if rediseach is only an index on an external document collection
- **RETURN {num} {field} ...**: Use this keyword to limit which fields from the document are returned.
  `num` is the number of fields following the keyword. If `num` is 0, it acts like `NOCONTENT`.
- **LIMIT first num**: If the parameters appear after the query, we limit the results to 
  the offset and number of results given. The default is 0 10
- **INFIELDS {num} {field} ...**: If set, filter the results to ones appearing only in specific
  fields of the document, like title or url. num is the number of specified field arguments
- **INKEYS {num} {field} ...**: If set, we limit the result to a given set of keys specified in the list. 
  the first argument must be the length of the list, and greater than zero.
  Non existent keys are ignored - unless all the keys are non existent.
- **SLOP {slop}**: If set, we allow a maximum of N intervening number of unmatched offsets between phrase terms. (i.e the slop for exact phrases is 0)
- **INORDER**: If set, and usually used in conjunction with SLOP, we make sure the query terms appear in the same order in the document as in the query, regardless of the offsets between them. 
- **FILTER numeric_field min max**: If set, and numeric_field is defined as a numeric field in 
  FT.CREATE, we will limit results to those having numeric values ranging between min and max.
  min and max follow ZRANGE syntax, and can be **-inf**, **+inf** and use `(` for exclusive ranges. 
  Multiple numeric filters for different fields are supported in one query.
- **GEOFILTER {geo_field} {lon} {lat} {raius} m|km|mi|ft**: If set, we filter the results to a given radius 
  from lon and lat. Radius is given as a number and units. See [GEORADIUS](https://redis.io/commands/georadius) for more details. 
- **NOSTOPWORDS**: If set, we do not filter stopwords from the query. 
- **WITHSCORES**: If set, we also return the relative internal score of each document. this can be
  used to merge results from multiple instances
- **WITHSORTKEYS**: Only relevant in conjunction with **SORTBY**. Returns the value of the sorting key, right after the id and score and /or payload if requested. This is usually not needed by users, and exists for distributed search coordination purposes.
- **VERBATIM**: if set, we do not try to use stemming for query expansion but search the query terms verbatim.
- **LANGUAGE {language}**: If set, we use a stemmer for the supplied langauge during search for query expansion. 
  Defaults to English. If an unsupported language is sent, the command returns an error. See FT.ADD for the list of languages.
- **EXPANDER {expander}**: If set, we will use a custom query expander instead of the stemmer. [See Extensions](/Extensions).
- **SCORER {scorer}**: If set, we will use a custom scoring function defined by the user. [See Extensions](/Extensions).
- **PAYLOAD {payload}**: Add an arbitrary, binary safe payload that will be exposed to custom scoring functions. [See Extensions](/Extensions).
- **WITHPAYLOADS**: If set, we retrieve optional document payloads (see FT.ADD). 
  the payloads follow the document id, and if `WITHSCORES` was set, follow the scores.
- **SORTBY {field} [ASC|DESC]**: If specified, and field is a [sortable field](/Sorting), the results are ordered by the value of this field. This applies to both text and numeric fields.

### Complexity

O(n) for single word queries (though for popular words we save a cache of the top 50 results).

Complexity for complex queries changes, but in general it's proportional to the number of words and the number of intersection points between them.

### Returns

**Array reply,** where the first element is the total number of results, and then pairs of document id, and a nested array of field/value. 

If **NOCONTENT** was given, we return an array where the first element is the total number of results, and the rest of the members are document ids.

---

## FT.EXPLAIN

### Format

```
FT.EXPLAIN {index} {query}
```

### Description

Return the execution plan for a complex query

Example:

```sh
$ redis-cli --raw

127.0.0.1:6379> FT.EXPLAIN rd "(foo bar)|(hello world) @date:[100 200]|@date:[500 +inf]"
INTERSECT {
  UNION {
    INTERSECT {
      foo
      bar
    }
    INTERSECT {
      hello
      world
    }
  }
  UNION {
    NUMERIC {100.000000 <= x <= 200.000000}
    NUMERIC {500.000000 <= x <= inf}
  }
}
```
### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **query**: The query string, as if sent to FT.SEARCH

### Complexity

O(1)

### Returns

String Response. A string representing the execution plan (see above example). 

**Note**: You should use `redis-cli --raw` to properly read line-breaks in the returned response.

---


## FT.DEL

### Format

```
FT.DEL {index} {doc_id}
```

### Description

Delete a document from the index. Returns 1 if the document was in the index, or 0 if not. 

After deletion, the document can be re-added to the index. It will get a different internal id and will be a new document from the index's POV.

**NOTE**: This does not actually delete the document from the index, just marks it as deleted. 
Thus, deleting and re-inserting the same document over and over will inflate the index size with each re-insertion.

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **doc_id**: the id of the document to be deleted. It does not actually delete the HASH key in which the document is stored. Use DEL to do that manually if needed.


### Complexity

O(1)

### Returns

Integer Reply: 1 if the document was deleted, 0 if not.

---

## FT.DROP

### Format

```
FT.DROP {index}
```

### Description

Deletes all the keys associated with the index. 

If no other data is on the redis instance, this is equivalent to FLUSHDB, apart from the fact
that the index specification is not deleted.

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

### Returns

Status Reply: OK on success.

---

## FT.OPTIMIZE

Format

```
FT.OPTIMIZE {index}
```

Description

After the index is built (and doesn't need to be updated again withuot a complete rebuild)
we can optimize memory consumption by trimming all index buffers to their actual size.

  **Warning 1**: Do not run it if you intend to update your index afterward.

  **Warning 2**: This blocks redis for a long time. Do not run it on production instances

### Parameters

* **index**: The Fulltext index name. The index must be first created with FT.CREATE

### Returns:

Integer Reply - the number of index entries optimized.

---

## FT.SUGGADD

### Format

```
FT.SUGADD {key} {string} {score} [INCR] [PAYLOAD {payload}]
```

### Description

Add a suggestion string to an auto-complete suggestion dictionary. This is disconnected from the
index definitions, and leaves creating and updating suggestino dictionaries to the user.

### Parameters

- **key**: the suggestion dictionary key.
- **string**: the suggestion string we index
- **score**: a floating point number of the suggestion string's weight
- **INCR**: if set, we increment the existing entry of the suggestion by the given score, instead of replacing the score. This is useful for updating the dictionary based on user queries in real time
- **PAYLOAD {payload}**: If set, we save an extra payload with the suggestion, that can be fetched by adding the `WITHPAYLOADS` argument to `FT.SUGGET`.

### Returns:

Integer Reply: the current size of the suggestion dictionary.

---

## FT.SUGGET

### Format

```
FT.SUGGET {key} {prefix} [FUZZY] [WITHPAYLOADS] [MAX num]
```

### Description

Get completion suggestions for a prefix

### Parameters:

- **key**: the suggestion dictionary key.
- **prefix**: the prefix to complete on
- **FUZZY**: if set,we do a fuzzy prefix search, including prefixes at levenshtein distance of 1 from the prefix sent
- **MAX num**: If set, we limit the results to a maximum of `num`. (**Note**: The default is 5, and the number cannot be greater than 10).
- **WITHSCORES**: If set, we also return the score of each suggestion. this can be used to merge results from multiple instances
- **WITHPAYLOADS**: If set, we return optional payloads saved along with the suggestions. If no payload is present for an entry, we return a Null Reply.

### Returns:

Array Reply: a list of the top suggestions matching the prefix, optionally with score after each entry

---

## FT.SUGDEL

### Format

```
FT.SUGDEL {key} {string}
```

### Description

Delete a string from a suggestion index. 

### Parameters

- **key**: the suggestion dictionary key.
- **string**: the string to delete

### Returns:

Integer Reply: 1 if the string was found and deleted, 0 otherwise.

----

## FT.SUGLEN

Format

```
FT.SUGLEN {key}
```

### Description

Get the size of an autoc-complete suggestion dictionary

### Parameters

* **key**: the suggestion dictionary key.

### Returns:

Integer Reply: the current size of the suggestion dictionary.

