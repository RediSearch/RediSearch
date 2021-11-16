# RSCoordinator Command Documentation

This is the list of supported commands for the module. Note that most of them follow the exact semantics of  RediSearch.


## DFT.CLUSTERINFO 

### Format:
```
  DFT.CLUSTERINFO
```

### Description:

Returns the full list of partitions and slot ranges in the cluster.

---

## DFT.CREATE 

### Format:
```
  DFT.CREATE {index} 
    [NOOFFSETS] [NOFIELDS] [NOSCOREIDX]
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

* **SCHEMA {field} {options...}**: After the SCHEMA keyword we define the index fields. 
They can be numeric, textual or geographical. For textual fields we optionally specify a weight. The default weight is 1.0.

Numeric or text field can have the optional SORTABLE argument that allows the user to later [sort the results by the value of this field](/Sorting) (this adds memory overhead so do not declare it on large text fields).

### Complexity
O(1)

### Returns:
OK or an error

---


## DFT.ADD 

### Format:

```
DFT.ADD {index} {docId} {score} 
  [NOSAVE]
  [REPLACE]
  [LANGUAGE {language}] 
  [PAYLOAD {payload}]
  FIELDS {field} {value} [{field} {value}...]
```

### Description

Add a documet to the index.

### Parameters:

- **index**: The Fulltext index name. The index must be first created with DFT.CREATE

- **docId**: The document's id that will be returned from searches. 
  Note that the same docId cannot be added twice to the same index

- **score**: The document's rank based on the user's ranking. This must be between 0.0 and 1.0. 
  If you don't have a score just set it to 1

- **NOSAVE**: If set to true, we will not save the actual document in the index and only index it.

- **REPLACE**: If set, we will do an UPSERT style insertion - and delete an older version of the document if it exists.

- **FIELDS**: Following the FIELDS specifier, we are looking for pairs of  `{field} {value}` to be indexed.

  Each field will be scored based on the index spec given in DFT.CREATE. 
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

## DFT.ADDHASH

### Format

```
 DFT.ADDHASH {index} {docId} {score} [LANGUAGE language] [REPLACE]
```

### Description

Add a documet to the index from an existing HASH key in Redis.

### Parameters:

- **index**: The Fulltext index name. The index must be first created with DFT.CREATE

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

## DFT.INFO

### Format

```
DFT.INFO {index} 
```

### Description

Return information and statistics on the index. It will return information on all sub indexes. Returned values include:

* Number of documents.
* Number of distinct terms.
* Average bytes per record.
* Size and capacity of the index buffers.


### Parameters

- **index**: The Fulltext index name. The index must be first created with DFT.CREATE

### Complexity

O(1)

### Returns

Array Response. A nested array of keys and values.

---

## DFT.SEARCH 

### Format

```
DFT.SEARCH {index} {query} [NOCONTENT] [VERBATIM] [NOSTOPWORDS] [WITHSCORES] [WITHPAYLOADS]
  [FILTER {numeric_field} {min} {max}] ...
  [GEOFILTER {geo_field} {lon} {lat} {raius} m|km|mi|ft]
  [INKEYS {num} {key} ... ]
  [INFIELDS {num {field} ... ]
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

- **index**: The Fulltext index name. The index must be first created with DFT.CREATE
- **query**: the text query to search. If it's more than a single word, put it in quotes.
  See below for documentation on query syntax. 
- **NOCONTENT**: If it appears after the query, we only return the document ids and not 
  the content. This is useful if rediseach is only an index on an external document collection
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
  DFT.CREATE, we will limit results to those having numeric values ranging between min and max.
  min and max follow ZRANGE syntax, and can be **-inf**, **+inf** and use `(` for exclusive ranges. 
  Multiple numeric filters for different fields are supported in one query.
- **GEOFILTER {geo_field} {lon} {lat} {raius} m|km|mi|ft**: If set, we filter the results to a given radius 
  from lon and lat. Radius is given as a number and units. See [GEORADIUS](https://redis.io/commands/georadius) for more details. 
- **NOSTOPWORDS**: If set, we do not filter stopwords from the query. 
- **WITHSCORES**: If set, we also return the relative internal score of each document. this can be
  used to merge results from multiple instances
- **VERBATIM**: if set, we do not try to use stemming for query expansion but search the query terms verbatim.
- **LANGUAGE {language}**: If set, we use a stemmer for the supplied langauge during search for query expansion. 
  Defaults to English. If an unsupported language is sent, the command returns an error. See DFT.ADD for the list of languages.
- **EXPANDER {expander}**: If set, we will use a custom query expander instead of the stemmer. [See Extensions](/Extensions).
- **SCORER {scorer}**: If set, we will use a custom scoring function defined by the user. [See Extensions](/Extensions).
- **PAYLOAD {payload}**: Add an arbitrary, binary safe payload that will be exposed to custom scoring functions. [See Extensions](/Extensions).
- **WITHPAYLOADS**: If set, we retrieve optional document payloads (see DFT.ADD). 
  the payloads follow the document id, and if `WITHSCORES` was set, follow the scores.
- **SORTBY {field} [ASC|DESC]**: If specified, and field is a [sortable field](/Sorting), the results are ordered by the value of this field. This applies to both text and numeric fields.

### Complexity

O(n) for single word queries (though for popular words we save a cache of the top 50 results).

Complexity for complex queries changes, but in general it's proportional to the number of words and the number of intersection points between them.

### Returns

**Array reply,** where the first element is the total number of results, and then pairs of document id, and a nested array of field/value. 

If **NOCONTENT** was given, we return an array where the first element is the total number of results, and the rest of the members are document ids.

----

## DFT.DEL

### Format

```
DFT.DEL {index} {doc_id}
```

### Description

Delete a document from the index. Returns 1 if the document was in the index, or 0 if not. 

After deletion, the document can be re-added to the index. It will get a different internal id and will be a new document from the index's POV.

**NOTE**: This does not actually delete the document from the index, just marks it as deleted. 
Thus, deleting and re-inserting the same document over and over will inflate the index size with each re-insertion.

### Parameters

- **index**: The Fulltext index name. The index must be first created with DFT.CREATE
- **doc_id**: the id of the document to be deleted. It does not actually delete the HASH key in which the document is stored. Use DEL to do that manually if needed.


### Complexity

O(1)

### Returns

Integer Reply: 1 if the document was deleted, 0 if not.

---

## DFT.DELETE

### Format

```
DFT.DELETE {index} [DD]
```

### Description

Deletes the index.

### Parameters

- **index**: The Fulltext index name. The index must be first created with DFT.CREATE
- **DD**:  All documents associated with the index will be deleted.

### Returns

Status Reply: OK on success.

---

## DFT.DROP

### Format

```
DFT.DROP {index} [KEEPDOCS]
```

### Description

!!! warning "This command is deprecated"

Deletes the index and all the keys associated with it. 

If no other data is on the redis instance, this is equivalent to FLUSHDB, apart from the fact
that the index specification is not deleted.

### Parameters

- **index**: The Fulltext index name. The index must be first created with DFT.CREATE
- **KEEPDOCS**: Documents associated with the index will not be deleted.

### Returns

Status Reply: OK on success.

---

## DFT.BROADCAST

### Format

```
DFT.BROADCAST [ANY REDIS COMMAND]
```

### Description

Broadcasts any redis commands to all the shards containing index partitions. 

---


