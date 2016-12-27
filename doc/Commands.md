# RediSeach Full Command Documentation

## FT.CREATE 

### Format:
```
  FT.CREATE {index} {field} {weight}|NUMERIC [{field} {weight}|NUMERIC ...]
```

### Description:
Creates an index with the given spec. The index name will be used in all the key names
so keep it short!

### Parameters:

* **index**: the index name to create. If it exists the old spec will be overwritten

* **field weight|NUMERIC**:  pairs of field name and relative weight in scoring. The weight is a double, but does not need to be normalized. If NUMRERIC is set instead of a weight, the index will expect numeric values in this field.

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
	[LANGUAGE {language}] 
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

- **FIELDS**: Following the FIELDS specifier, we are looking for pairs of  `{field} {value}` to be indexed.

  Each field will be scored based on the index spec given in FT.CREATE. 
  Passing fields that are not in the index spec will make them be stored as part of the document, or ignored if NOSAVE is set 

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
 FT.ADDHASH {index} {docId} {score} [LANGUAGE language]
```

### Description

Add a documet to the index from an existing HASH key in Redis.

### Parameters:

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

-  **docId**: The document's id. This has to be an existing HASH key in redis that will hold the fields 
    the index needs.

- **score**: The document's rank based on the user's ranking. This must be between 0.0 and 1.0. 
  If you don't have a score just set it to 1

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

## FT.SEARCH 

### Format

```
FT.SEARCH {index} {query} [NOCONTENT] [VERBATIM] [NOSTOPWORDS] [WITHSCORES]
	[FILTER numeric_field min max]
	[LANGUAGE language]
	[EXPANDER expander]
	[INFIELDS num field ... ]
    [LIMIT offset num]
```

### Description

Search the index with a textual query, returning either documents or just ids.

### Parameters:

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **query**: the text query to search. If it's more than a single word, put it in quotes.
  See below for documentation on query syntax. 
- **NOCONTENT**: If it appears after the query, we only return the document ids and not 
  the content. This is useful if rediseach is only an index on an external document collection
- **LIMIT first num**: If the parameters appear after the query, we limit the results to 
  the offset and number of results given. The default is 0 10
- **INFIELDS num field ...**: If set, filter the results to ones appearing only in specific
  fields of the document, like title or url. num is the number of specified field arguments
- **FILTER numeric_field min max**: If set, and numeric_field is defined as a numeric field in 
  FT.CREATE, we will limit results to those having numeric values ranging between min and max.
  min and max follow ZRANGE syntax, and can be **-inf**, **+inf** and use `(` for exclusive ranges.
- **NOSTOPWORDS**: If set, we do not filter stopwords from the query. 
- **WITHSCORES**: If set, we also return the relative internal score of each document. this can be
  used to merge results from multiple instances
- **VERBATIM**: if set, we do not try to use stemming for query expansion but search the query terms verbatim.
- **LANGUAGE language**: If set, we use a stemmer for the supplied langauge during search for query expansion. 
  Defaults to English. If an unsupported language is sent, the command returns an error. See FT.ADD for the list of languages.
- EXPANDER expander: If set, we will use a custom query expander instead of the stemmer. Currently has no affect.


### Returns:

Array reply, where the first element is the total number of results, and then pairs of document id, and a nested array of field/value. 

If NOCONTENT was given, we return an array where the first element is the total number of results, and the rest of the members are document ids.

## Search Query Syntax (since 0.3):

  We support a simple syntax for complex queries with the following rules:

* Multi-word phrases are (AND) simply a list of tokens, e.g. `foo bar baz`.
* Exact phrases are wrapped in qoutes, e.e.g `"hello world"`.
    * OR unions i.e word1 OR word2, are expressed with a pipe (`|`), e.g. `hello|hallo|shalom|hola`.
    * An expression in a query can be wrapped in parentheses to resolve disambiguity, e.g. `(hello|hella) (world|werld)`.
    * Combinations of the above can be used together, e.g `hello (world|foo) "bar baz" bbbb`

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

   - **WITHSCORES**: If set, we also return the score of each suggestion. this can be
     used to merge results from multiple instances


### Returns:

> Array reply: a list of the top suggestions matching the prefix

## FT.SUGDEL key str

Delete a string from a suggestion index. 

### Parameters:

- **key**: the suggestion dictionary key.

- **str**: the string to delete

### Returns:

> Integer reply: 1 if the string was found and deleted, 0 otherwise.
