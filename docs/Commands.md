# RediSearch Full Command Documentation

## FT.CREATE 

### Format
```
  FT.CREATE {index} 
    [MAXTEXTFIELDS] [TEMPORARY {seconds}] [NOOFFSETS] [NOHL] [NOFIELDS] [NOFREQS]
    [STOPWORDS {num} {stopword} ...]
    SCHEMA {field} [TEXT [NOSTEM] [WEIGHT {weight}] [PHONETIC {matcher}] | NUMERIC | GEO | TAG [SEPARATOR {sep}] ] [SORTABLE][NOINDEX] ...
```

### Description
Creates an index with the given spec. The index name will be used in all the key names so keep it short!

!!! warning "Note on field number limits"
        
        RediSearch supports up to 1024 fields per schema, out of which at most 128 can be TEXT fields.
    
        On 32 bit builds, at most 64 fields can be TEXT fields.
    
        Note that the more fields you have, the larger your index will be, as each additional 8 fields require one extra byte per index record to encode.
    
        You can always use the `NOFIELDS` option and not encode field information into the index, for saving space, if you do not need filtering by text fields. This will still allow filtering by numeric and geo fields.

#### Example
```sql
FT.CREATE idx SCHEMA name TEXT SORTABLE age NUMERIC SORTABLE myTag TAG SORTABLE
```

### Parameters

* **index**: the index name to create. If it exists the old spec will be overwritten

* **MAXTEXTFIELDS**: For efficiency, RediSearch encodes indexes differently if they are
  created with less than 32 text fields. This option forces RediSearch to encode indexes as if
  there were more than 32 text fields, which allows you to add additional fields (beyond 32)
  using `FT.ALTER`.

* **NOOFFSETS**: If set, we do not store term offsets for documents (saves memory, does not
  allow exact searches or highlighting). Implies `NOHL`.

* **TEMPORARY**: Create a lightweight temporary index which will expire after the specified period of inactivity. The internal idle timer is reset whenever the index is searched or added to. Because such indexes are lightweight, you can create thousands of such indexes without negative performance implications.

* **NOHL**: Conserves storage space and memory by disabling highlighting support. If set, we do
  not store corresponding byte offsets for term positions. `NOHL` is also implied by `NOOFFSETS`.

* **NOFIELDS**: If set, we do not store field bits for each term. Saves memory, does not allow
  filtering by specific fields.

* **NOFREQS**: If set, we avoid saving the term frequencies in the index. This saves
  memory but does not allow sorting based on the frequencies of a given term within
  the document.

* **STOPWORDS**: If set, we set the index with a custom stopword list, to be ignored during
  indexing and search time. {num} is the number of stopwords, followed by a list of stopword
  arguments exactly the length of {num}. 

    If not set, we take the default list of stopwords. 

    If **{num}** is set to 0, the index will not have stopwords.

* **SCHEMA {field} {options...}**: After the SCHEMA keyword we define the index fields. They
  can be numeric, textual or geographical. For textual fields we optionally specify a weight.
  The default weight is 1.0.

    ### Field Options


    * **SORTABLE**
    
        Numeric, tag or text field can have the optional SORTABLE argument that allows the user to later [sort the results by the value of this field](Sorting.md) (this adds memory overhead so do not declare it on large text fields).
      
    * **NOSTEM**
    
        Text fields can have the NOSTEM argument which will disable stemming when indexing its values. 
        This may be ideal for things like proper names.
      
    * **NOINDEX**
    
        Fields can have the `NOINDEX` option, which means they will not be indexed. 
        This is useful in conjunction with `SORTABLE`, to create fields whose update using PARTIAL will not cause full reindexing of the document. If a field has NOINDEX and doesn't have SORTABLE, it will just be ignored by the index.
    
    * **PHONETIC {matcher}**
    
        Declaring a text field as `PHONETIC` will perform phonetic matching on it in searches by default. The obligatory {matcher} argument specifies the phonetic algorithm and language used. The following matchers are supported:
    
        * `dm:en` - Double Metaphone for English
        * `dm:fr` - Double Metaphone for French
        * `dm:pt` - Double Metaphone for Portuguese
        * `dm:es` - Double Metaphone for Spanish
    
        For more details see [Phonetic Matching](Phonetic_Matching.md).
    
    * **WEIGHT {weight}**

        For `TEXT` fields, declares the importance of this field when
        calculating result accuracy. This is a multiplication factor, and
        defaults to 1 if not specified.
    
    * **SEPARATOR {sep}**

        For `TAG` fields, indicates how the text contained in the field
        is to be split into individual tags. The default is `,`. The value
        must be a single character.
    
    

### Complexity
O(1)

### Returns
OK or an error

---

## FT.ADD 

### Format

```
FT.ADD {index} {docId} {score} 
  [NOSAVE]
  [REPLACE [PARTIAL] [NOCREATE]]
  [LANGUAGE {language}] 
  [PAYLOAD {payload}]
  [IF {condition}]
  FIELDS {field} {value} [{field} {value}...]
```

### Description

Adds a document to the index.

#### Example
```sql
FT.ADD idx doc1 1.0 FIELDS title "hello world"
```

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

- **docId**: The document's id that will be returned from searches. 

!!! note "Notes on docId"

        The same docId cannot be added twice to the same index.
    
        The same docId can be added to multiple indices, but a single document with that docId is saved in the database.

- **score**: The document's rank based on the user's ranking. This must be between 0.0 and 1.0. 
  If you don't have a score just set it to 1

- **NOSAVE**: If set to true, we will not save the actual document in the database and only index it.

- **REPLACE**: If set, we will do an UPSERT style insertion - and delete an older version of the
  document if it exists. 

- **PARTIAL** (only applicable with REPLACE): If set, you do not have to specify all fields for
  reindexing. Fields not given to the command will be loaded from the current version of the
  document. Also, if only non-indexable fields, score or payload are set - we do not do a full
  re-indexing of the document, and this will be a lot faster.

- **NOCREATE** (only applicable with REPLACE): If set, the document is only updated
  and reindexed if it already exists. If the document does not exist, an error
  will be returned.

- **FIELDS**: Following the FIELDS specifier, we are looking for pairs of  `{field} {value}` to be
  indexed. Each field will be scored based on the index spec given in `FT.CREATE`. 
  Passing fields that are not in the index spec will make them be stored as part of the document,
  or ignored if NOSAVE is set 

- **PAYLOAD {payload}**: Optionally set a binary safe payload string to the document, 
  that can be evaluated at query time by a custom scoring function, or retrieved to the client.

- **IF {condition}**: (Applicable only in conjunction with `REPLACE` and optionally `PARTIAL`). 
  Update the document only if a boolean expression applies to the document **before the update**, 
  e.g. `FT.ADD idx doc 1 REPLACE IF "@timestamp < 23323234234"`. 

  The expression is evaluated atomically before the update, ensuring that the update will happen only if it is true.

  See [Aggregations](Aggregations.md) for more details on the expression language. 

- **LANGUAGE language**: If set, we use a stemmer for the supplied language during indexing. Default
  to English. 
  If an unsupported language is sent, the command returns an error. 
  The supported languages are:

    > "arabic",  "danish",    "dutch",   "english",   "finnish",    "french",
    > "german",  "hungarian", "italian", "norwegian", "portuguese", "romanian",
    > "russian", "spanish",   "swedish", "tamil",     "turkish"
    > "chinese"

  If indexing a Chinese language document, you must set the language to `chinese`
  in order for Chinese characters to be tokenized properly.

### Adding Chinese Documents

When adding Chinese-language documents, `LANGUAGE chinese` should be set in
order for the indexer to properly tokenize the terms. If the default language
is used then search terms will be extracted based on punctuation characters and
whitespace. The Chinese language tokenizer makes use of a segmentation algorithm
(via [Friso](https://github.com/lionsoul2014/friso)) which segments texts and
checks it against a predefined dictionary. See [Stemming](Stemming.md) for more
information.

### Complexity

O(n), where n is the number of tokens in the document

### Returns

OK on success, or an error if something went wrong.

A special status `NOADD` is returned if an `IF` condition evaluated to false.

!!! warning "FT.ADD with REPLACE and PARTIAL"
        
        By default, FT.ADD does not allow updating the document, and will fail if it already exists in the index.
    
        However, updating the document is possible with the REPLACE and REPLACE PARTIAL options.
    
        **REPLACE**: On its own, sets the document to the new values, and reindexes it. Any fields not given will not be loaded from the current version of the document.
    
        **REPLACE PARTIAL**: When both arguments are used, we can update just part of the document fields, and the rest will be loaded before reindexing. Not only that, but if only the score, payload and non-indexed fields (using NOINDEX) are updated, we will not actually reindex the document, just update its metadata internally, which is a lot faster and does not create index garbage.

---

### Warning!!!

FT.ADD will actually create a hash in Redis with the given fields and value. This means that if the hash already exists, it will override with the new values. Moreover, if you try to add a document with the same id to two different indexes one of them will override the other and you will get wrong responses from one of the indexes.
For this reason, it is recommended to create global unique documents ids (this can e.g. be achieved by adding the index name to the document id as prefix).

## FT.ADDHASH

### Format

```
 FT.ADDHASH {index} {docId} {score} [LANGUAGE language] [REPLACE]
```

### Description

Adds a document to the index from an existing HASH key in Redis.

#### Example
```sql
FT.ADDHASH idx hash1 1.0 REPLACE
```

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

-  **docId**: The document's id. This has to be an existing HASH key in Redis that will hold the fields 
    the index needs.

- **score**: The document's rank based on the user's ranking. This must be between 0.0 and 1.0. 
  If you don't have a score just set it to 1

- **REPLACE**: If set, we will do an UPSERT style insertion - and delete an older version of the document if it exists.

- **LANGUAGE language**: If set, we use a stemmer for the supplied language during indexing. Defaults 
  to English. 
  If an unsupported language is sent, the command returns an error. 
  The supported languages are:

  > "arabic",  "danish",    "dutch",   "english",   "finnish",    "french",
  > "german",  "hungarian", "italian", "norwegian", "portuguese", "romanian",
  > "russian", "spanish",   "swedish", "tamil",     "turkish"

### Complexity

O(n), where n is the number of tokens in the document

### Returns

OK on success, or an error if something went wrong.

---

## FT.ALTER SCHEMA ADD

### Format

```
FT.ALTER {index} SCHEMA ADD {field} {options} ...
```

### Description

Adds a new field to the index.

Adding a field to the index will cause any future document updates to use the new field when
indexing. Existing documents will not be reindexed.

!!! note
    Depending on how the index was created, you may be limited by the amount of additional text
    fields which can be added to an existing index. If the current index contains less than 32
    text fields, then `SCHEMA ADD` will only be able to add up to 32 fields (meaning that the
    index will only ever be able to contain 32 total text fields). If you wish for the index to
    contain more than 32 fields, create it with the `MAXTEXTFIELDS` option.

#### Example
```sql
FT.ALTER idx SCHEMA ADD id2 NUMERIC SORTABLE
```

### Parameters

* **index**: the index name.
* **field**: the field name.
* **options**: the field options - refer to `FT.CREATE` for more information.

### Complexity

O(1)

### Returns

OK or an error.


---

## FT.ALIASADD
## FT.ALIASUPDATE
## FT.ALIASDEL

### Format

```
FT.ALIASADD {name} {index}
FT.ALIASUPDATE {name} {index}
FT.ALIASDEL {name}
FT.ALTER {index} ALIAS DEL {alias}
```

The `FT.ALIASADD` and `FT.ALIASDEL` commands will add or remove an alias from
an index. Index aliases can be used to refer to actual indexes in data
commands such as `FT.SEARCH` or `FT.ADD`. This allows an administrator
to transparently redirect application queries to alternative indexes.

Indexes can have more than one alias, though an alias cannot refer to another
alias.

The `FT.ALIASUPDATE` command differs from the `FT.ALIASADD` command in that
it will remove the alias association with a previous index, if any. `FT.ALIASDD`
will fail, on the other hand, if the alias is already associated with another
index.

### Complexity

O(1)

### Returns

OK or an error.

---

## FT.INFO

### Format

```
FT.INFO {index} 
```

### Description

Returns information and statistics on the index. Returned values include:

* Number of documents.
* Number of distinct terms.
* Average bytes per record.
* Size and capacity of the index buffers.

#### Example
```bash
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
  [SUMMARIZE [FIELDS {num} {field} ... ] [FRAGS {num}] [LEN {fragsize}] [SEPARATOR {separator}]]
  [HIGHLIGHT [FIELDS {num} {field} ... ] [TAGS {open} {close}]]
  [SLOP {slop}] [INORDER]
  [LANGUAGE {language}]
  [EXPANDER {expander}]
  [SCORER {scorer}]
  [PAYLOAD {payload}]
  [SORTBY {field} [ASC|DESC]]
  [LIMIT offset num]
```

### Description

Searches the index with a textual query, returning either documents or just ids.

### Example
```sql
FT.SEARCH idx "@text:morphix=>{$phonetic:false}"
```

### Parameters

- **index**: The index name. The index must be first created with `FT.CREATE`.
- **query**: the text query to search. If it's more than a single word, put it in quotes.
  Refer to [query syntax](Query_Syntax.md) for more details. 
  
- **NOCONTENT**: If it appears after the query, we only return the document ids and not 
  the content. This is useful if RediSearch is only an index on an external document collection
- **VERBATIM**: if set, we do not try to use stemming for query expansion but search the query terms 
  verbatim.
- **NOSTOPWORDS**: If set, we do not filter stopwords from the query.
- **WITHSCORES**: If set, we also return the relative internal score of each document. this can be
  used to merge results from multiple instances
- **WITHPAYLOADS**: If set, we retrieve optional document payloads (see FT.ADD). 
  the payloads follow the document id, and if `WITHSCORES` was set, follow the scores.
- **WITHSORTKEYS**: Only relevant in conjunction with **SORTBY**. Returns the value of the sorting key,
  right after the id and score and /or payload if requested. This is usually not needed by users, and 
  exists for distributed search coordination purposes.
  
- **FILTER numeric_field min max**: If set, and numeric_field is defined as a numeric field in 
  FT.CREATE, we will limit results to those having numeric values ranging between min and max.
  min and max follow ZRANGE syntax, and can be **-inf**, **+inf** and use `(` for exclusive ranges. 
  Multiple numeric filters for different fields are supported in one query.
- **GEOFILTER {geo_field} {lon} {lat} {radius} m|km|mi|ft**: If set, we filter the results to a given radius 
  from lon and lat. Radius is given as a number and units. See [GEORADIUS](https://redis.io/commands/georadius) 
  for more details.
- **INKEYS {num} {field} ...**: If set, we limit the result to a given set of keys specified in the 
  list. 
  the first argument must be the length of the list, and greater than zero.
  Non-existent keys are ignored - unless all the keys are non-existent.
- **INFIELDS {num} {field} ...**: If set, filter the results to ones appearing only in specific
  fields of the document, like title or URL. num is the number of specified field arguments  
  
- **RETURN {num} {field} ...**: Use this keyword to limit which fields from the document are returned.
  `num` is the number of fields following the keyword. If `num` is 0, it acts like `NOCONTENT`.  
- **SUMMARIZE ...**: Use this option to return only the sections of the field which contain the 
  matched text.
  See [Highlighting](Highlight.md) for more details
- **HIGHLIGHT ...**: Use this option to format occurrences of matched text. See [Highligting](Highlight.md) for more
  details
- **SLOP {slop}**: If set, we allow a maximum of N intervening number of unmatched offsets between 
  phrase terms. (i.e the slop for exact phrases is 0)
- **INORDER**: If set, and usually used in conjunction with SLOP, we make sure the query terms appear 
  in the same order in the document as in the query, regardless of the offsets between them. 
- **LANGUAGE {language}**: If set, we use a stemmer for the supplied language during search for query 
  expansion.
  If querying documents in Chinese, this should be set to `chinese` in order to
  properly tokenize the query terms. 
  Defaults to English. If an unsupported language is sent, the command returns an error.
  See FT.ADD for the list of languages.

- **EXPANDER {expander}**: If set, we will use a custom query expander instead of the stemmer. [See Extensions](Extensions.md).
- **SCORER {scorer}**: If set, we will use a custom scoring function defined by the user. [See Extensions](Extensions.md).
- **PAYLOAD {payload}**: Add an arbitrary, binary safe payload that will be exposed to custom scoring 
  functions. [See Extensions](Extensions.md).
  
- **SORTBY {field} [ASC|DESC]**: If specified, and field is a [sortable field](Sorting.md), the results 
  are ordered by the value of this field. This applies to both text and numeric fields.
- **LIMIT first num**: If the parameters appear after the query, we limit the results to 
  the offset and number of results given. The default is 0 10.
  Note that you can use `LIMIT 0 0` to count the number of documents in
  the resultset without actually returning them.

### Complexity

O(n) for single word queries (though for popular words we save a cache of the top 50 results).

Complexity for complex queries changes, but in general it's proportional to the number of words and the number of intersection points between them.

### Returns

**Array reply,** where the first element is the total number of results, and then pairs of document id, and a nested array of field/value. 

If **NOCONTENT** was given, we return an array where the first element is the total number of results, and the rest of the members are document ids.

---

## FT.AGGREGATE 

### Format

```
FT.AGGREGATE  {index_name}
  {query_string}
  [WITHSCHEMA] [VERBATIM]
  [LOAD {nargs} {property} ...]
  [GROUPBY {nargs} {property} ...
    REDUCE {func} {nargs} {arg} ... [AS {name:string}]
    ...
  ] ...
  [SORTBY {nargs} {property} [ASC|DESC] ... [MAX {num}]]
  [APPLY {expr} AS {alias}] ...
  [LIMIT {offset} {num}] ...
  [FILTER {expr}] ...
```

### Description

Runs a search query on an index, and performs aggregate transformations on the results, extracting statistics etc from them. See [the full documentation on aggregations](Aggregations.md) for further details.

### Example
```sql
FT.AGGREGATE idx "@url:\"about.html\""
    APPLY "@timestamp - (@timestamp % 86400)" AS day
    GROUPBY 2 @day @country
    	REDUCE count 0 AS num_visits 
    SORTBY 4 @day ASC @country DESC
```

### Parameters

* **index_name**: The index the query is executed against.

* **query_string**: The base filtering query that retrieves the documents. It follows
  **the exact same syntax** as the search query, including filters, unions, not, optional, etc.

* **LOAD {nargs} {property} …**: Load document fields from the document HASH objects. This should be 
  avoided as a general rule of thumb. Fields needed for aggregations should be stored as **SORTABLE**, 
  where they are available to the aggregation pipeline with very load latency. LOAD hurts the 
  performance of aggregate queries considerably, since every processed record needs to execute the 
  equivalent of HMGET against a Redis key, which when executed over millions of keys, amounts to very 
  high processing times. 

* **GROUPBY {nargs} {property}**: Group the results in the pipeline based on one or more properties. 
  Each group should have at least one reducer (See below), a function that handles the group entries, 
  either counting them, or performing multiple aggregate operations (see below).
    * **REDUCE {func} {nargs} {arg} … [AS {name}]**: Reduce the matching results in each group into a single record, using a reduction function. For example COUNT will count the number of records in the group. See the Reducers section below for more details on available reducers. 
    
          The reducers can have their own property names using the `AS {name}` optional argument. If a name is not given, the resulting name will be the name of the reduce function and the group properties. For example, if a name is not given to COUNT_DISTINCT by property `@foo`, the resulting name will be `count_distinct(@foo)`. 

* **SORTBY {nargs} {property} {ASC|DESC} [MAX {num}]**: Sort the pipeline up until the point of SORTBY,
  using a list of properties. By default, sorting is ascending, but `ASC` or `DESC ` can be added for 
  each property. `nargs` is the number of sorting parameters, including ASC and DESC. for example: 
  `SORTBY 4 @foo ASC @bar DESC`. 

    `MAX` is used to optimized sorting, by sorting only for the n-largest elements. Although it is not connected to `LIMIT`, you usually need just `SORTBY … MAX` for common queries. 

* **APPLY {expr} AS {name}**: Apply a 1-to-1 transformation on one or more properties, and either 
  store the result as a new property down the pipeline, or replace any property using this 
  transformation. `expr` is an expression that can be used to perform arithmetic operations on numeric 
  properties, or functions that can be applied on properties depending on their types (see below), or 
  any combination thereof. For example: `APPLY "sqrt(@foo)/log(@bar) + 5" AS baz` will evaluate this 
  expression dynamically for each record in the pipeline and store the result as a new property called 
  baz, that can be referenced by further APPLY / SORTBY / GROUPBY / REDUCE operations down the 
  pipeline. 

* **LIMIT {offset} {num}**. Limit the number of results to return just `num` results starting at index 
  `offset` (zero-based). AS mentioned above, it is much more efficient to use `SORTBY … MAX` if you 
  are interested in just limiting the output of a sort operation.

    However, limit can be used to limit results without sorting, or for paging the n-largest results as determined by `SORTBY MAX`. For example, getting results 50-100 of the top 100 results is most efficiently expressed as `SORTBY 1 @foo MAX 100 LIMIT 50 50`. Removing the MAX from SORTBY will result in the pipeline sorting _all_ the records and then paging over results 50-100. 

* **FILTER {expr}**. Filter the results using predicate expressions relating to values in each result. 
  They are is applied post-query and relate to the current state of the pipeline. 

### Complexity

Non-deterministic. Depends on the query and aggregations performed, but it is usually linear to the number of results returned. 

### Returns

Array Response. Each row is an array and represents a single aggregate result.

### Example output

Here we are counting GitHub events by user (actor), to produce the most active users:

```sh
127.0.0.1:6379> FT.AGGREGATE gh "*" GROUPBY 1 @actor REDUCE COUNT 0 AS num SORTBY 2 @num DESC MAX 10
 1) (integer) 284784
 2) 1) "actor"
    2) "lombiqbot"
    3) "num"
    4) "22197"
 3) 1) "actor"
    2) "codepipeline-test"
    3) "num"
    4) "17746"
 4) 1) "actor"
    2) "direwolf-github"
    3) "num"
    4) "10683"
 5) 1) "actor"
    2) "ogate"
    3) "num"
    4) "6449"
 6) 1) "actor"
    2) "openlocalizationtest"
    3) "num"
    4) "4759"
 7) 1) "actor"
    2) "digimatic"
    3) "num"
    4) "3809"
 8) 1) "actor"
    2) "gugod"
    3) "num"
    4) "3512"
 9) 1) "actor"
    2) "xdzou"
    3) "num"
    4) "3216"
10) 1) "actor"
    2) "opstest"
    3) "num"
    4) "2863"
11) 1) "actor"
    2) "jikker"
    3) "num"
    4) "2794"
(0.59s)
```

---

## FT.EXPLAIN

### Format

```
FT.EXPLAIN {index} {query}
```

### Description

Returns the execution plan for a complex query.

In the returned response, a `+` on a term is an indication of stemming. 

### Example
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

- **index**: The index name. The index must be first created with FT.CREATE
- **query**: The query string, as if sent to FT.SEARCH

### Complexity

O(1)

### Returns

String Response. A string representing the execution plan (see above example). 

**Note**: You should use `redis-cli --raw` to properly read line-breaks in the returned response.

---

## FT.EXPLAINCLI

### Format

```
FT.EXPLAINCLI {index} {query}
```

### Description

Returns the execution plan for a complex query but formatted for easier reading without using `redis-cli --raw`.

In the returned response, a `+` on a term is an indication of stemming. 

### Example
```sh
$ redis-cli

127.0.0.1:6379> FT.EXPLAINCLI rd "(foo bar)|(hello world) @date:[100 200]|@date:[500 +inf]"
 1) INTERSECT {
 2)   UNION {
 3)     INTERSECT {
 4)       UNION {
 5)         foo
 6)         +foo(expanded)
 7)       }
 8)       UNION {
 9)         bar
10)         +bar(expanded)
11)       }
12)     }
13)     INTERSECT {
14)       UNION {
15)         hello
16)         +hello(expanded)
17)       }
18)       UNION {
19)         world
20)         +world(expanded)
21)       }
22)     }
23)   }
24)   UNION {
25)     NUMERIC {100.000000 <= @date <= 200.000000}
26)     NUMERIC {500.000000 <= @date <= inf}
27)   }
28) }
29)
```

### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **query**: The query string, as if sent to FT.SEARCH

### Complexity

O(1)

### Returns

String Response. A string representing the execution plan (see above example). 

---

## FT.DEL

### Format

```
FT.DEL {index} {doc_id} [DD]
```

### Description

Deletes a document from the index. Returns 1 if the document was in the index, or 0 if not. 

After deletion, the document can be re-added to the index. It will get a different internal id and will be a new document from the index's POV.

!!! warning "FT.DEL does not delete the actual document By default!"
        
        Since RediSearch regards documents as separate entities to the index and allows things like adding existing documents or indexing without saving the document - by default FT.DEL only deletes the reference to the document from the index, not the actual Redis HASH key where the document is stored. 
    
        Specifying **DD** (Delete Document) after the document ID, will make RediSearch also delete the actual document **if it is in the index**.
        
        Alternatively, you can just send an extra **DEL {doc_id}** to redis and delete the document directly. You can run both of them in a MULTI transaction.

### Example
```sql
FT.DEL idx doc1 
```

### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **doc_id**: the id of the document to be deleted. It does not actually delete the HASH key in which 
  the document is stored. Use DEL to do that manually if needed.


### Complexity

O(1)

### Returns

Integer Reply: 1 if the document was deleted, 0 if not.

---

## FT.GET

### Format

```
FT.GET {index} {doc id}
```

### Description

Returns the full contents of a document.

Currently it is equivalent to HGETALL, but this is future-proof and will allow us to change the internal representation of documents inside Redis in the future. In addition, it allows simpler implementation of fetching documents in clustered mode.

If the document does not exist or is not a HASH object, we return a NULL reply

### Example
```sql
FT.GET idx doc1 
```

### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **documentId**: The id of the document as inserted to the index

### Returns

Array Reply: Key-value pairs of field names and values of the document

---

## FT.MGET

### Format

```
FT.MGET {index} {docId} ...
```

### Description

Returns the full contents of multiple documents. 

Currently it is equivalent to calling multiple HGETALL commands, although faster. 
This command is also future-proof and will allow us to change the internal representation of documents inside Redis in the future. 
In addition, it allows simpler implementation of fetching documents in clustered mode.

We return an array with exactly the same number of elements as the number of keys sent to the command. 

Each element, in turn, is an array of key-value pairs representing the document. 

If a document is not found or is not a valid HASH object, its place in the parent array is filled with a Null reply object.

### Example
```sql
FT.MGET idx doc1 doc2
```

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **documentIds**: The ids of the requested documents as inserted to the index

### Returns

Array Reply: An array with exactly the same number of elements as the number of keys sent to the command.  Each element in it is either an array representing the document or Null if it was not found.

---

## FT.DROP

### Format

```
FT.DROP {index} [KEEPDOCS]
```

### Description

Deletes all the keys associated with the index. 

By default, DROP deletes the document hashes as well, but adding the KEEPDOCS option keeps the documents in place, ready for re-indexing.

If no other data is on the Redis instance, this is equivalent to FLUSHDB, apart from the fact
that the index specification is not deleted.

### Example
```sql
FT.DROP idx KEEPDOCS 
```

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **KEEPDOCS**: If set, the drop operation will not delete the actual document hashes.

### Returns

Status Reply: OK on success.

---

## FT.TAGVALS

### Format

```
FT.TAGVALS {index} {field_name}
```

### Description

Returns the distinct tags indexed in a [Tag field](Tags.md). 

This is useful if your tag field indexes things like cities, categories, etc.

!!! warning "Limitations"

      There is no paging or sorting, the tags are not alphabetically sorted. 
    
      This command only operates on [Tag fields](Tags.md).  
    
      The strings return lower-cased and stripped of whitespaces, but otherwise unchanged.
      
### Example
```sql
FT.TAGVALS idx myTag 
```

### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **filed_name**: The name of a Tag file defined in the schema.

### Returns

Array Reply: All the distinct tags in the tag index.

### Complexity

O(n), n being the cardinality of the tag field.

---

## FT.SUGADD

### Format

```
FT.SUGADD {key} {string} {score} [INCR] [PAYLOAD {payload}]
```

### Description

Adds a suggestion string to an auto-complete suggestion dictionary. This is disconnected from the
index definitions, and leaves creating and updating suggestions dictionaries to the user.

### Example
```sql
FT.SUGADD ac "hello world" 1
```

### Parameters

- **key**: the suggestion dictionary key.
- **string**: the suggestion string we index
- **score**: a floating point number of the suggestion string's weight
- **INCR**: if set, we increment the existing entry of the suggestion by the given score, instead of 
  replacing the score. This is useful for updating the dictionary based on user queries in real time
- **PAYLOAD {payload}**: If set, we save an extra payload with the suggestion, that can be fetched by 
  adding the `WITHPAYLOADS` argument to `FT.SUGGET`.

### Returns

Integer Reply: the current size of the suggestion dictionary.

---

## FT.SUGGET

### Format

```
FT.SUGGET {key} {prefix} [FUZZY] [WITHSCORES] [WITHPAYLOADS] [MAX num]
```

### Description

Gets completion suggestions for a prefix.

### Example
```sql
FT.SUGGET ac hell FUZZY MAX 3 WITHSCORES
```

### Parameters

- **key**: the suggestion dictionary key.
- **prefix**: the prefix to complete on
- **FUZZY**: if set, we do a fuzzy prefix search, including prefixes at Levenshtein distance of 1 from 
  the prefix sent
- **MAX num**: If set, we limit the results to a maximum of `num` (default: 5).
- **WITHSCORES**: If set, we also return the score of each suggestion. this can be used to merge 
  results from multiple instances
- **WITHPAYLOADS**: If set, we return optional payloads saved along with the suggestions. If no 
  payload is present for an entry, we return a Null Reply.

### Returns

Array Reply: a list of the top suggestions matching the prefix, optionally with score after each entry

---

## FT.SUGDEL

### Format

```
FT.SUGDEL {key} {string}
```

### Description

Deletes a string from a suggestion index. 

### Example
```sql
FT.SUGDEL ac "hello world"
```

### Parameters

- **key**: the suggestion dictionary key.
- **string**: the string to delete

### Returns

Integer Reply: 1 if the string was found and deleted, 0 otherwise.

---

## FT.SUGLEN

### Format

```
FT.SUGLEN {key}
```

### Description

Gets the size of an auto-complete suggestion dictionary

### Example
```sql
FT.SUGDEL ac 
```

### Parameters

* **key**: the suggestion dictionary key.

### Returns

Integer Reply: the current size of the suggestion dictionary.

---

## FT.OPTIMIZE

!!! warning "This command is deprecated"
    Index optimizations are done by the internal garbage collector in the background. Client libraries should not implement this command and remove it if they haven't already.

### Format

```
FT.OPTIMIZE {index}
```

### Description

This command is deprecated. 

---

## FT.SYNADD

### Format

```
FT.SYNADD <index name> <term1> <term2> ...
```

### Description

Adds a synonym group.

The command is used to create a new synonyms group. The command returns the synonym group id which can later be used to add additional terms to that synonym group. Only documents which was indexed after the adding operation will be effected.

---

## FT.SYNUPDATE

### Format

```
FT.SYNUPDATE <index name> <synonym group id> <term1> <term2> ...
```

### Description

Updates a synonym group.

The command is used to update an existing synonym group with additional terms. Only documents which was indexed after the update will be effected.

---

## FT.SYNDUMP

### Format

```
FT.SYNDUMP <index name>
```

### Description

Dumps the contents of a synonym group.

The command is used to dump the synonyms data structure. Returns a list of synonym terms and their synonym group ids.

---

## FT.SPELLCHECK 

### Format
```
  FT.SPELLCHECK {index} {query}
    [DISTANCE dist]
    [TERMS {INCLUDE | EXCLUDE} {dict} [TERMS ...]]
```

### Description

Performs spelling correction on a query, returning suggestions for misspelled terms.

See [Query Spelling Correction](Spellcheck.md) for more details.

### Parameters

* **index**: the index with the indexed terms.

* **query**: the search query.

* **TERMS**: specifies an inclusion (`INCLUDE`) or exclusion (`EXCLUDE`) custom dictionary named `{dict}`. Refer to [`FT.DICTADD`](Commands.md#ftdictadd), [`FT.DICTDEL`](Commands.md#ftdictdel) and [`FT.DICTDUMP`](Commands.md#ftdictdump) for managing custom dictionaries.

* **DISTANCE**: the maximal Levenshtein distance for spelling suggestions (default: 1, max: 4).

### Returns

An array, in which each element represents a misspelled term from the query. The misspelled terms are ordered by their order of appearance in the query.

Each misspelled term, in turn, is a 3-element array consisting of the constant string "TERM", the term itself and an array of suggestions for spelling corrections.

Each element in the spelling corrections array consists of the score of the suggestion and the suggestion itself. The suggestions array, per misspelled term, is ordered in descending order by score.

### Example output

```
1)  1) "TERM"
    2) "{term1}"
    3)  1)  1)  "{score1}"
            2)  "{suggestion1}"
        2)  1)  "{score2}"
            2)  "{suggestion2}"
        .
        .
        .
2)  1) "TERM"
    2) "{term2}"
    3)  1)  1)  "{score1}"
            2)  "{suggestion1}"
        2)  1)  "{score2}"
            2)  "{suggestion2}"
        .
        .
        .
.
.
.

```

---


## FT.DICTADD

### Format
```
  FT.DICTADD {dict} {term} [{term} ...]
```

### Description

Adds terms to a dictionary.

### Parameters

* **dict**: the dictionary name.

* **term**: the term to add to the dictionary.

### Returns

Returns int, specifically the number of new terms that were added.

---

## FT.DICTDEL

### Format
```
  FT.DICTDEL {dict} {term} [{term} ...]
```

### Description

Deletes terms from a dictionary.

### Parameters

* **dict**: the dictionary name.

* **term**: the term to delete from the dictionary.

### Returns

Returns int, specifically the number of terms that were deleted.

---

## FT.DICTDUMP

### Format
```
  FT.DICTDUMP {dict}
```

### Description

Dumps all terms in the given dictionary.

### Parameters

* **dict**: the dictionary name.

### Returns

Returns an array, where each element is term (string).

---

## FT.CONFIG

### Format
```
  FT.CONFIG <GET|HELP> {option}
  FT.CONFIG SET {option} {value}
```

### Description

Retrieves, describes and sets runtime configuration options.

### Parameters

* **option**: the name of the configuration option, or '*' for all.
* **value**: a value for the configuration option.

For details about the configuration options refer to [Configuring](Configuring.md).

Setting values in runtime is supported for these configuration options:

* `NOGC`
* `MINPREFIX`
* `MAXEXPANSIONS`
* `TIMEOUT`
* `ON_TIMEOUT`
* `MIN_PHONETIC_TERM_LEN`

### Returns

When provided with a valid option name, the `GET` subcommand returns a string with the current option's value. An array containing an array for each configuration option, consisting of the option's name and current value, is returned when '*' is provided.

The `SET` subcommand returns 'OK' for valid runtime-settable option names and values.
