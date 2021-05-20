# RediSearch Full Command Documentation

## Create

### FT.CREATE

#### Format
```
  FT.CREATE {index}
    [ON {structure}]
       [PREFIX {count} {prefix} [{prefix} ..]
       [FILTER {filter}]
       [LANGUAGE {default_lang}]
       [LANGUAGE_FIELD {lang_field}]
       [SCORE {default_score}]
       [SCORE_FIELD {score_field}]
       [PAYLOAD_FIELD {payload_field}]
    [MAXTEXTFIELDS] [TEMPORARY {seconds}] [NOOFFSETS] [NOHL] [NOFIELDS] [NOFREQS] [SKIPINITIALSCAN]
    [STOPWORDS {num} {stopword} ...]
    SCHEMA {field} [TEXT [NOSTEM] [WEIGHT {weight}] [PHONETIC {matcher}] | NUMERIC | GEO | TAG [SEPARATOR {sep}] ] [SORTABLE][NOINDEX] ...
```

#### Description
Creates an index with the given spec.

!!! warning "Note on field number limits"
    RediSearch supports up to 1024 fields per schema, out of which at most 128 can be TEXT fields.
    On 32 bit builds, at most 64 fields can be TEXT fields.
    Note that the more fields you have, the larger your index will be, as each additional 8 fields require one extra byte per index record to encode.
    You can always use the `NOFIELDS` option and not encode field information into the index, for saving space, if you do not need filtering by text fields. This will still allow filtering by numeric and geo fields.

!!! info "Note on running in clustered databases"
    When having several indices in a clustered database, you need to make sure the documents you want to index reside on the same shard as the index. You can achieve this by having your documents tagged by the index name.

    ```sql
    HSET doc:1{idx} ...
    FT.CREATE idx ... PREFIX 1 doc: ...
    ```

    When Running RediSearch in a clustered database, there is the ability to span the index across shards with [RSCoordinator](https://github.com/RedisLabsModules/RSCoordinator). In this case the above does not apply.

##### Examples

Creating an index that stores the title, publication date, and categories of blog post hashes whose keys start with `blog:post:` (e.g., `blog:post:1`):

```sql
FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE
```

Indexing two different hashes -- one containing author data and one containing books -- in the same index:

```sql
FT.CREATE author-books-idx ON HASH PREFIX 2 author:details: book:details: SCHEMA
author_id TAG SORTABLE author_ids TAG title TEXT name TEXT
```

!!! note
    In this example, keys for author data use the key pattern `author:details:<id>` while keys for book data use the pattern `book:details:<id>`.

Indexing only authors whose names start with "G":

```sql
FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, "G")' SCHEMA name TEXT
```

Indexing only books that have a subtitle:

```sql
FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != ""' SCHEMA title TEXT
```

Indexing books that have a "categories" field in which semicolons separate the values:

```sql
FT.CREATE books-idx ON HASH PREFIX 1 book:details FILTER SCHEMA title TEXT categories TAG SEPARATOR ";"
```

#### Parameters

* **index**: the index name to create. If it exists the old spec will be overwritten

* **ON {structure}** currently supports only HASH (default)

* **PREFIX {count} {prefix}** tells the index which keys it should index. You can add several prefixes to index. Since the argument is optional, the default is * (all keys)

* **FILTER {filter}** is a filter expression with the full RediSearch aggregation expression language. It is possible to use @__key to access the key that was just added/changed. A field can be used to set field name by passing `'FILTER @indexName=="myindexname"'`

* **LANGUAGE {default_lang}**: If set indicates the default language for documents in the index. Default to English.
* **LANGUAGE_FIELD {lang_field}**: If set indicates the document field that should be used as the document language.

!!! info "Supported languages"
    A stemmer is used for the supplied language during indexing.
    If an unsupported language is sent, the command returns an error.
    The supported languages are:

    Arabic, Basque, Catalan, Danish, Dutch, English, Finnish, French, German, Greek, Hungarian,
    Indonesian, Irish, Italian, Lithuanian, Nepali, Norwegian, Portuguese, Romanian, Russian,
    Spanish, Swedish, Tamil, Turkish, Chinese

    When adding Chinese-language documents, `LANGUAGE chinese` should be set in
    order for the indexer to properly tokenize the terms. If the default language
    is used then search terms will be extracted based on punctuation characters and
    whitespace. The Chinese language tokenizer makes use of a segmentation algorithm
    (via [Friso](https://github.com/lionsoul2014/friso)) which segments texts and
    checks it against a predefined dictionary. See [Stemming](Stemming.md) for more
    information.

* **SCORE {default_score}**: If set indicates the default score for documents in the index. Default score is 1.0.
* **SCORE_FIELD {score_field}**: If set indicates the document field that should be used as the document's rank based on the user's ranking.
  Ranking must be between 0.0 and 1.0. If not set the default score is 1.

* **PAYLOAD_FIELD {payload_field}**: If set indicates the document field that should be used as a binary safe payload string to the document,
  that can be evaluated at query time by a custom scoring function, or retrieved to the client.

* **MAXTEXTFIELDS**: For efficiency, RediSearch encodes indexes differently if they are
  created with less than 32 text fields. This option forces RediSearch to encode indexes as if
  there were more than 32 text fields, which allows you to add additional fields (beyond 32)
  using `FT.ALTER`.

* **NOOFFSETS**: If set, we do not store term offsets for documents (saves memory, does not
  allow exact searches or highlighting). Implies `NOHL`.

* **TEMPORARY**: Create a lightweight temporary index which will expire after the specified period of inactivity. The internal idle timer is reset whenever the index is searched or added to. Because such indexes are lightweight, you can create thousands of such indexes without negative performance implications and therefore you should consider using `SKIPINITIALSCAN` to avoid costly scanning.

!!! warning "Note about deleting a temporary index"
    When dropped, a temporary index does not delete the hashes as they may have been indexed in several indexes. Adding the `DD` flag will delete the hashes as well.

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

* **SKIPINITIALSCAN**: If set, we do not scan and index.

* **SCHEMA {field name} {field type} {options...}**: After the SCHEMA keyword we define the index fields. The field name is the name of the field within the hashes that this index follows. Field types can be numeric, textual or geographical.

    #### Field Types

    * **TEXT**

      Allows full-text search queries against the value in this field.

    * **TAG**

      Allows exact-match queries, such as categories or primary keys, against the value in this field. For more information, see [Tag Fields](Tags.md).

    * **NUMERIC**

      Allows numeric range queries against the value in this field. See [query syntax docs](Query_Syntax.md) for details on how to use numeric ranges.

    * **GEO**

      Allows geographic range queries against the value in this field. The value of the field must be a string containing a longitude (first) and latitude separated by a comma.

    #### Field Options

    * **SORTABLE**

        Numeric, tag or text fields can have the optional SORTABLE argument that allows the user to later [sort the results by the value of this field](Sorting.md) (this adds memory overhead so do not declare it on large text fields).

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



#### Complexity
O(1)

#### Returns
OK or an error

---

## Insert

### HSET/HSETNX/HDEL/HINCRBY/HDECRBY

#### Format

```
HSET {hash} {field} {value} [{field} {value} ...]
```

#### Description

Since RediSearch v2.0, native redis commands are used to add, update or delete hashes using [HSET](https://redis.io/commands/hset), [HINCRBY](https://redis.io/commands/hincrby), [HDEL](https://redis.io/commands/hdel) or other hash commands which alter the hash.

If a hash is modified, all matching indexes are updated automatically. Deletion of a hash by redis, whether by calling `DEL`, expiring a hash or evicting one, is handled automatically as well.

If a field fails to be indexed (for example, if a numeric fields gets a string value) the whole document is not indexed. `FT.INFO` provides the number of document-indexing-failures under `hash_indexing_failures`.

If `LANGUAGE_FIELD`, `SCORE_FIELD`, or `PAYLOAD_FIELD` were used with `FT.CREATE`, the document will extract the properties. A field can be used to get the name of the index it belongs to.

!!! warning "Schema mismatch"
    If a value in a hash does not match the schema type for that field, indexing of the hash will fail. The number of 'failed' document is under `hash_indexing_failures` at `FT.INFO`.

!!! info "Complete list of redis commands which might modify the index:"
    HSET, HMSET, HSETNX, HINCRBY, HINCRBYFLOAT, HDEL, DEL, SET, RENAME_FROM, RENAME_TO, TRIMMED, RESTORE, EXPIRED, EVICTED, CHANGE, LOADED

##### Example
```sql
HSET doc1 cs101 "hello world" number 3.141 geopoint "-122.064228,37.377658" tags foo,bar,baz
HSET doc2 cs201 "foo bar baz" number 2.718 geopoint "-0.084324,51.515583" tags foo,bar,baz
HSET doc3 Name "RedisLabs" indexName "myindexname"
```

!!! note
   The syntax for geographical values is a quoted string with longitude (first) and latitude separated by a comma.

---

## Search

### FT.SEARCH

#### Format

```
FT.SEARCH {index} {query} [NOCONTENT] [VERBATIM] [NOSTOPWORDS] [WITHSCORES] [WITHPAYLOADS] [WITHSORTKEYS]
  [FILTER {numeric_field} {min} {max}] ...
  [GEOFILTER {geo_field} {lon} {lat} {radius} m|km|mi|ft]
  [INKEYS {num} {key} ... ]
  [INFIELDS {num} {field} ... ]
  [RETURN {num} {field} ... ]
  [SUMMARIZE [FIELDS {num} {field} ... ] [FRAGS {num}] [LEN {fragsize}] [SEPARATOR {separator}]]
  [HIGHLIGHT [FIELDS {num} {field} ... ] [TAGS {open} {close}]]
  [SLOP {slop}] [INORDER]
  [LANGUAGE {language}]
  [EXPANDER {expander}]
  [SCORER {scorer}] [EXPLAINSCORE]
  [PAYLOAD {payload}]
  [SORTBY {field} [ASC|DESC]]
  [LIMIT offset num]
```

#### Description

Searches the index with a textual query, returning either documents or just ids.

#### Examples

Searching for the term "wizard" in every TEXT field of an index containing book data:

```sql
FT.SEARCH books-idx "wizard"
```
Searching for the term "dogs" in only the "title" field:

```sql
FT.SEARCH books-idx "@title:dogs"
```

Searching for books published in 2020 or 2021:

```sql
FT.SEARCH books-idx "@published_at:[2020 2021]
```

Searching for Chinese restaurants within 5 kilometers of longitude -122.41, latitude 37.77 (San Francisco):

```sql
FT.SEARCH restaurants-idx "chinese @location:[-122.41 37.77 5 km]"
```

Searching for the term "dogs" or "cats" in the "title" field, but giving matches of "dogs" a higher relevance score (also known as *boosting*):

```sql
FT.SEARCH books-idx "(@title:dogs | @title:cats) | (@title:dogs) => { $weight: 5.0; }"
```
Searching for books with "dogs" in any TEXT field in the index and requesting an explanation of scoring for each result:

```sql
FT.SEARCH books-idx "dogs" WITHSCORES EXPLAINSCORE
```

Searching for books with "space" in the title that have "science" in the TAG field "categories":

```sql
FT.SEARCH books-idx "@title:space @categories:{science}"
```

Searching for books with "Python" in any TEXT field, returning ten results starting with the eleventh result in the entire result set (the offset parameter is zero-based), and returning only the "title" field for each result:

```sql
FT.SEARCH books-idx "python" LIMIT 10 10 RETURN 1 title
```

!!! tip "More examples"
    For more details and query examples, see [query syntax](Query_Syntax.md).


#### Parameters

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
- **HIGHLIGHT ...**: Use this option to format occurrences of matched text. See [Highlighting](Highlight.md) for more
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
- **EXPLAINSCORE**: If set, will return a textual description of how the scores were calculated. Using this options requires the WITHSCORES option.
- **PAYLOAD {payload}**: Add an arbitrary, binary safe payload that will be exposed to custom scoring
  functions. [See Extensions](Extensions.md).

- **SORTBY {field} [ASC|DESC]**: If specified, the results
  are ordered by the value of this field. This applies to both text and numeric fields.
- **LIMIT first num**: Limit the results to
  the offset and number of results given. Note that the offset is zero-indexed. The default is 0 10, which returns 10 items starting from the first result.

!!! tip
    `LIMIT 0 0` can be used to count the number of documents in the result set without actually returning them.

#### Complexity

O(n) for single word queries. `n` is the number of the results in the result set. Finding all the documents that have a specific term is O(1), however, a scan on all those documents is needed to load the documents data from redis hashes and return them.

The time complexity for more complex queries varies, but in general it's proportional to the number of words, the number of intersection points between them and the number of results in the result set.

#### Returns

**Array reply,** where the first element is the total number of results, and then pairs of document id, and a nested array of field/value.

If **NOCONTENT** was given, we return an array where the first element is the total number of results, and the rest of the members are document ids.

!!! note "Expiration of hashes during a search query"
    If a hash expiry time is reached after the start of the query process, the hash will be counted in the total number of results but name and content of the hash will not be returned.

---

### FT.AGGREGATE

#### Format

```
FT.AGGREGATE {index_name}
  {query_string}
  [VERBATIM]
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

#### Description

Runs a search query on an index, and performs aggregate transformations on the results, extracting statistics etc from them. See [the full documentation on aggregations](Aggregations.md) for further details.

#### Examples

Finding visits to the page "about.html", grouping them by the day of the visit, counting the number of visits, and sorting them by day:

```sql
FT.AGGREGATE idx "@url:\"about.html\""
    APPLY "day(@timestamp)" AS day
    GROUPBY 2 @day @country
      REDUCE count 0 AS num_visits
    SORTBY 4 @day
```

Finding the most books ever published in a single year:

```sql
FT.AGGREGATE books-idx *
    GROUPBY 1 @published_year
      REDUCE COUNT 0 AS num_published
    GROUPBY 0
      REDUCE MAX 1 @num_published AS max_books_published_per_year
```

!!! tip "Reducing all results"
    The last example used `GROUPBY 0`. Use `GROUPBY 0` to apply a `REDUCE` function over all results from the last step of an aggregation pipeline -- this works on both the  initial query and subsequent `GROUPBY` operations.

Searching for libraries within 10 kilometers of the longitude -73.982254 and latitude 40.753181 then annotating them with the distance between their location and those coordinates:

```sql
 FT.AGGREGATE libraries-idx "@location:[-73.982254 40.753181 10 km]"
    LOAD 1 @location
    APPLY "geodistance(@location, -73.982254, 40.753181)"
```

Here, we needed to use `LOAD` to pre-load the @location field because it is a GEO field.

!!! tip "More examples"
    For more details on aggreations and detailed examples of aggregation queries, see [Aggregations](Aggregations.md).


#### Parameters

* **index_name**: The index the query is executed against.

* **query_string**: The base filtering query that retrieves the documents. It follows
  **the exact same syntax** as the search query, including filters, unions, not, optional, etc.

* **LOAD {nargs} {property} …**: Load document fields from the document HASH objects. This should be
  avoided as a general rule of thumb. Fields needed for aggregations should be stored as **SORTABLE**,
  where they are available to the aggregation pipeline with very low latency. LOAD hurts the
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

#### Complexity

Non-deterministic. Depends on the query and aggregations performed, but it is usually linear to the number of results returned.

#### Returns

Array Response. Each row is an array and represents a single aggregate result.

#### Example output

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
[10](10)) 1) "actor"
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

### FT.EXPLAIN

#### Format

```
FT.EXPLAIN {index} {query}
```

#### Description

Returns the execution plan for a complex query.

In the returned response, a `+` on a term is an indication of stemming.

#### Example
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

#### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **query**: The query string, as if sent to FT.SEARCH

#### Complexity

O(1)

#### Returns

String Response. A string representing the execution plan (see above example).

!!! tip
    You should use `redis-cli --raw` to properly read line-breaks in the returned response.

---

### FT.EXPLAINCLI

#### Format

```
FT.EXPLAINCLI {index} {query}
```

#### Description

Returns the execution plan for a complex query but formatted for easier reading without using `redis-cli --raw`.

In the returned response, a `+` on a term is an indication of stemming.

#### Example
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

#### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **query**: The query string, as if sent to FT.SEARCH

#### Complexity

O(1)

#### Returns

String Response. A string representing the execution plan (see above example).

---

### FT.PROFILE

#### Format

```
FT.PROFILE {index} {[SEARCH, AGGREGATE]} [LIMITED] QUERY {query}
```

#### Description

Performs a `FT.SEARCH` or `FT.AGGREGATE` command and collects performance information.
Return value has an array with two elements:

  * **Results** - The normal reply from RediSearch, similar to a cursor.
  * **Profile** - The details in the profile are:
    * **Total profile time** - The total runtime of the query.
    * **Parsing time** - Parsing time of the query and parameters into an execution plan.
    * **Pipeline creation time** - Creation time of execution plan including iterators,
  result processors and reducers creation.
    * **Iterators profile** - Index iterators information including their type, term, count and time data.
  Inverted-index iterators have in addition the number of elements they contain.
    * **Result processors profile** - Result processors chain with type, count and time data.

#### Example
```sh
FT.PROFILE idx SEARCH QUERY "hello world"
1) 1) (integer) 1
   2) "doc1"
   3) 1) "t"
      2) "hello world"
2) 1) 1) Total profile time
      2) "0.47199999999999998"
   2) 1) Parsing time
      2) "0.218"
   3) 1) Pipeline creation time
      2) "0.032000000000000001"
   4) 1) Iterators profile
      2) 1) Type
         2) INTERSECT
         3) Time
         4) "0.025000000000000001"
         5) Counter
         6) (integer) 1
         7) Children iterators
         8)  1) Type
             2) TEXT
             3) Term
             4) hello
             5) Time
             6) "0.0070000000000000001"
             7) Counter
             8) (integer) 1
             9) Size
            10) (integer) 1
         9)  1) Type
             2) TEXT
             3) Term
             4) world
             5) Time
             6) "0.0030000000000000001"
             7) Counter
             8) (integer) 1
             9) Size
            10) (integer) 1
   5) 1) Result processors profile
      2) 1) Type
         2) Index
         3) Time
         4) "0.036999999999999998"
         5) Counter
         6) (integer) 1
      3) 1) Type
         2) Scorer
         3) Time
         4) "0.025000000000000001"
         5) Counter
         6) (integer) 1
      4) 1) Type
         2) Sorter
         3) Time
         4) "0.013999999999999999"
         5) Counter
         6) (integer) 1
      5) 1) Type
         2) Loader
         3) Time
         4) "0.10299999999999999"
         5) Counter
         6) (integer) 1
```

#### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **SEARCH,AGGREGATE**: Differ between `FT.SEARCH` and `FT.AGGREGATE`
- **LIMITED**: Removes details of `reader` iterator
- **QUERY {query}**: The query string, as if sent to FT.SEARCH

#### Complexity

Non-deterministic. Depends on the query and aggregations performed, but it is usually linear to the number of results returned.
#### Returns

Array Response.

!!! tip
    To reduce the size of the output, use `NOCONTENT` or `LIMIT 0 0` to reduce results reply or `LIMITED` to not reply with details of `reader iterators` inside builtin-unions such as `fuzzy` or `prefix`.

---

## Update

### FT.ALTER SCHEMA ADD

#### Format

```
FT.ALTER {index} SCHEMA ADD {field} {options} ...
```

#### Description

Adds a new field to the index.

Adding a field to the index will cause any future document updates to use the new field when
indexing and reindexing of existing documents.

!!! note
    Depending on how the index was created, you may be limited by the amount of additional text
    fields which can be added to an existing index. If the current index contains less than 32
    text fields, then `SCHEMA ADD` will only be able to add fields up to 32 total fields (meaning that the
    index will only ever be able to contain 32 total text fields). If you wish for the index to
    contain more than 32 fields, create it with the `MAXTEXTFIELDS` option.

##### Example
```sql
FT.ALTER idx SCHEMA ADD id2 NUMERIC SORTABLE
```

#### Parameters

* **index**: the index name.
* **field**: the field name.
* **options**: the field options - refer to `FT.CREATE` for more information.

#### Complexity

O(1)

#### Returns

OK or an error.

---

## Delete

### FT.DROPINDEX

#### Format

```
FT.DROPINDEX {index} [DD]
```

#### Description

Deletes the index.

By default, FT.DROPINDEX does not delete the document hashes associated with the index. Adding the DD option deletes the hashes as well.

Since RediSearch 2.0

#### Example
```sql
FT.DROPINDEX idx DD
```

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **DD**: If set, the drop operation will delete the actual document hashes.

#### Returns

Status Reply: OK on success.

!!! note
     When using FT.DROPINDEX with the parameter DD, if an index creation is still running (FT.CREATE is running asynchronously),
     only the document hashes that have already been indexed are deleted. The document hashes left to be indexed will remain in the database.
     You can use FT.INFO to check the completion of the indexing.

---

## Alias

### FT.ALIASADD
### FT.ALIASUPDATE
### FT.ALIASDEL

#### Format

```
FT.ALIASADD {name} {index}
FT.ALIASUPDATE {name} {index}
FT.ALIASDEL {name}
```

The `FT.ALIASADD` and `FT.ALIASDEL` commands will add or remove an alias from
an index. Index aliases can be used to refer to actual indexes in data
commands such as `FT.SEARCH` or `FT.ADD`. This allows an administrator
to transparently redirect application queries to alternative indexes.

Indexes can have more than one alias, though an alias cannot refer to another
alias.

The `FT.ALIASUPDATE` command differs from the `FT.ALIASADD` command in that
it will remove the alias association with a previous index, if any. `FT.ALIASADD`
will fail, on the other hand, if the alias is already associated with another
index.

#### Complexity

O(1)

#### Returns

OK or an error.

---

## Tags

### FT.TAGVALS

#### Format

```
FT.TAGVALS {index} {field_name}
```

#### Description

Returns the distinct tags indexed in a [Tag field](Tags.md).

This is useful if your tag field indexes things like cities, categories, etc.

!!! warning "Limitations"
    There is no paging or sorting, the tags are not alphabetically sorted.
    This command only operates on [Tag fields](Tags.md).
    The strings return lower-cased and stripped of whitespaces, but otherwise unchanged.

#### Example
```sql
FT.TAGVALS idx myTag
```

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **filed_name**: The name of a Tag file defined in the schema.

#### Returns

Array Reply: All the distinct tags in the tag index.

#### Complexity

O(n), n being the cardinality of the tag field.

---

## Suggestions

### FT.SUGADD

#### Format

```
FT.SUGADD {key} {string} {score} [INCR] [PAYLOAD {payload}]
```

#### Description

Adds a suggestion string to an auto-complete suggestion dictionary. This is disconnected from the
index definitions, and leaves creating and updating suggestions dictionaries to the user.

#### Example
```sql
FT.SUGADD ac "hello world" 1
```

#### Parameters

- **key**: the suggestion dictionary key.
- **string**: the suggestion string we index
- **score**: a floating point number of the suggestion string's weight
- **INCR**: if set, we increment the existing entry of the suggestion by the given score, instead of
  replacing the score. This is useful for updating the dictionary based on user queries in real time
- **PAYLOAD {payload}**: If set, we save an extra payload with the suggestion, that can be fetched by
  adding the `WITHPAYLOADS` argument to `FT.SUGGET`.

#### Returns

Integer Reply: the current size of the suggestion dictionary.

---

### FT.SUGGET

#### Format

```
FT.SUGGET {key} {prefix} [FUZZY] [WITHSCORES] [WITHPAYLOADS] [MAX num]
```

#### Description

Gets completion suggestions for a prefix.

#### Example
```sql
FT.SUGGET ac hell FUZZY MAX 3 WITHSCORES
```

#### Parameters

- **key**: the suggestion dictionary key.
- **prefix**: the prefix to complete on
- **FUZZY**: if set, we do a fuzzy prefix search, including prefixes at Levenshtein distance of 1 from
  the prefix sent
- **MAX num**: If set, we limit the results to a maximum of `num` (default: 5).
- **WITHSCORES**: If set, we also return the score of each suggestion. this can be used to merge
  results from multiple instances
- **WITHPAYLOADS**: If set, we return optional payloads saved along with the suggestions. If no
  payload is present for an entry, we return a Null Reply.

#### Returns

Array Reply: a list of the top suggestions matching the prefix, optionally with score after each entry

---

### FT.SUGDEL

#### Format

```
FT.SUGDEL {key} {string}
```

#### Description

Deletes a string from a suggestion index.

#### Example
```sql
FT.SUGDEL ac "hello world"
```

#### Parameters

- **key**: the suggestion dictionary key.
- **string**: the string to delete

#### Returns

Integer Reply: 1 if the string was found and deleted, 0 otherwise.

---

### FT.SUGLEN

#### Format

```
FT.SUGLEN {key}
```

#### Description

Gets the size of an auto-complete suggestion dictionary

#### Example
```sql
FT.SUGLEN ac
```

#### Parameters

* **key**: the suggestion dictionary key.

#### Returns

Integer Reply: the current size of the suggestion dictionary.

---

## Synonym

### FT.SYNUPDATE

#### Format

```
FT.SYNUPDATE <index name> <synonym group id> [SKIPINITIALSCAN] <term1> <term2> ...
```

#### Description

Updates a synonym group.

The command is used to create or update a synonym group with additional terms. Only documents which were indexed after the update will be affected.

#### Parameters

* **SKIPINITIALSCAN**: If set, we do not scan and index.

---

### FT.SYNDUMP

#### Format

```
FT.SYNDUMP <index name>
```

#### Description

Dumps the contents of a synonym group.

The command is used to dump the synonyms data structure. Returns a list of synonym terms and their synonym group ids.

---

### FT.SPELLCHECK

#### Format
```
  FT.SPELLCHECK {index} {query}
    [DISTANCE dist]
    [TERMS {INCLUDE | EXCLUDE} {dict} [TERMS ...]]
```

#### Description

Performs spelling correction on a query, returning suggestions for misspelled terms.

See [Query Spelling Correction](Spellcheck.md) for more details.

#### Parameters

* **index**: the index with the indexed terms.

* **query**: the search query.

* **TERMS**: specifies an inclusion (`INCLUDE`) or exclusion (`EXCLUDE`) custom dictionary named `{dict}`. Refer to [`FT.DICTADD`](Commands.md#ftdictadd), [`FT.DICTDEL`](Commands.md#ftdictdel) and [`FT.DICTDUMP`](Commands.md#ftdictdump) for managing custom dictionaries.

* **DISTANCE**: the maximal Levenshtein distance for spelling suggestions (default: 1, max: 4).

#### Returns

An array, in which each element represents a misspelled term from the query. The misspelled terms are ordered by their order of appearance in the query.

Each misspelled term, in turn, is a 3-element array consisting of the constant string "TERM", the term itself and an array of suggestions for spelling corrections.

Each element in the spelling corrections array consists of the score of the suggestion and the suggestion itself. The suggestions array, per misspelled term, is ordered in descending order by score.

The score is calculated by dividing the number of documents in which the suggested term exists, by the total number of documents in the index. Results can be normalized by dividing scores by the highest score.

#### Example output

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

## Dictionary

### FT.DICTADD

#### Format
```
  FT.DICTADD {dict} {term} [{term} ...]
```

#### Description

Adds terms to a dictionary.

#### Parameters

* **dict**: the dictionary name.

* **term**: the term to add to the dictionary.

#### Returns

Returns int, specifically the number of new terms that were added.

---

### FT.DICTDEL

#### Format
```
  FT.DICTDEL {dict} {term} [{term} ...]
```

#### Description

Deletes terms from a dictionary.

#### Parameters

* **dict**: the dictionary name.

* **term**: the term to delete from the dictionary.

#### Returns

Returns int, specifically the number of terms that were deleted.

---

### FT.DICTDUMP

#### Format
```
  FT.DICTDUMP {dict}
```

#### Description

Dumps all terms in the given dictionary.

#### Parameters

* **dict**: the dictionary name.

#### Returns

Returns an array, where each element is term (string).

---

## Info

### FT.INFO

#### Format
```
FT.INFO {index}
```

#### Description

Returns information and statistics on the index. Returned values include:

* `index_definition`: reflection of `FT.CREATE` command parameters.
* `fields`: index schema - field names, types, and attributes.
* Number of documents.
* Number of distinct terms.
* Average bytes per record.
* Size and capacity of the index buffers.
* Indexing state and percentage as well as failures:
  * `indexing`: whether of not the index is being scanned in the background,
  * `percent_indexed`: progress of background indexing (1 if complete),
  * `hash_indexing_failures`: number of failures due to operations not compatible with index schema.

Optional

* Statistics about the `garbage collector` for all options other than NOGC.
* Statistics about `cursors` if a cursor exists for the index.
* Statistics about `stopword lists` if a custom stopword list is used.


##### Example
```bash
127.0.0.1:6379> ft.info wik{0}
1) index_name
 2) wikipedia
 3) index_options
 4) (empty array)
 5) index_definition
 6)  1) key_type
     2) HASH
     3) prefixes
     4) 1) thing:
     5) filter
     6) startswith(@__key, "thing:")
     7) language_field
     8) __language
     9) default_score
    10) "1"
    11) score_field
    12) __score
    13) payload_field
    14) __payload
 7) fields
 8) 1) 1) title
       2) type
       3) TEXT
       4) WEIGHT
       5) "1"
       6) SORTABLE
    2) 1) body
       2) type
       3) TEXT
       4) WEIGHT
       5) "1"
    3) 1) id
       2) type
       3) NUMERIC
    4) 1) subject location
       2) type
       3) GEO
 9) num_docs
10) "0"
11) max_doc_id
12) "345678"
13) num_terms
14) "691356"
15) num_records
16) "0"
17) inverted_sz_mb
18) "0"
19) total_inverted_index_blocks
20) "933290"
21) offset_vectors_sz_mb
22) "0.65932846069335938"
23) doc_table_size_mb
24) "29.893482208251953"
25) sortable_values_size_mb
26) "11.432285308837891"
27) key_table_size_mb
28) "1.239776611328125e-05"
29) records_per_doc_avg
30) "-nan"
31) bytes_per_record_avg
32) "-nan"
33) offsets_per_term_avg
34) "inf"
35) offset_bits_per_record_avg
36) "8"
37) hash_indexing_failures
38) "0"
39) indexing
40) "0"
41) percent_indexed
42) "1"
43) gc_stats
44)  1) bytes_collected
     2) "4148136"
     3) total_ms_run
     4) "14796"
     5) total_cycles
     6) "1"
     7) average_cycle_time_ms
     8) "14796"
     9) last_run_time_ms
    10) "14796"
    11) gc_numeric_trees_missed
    12) "0"
    13) gc_blocks_denied
    14) "0"
45) cursor_stats
46) 1) global_idle
    2) (integer) 0
    3) global_total
    4) (integer) 0
    5) index_capacity
    6) (integer) 128
    7) index_total
    8) (integer) 0
47) stopwords_list
48) 1) "tlv"
    2) "summer"
    3) "2020"
```

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

#### Complexity

O(1)

#### Returns

Array Response. A nested array of keys and values.

---

### FT._LIST

#### Format
```
  FT._LIST
```

#### Description

Returns a list of all existing indexes.

##### Example
```sql
FT._LIST
1) "idx"
2) "movies"
3) "imdb"
```

#### Complexity

O(n) where `n` is the number of indexes in the system.

#### Returns

An array with index names.

!!! note "Temporary command"
    The prefix `_` in the command indicates, this is a temporary command.

    In the future, a `SCAN` type of command will be added, for use when a database
    contains a large number of indices.

---

## Configuration

### FT.CONFIG

#### Format
```
  FT.CONFIG <GET|HELP> {option}
  FT.CONFIG SET {option} {value}
```

#### Description

Retrieves, describes and sets runtime configuration options.

#### Parameters

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

#### Returns

When provided with a valid option name, the `GET` subcommand returns a string with the current option's value. An array containing an array for each configuration option, consisting of the option's name and current value, is returned when '*' is provided.

The `SET` subcommand returns 'OK' for valid runtime-settable option names and values.

---

## Deprecated commands

### FT.ADD

#### Format

```
FT.ADD {index} {docId} {score}
  [REPLACE [PARTIAL] [NOCREATE]]
  [LANGUAGE {language}]
  [PAYLOAD {payload}]
  [IF {condition}]
  FIELDS {field} {value} [{field} {value}...]
```

#### Description

!!! warning "Deprecation warning"
    This command is deprecated and act as simple redis HSET, the document created will be indexed only if it matches one or some indexes definitions (as defined on [ft.create](Commands.md#ftcreate)), Use HSET instead.

Adds a document to the index.

##### Example
```sql
FT.ADD idx doc1 1.0 FIELDS title "hello world"
```

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

- **docId**: The document's id that will be returned from searches.

!!! note "Notes on docId"
    The same docId cannot be added twice to the same index.
    The same docId can be added to multiple indices, but a single document with that docId is saved in the database.

- **score**: The document's rank based on the user's ranking. This must be between 0.0 and 1.0.
  On v2.0 this will be translated to a '__score' field in the created hash.

- **REPLACE**: If set, we will do an UPSERT style insertion - and delete an older version of the
  document if it exists.

- **PARTIAL** (only applicable with REPLACE): If set, you do not have to specify all fields for
  reindexing. Fields not given to the command will be loaded from the current version of the
  document. Also, if only non-indexable fields, score or payload are set - we do not do a full
  re-indexing of the document, and this will be a lot faster.

- **NOCREATE** (only applicable with REPLACE): If set, the document is only updated
  and reindexed if it already exists. If the document does not exist, an error
  will be returned.

- **FIELDS**: Following the FIELDS specifier, we are looking for pairs of `{field} {value}` to be
  indexed. Each field will be scored based on the index spec given in `FT.CREATE`.
  Passing fields that are not in the index spec will make them be stored as part of the document,
  or ignored if NOSAVE is set

- **PAYLOAD {payload}**: Optionally set a binary safe payload string to the document,
  that can be evaluated at query time by a custom scoring function, or retrieved to the client.
  On v2.0 this will be translated to a '__payload' field in the created hash.

- **IF {condition}**: (Applicable only in conjunction with `REPLACE` and optionally `PARTIAL`).
  Update the document only if a boolean expression applies to the document **before the update**,
  e.g. `FT.ADD idx doc 1 REPLACE IF "@timestamp < 23323234234"`.

  The expression is evaluated atomically before the update, ensuring that the update will happen only if it is true.

  See [Aggregations](Aggregations.md) for more details on the expression language.

- **LANGUAGE language**: If set, we use a stemmer for the supplied language during indexing. Default
  to English.
  If an unsupported language is sent, the command returns an error.
  The supported languages are:

    Arabic, Basque, Catalan, Danish, Dutch, English, Finnish, French, German, Greek, Hungarian,
    Indonesian, Irish, Italian, Lithuanian, Nepali, Norwegian, Portuguese, Romanian, Russian,
    Spanish, Swedish, Tamil, Turkish, Chinese

  If indexing a Chinese language document, you must set the language to `chinese`
  in order for Chinese characters to be tokenized properly.
  On v2.0 this will be translated to a '__language' field in the created hash.

#### Adding Chinese Documents

When adding Chinese-language documents, `LANGUAGE chinese` should be set in
order for the indexer to properly tokenize the terms. If the default language
is used then search terms will be extracted based on punctuation characters and
whitespace. The Chinese language tokenizer makes use of a segmentation algorithm
(via [Friso](https://github.com/lionsoul2014/friso)) which segments texts and
checks it against a predefined dictionary. See [Stemming](Stemming.md) for more
information.

#### Complexity

O(n), where n is the number of tokens in the document

#### Returns

OK on success, or an error if something went wrong.

A special status `NOADD` is returned if an `IF` condition evaluated to false.

!!! warning "FT.ADD with REPLACE and PARTIAL"
    By default, FT.ADD does not allow updating the document, and will fail if it already exists in the index.
    However, updating the document is possible with the REPLACE and REPLACE PARTIAL options.
    **REPLACE**: On its own, sets the document to the new values, and reindexes it. Any fields not given will not be loaded from the current version of the document.
    **REPLACE PARTIAL**: When both arguments are used, we can update just part of the document fields, and the rest will be loaded before reindexing. Not only that, but if only the score, payload and non-indexed fields (using NOINDEX) are updated, we will not actually reindex the document, just update its metadata internally, which is a lot faster and does not create index garbage.

---

!!! warning "Overwriting other keys"
    FT.ADD will actually create a hash in Redis with the given fields and value. This means that if the hash already exists, it will override with the new values.

---

### FT.DEL

#### Format

```
FT.DEL {index} {doc_id} [DD]
```

#### Description

!!! warning "Deprecation warning"
    This command is deprecated and acts as a simple redis DEL, the deleted document will be deleted from all the indexes it indexed on", Use DEL instead.

Deletes a document from the index. Returns 1 if the document was in the index, or 0 if not.

!!! warning "since v2.0, the [DD] option is not longer support, deleting a document means to also delete the hash from redis"
!!! warning "since v2.0, deleting a document from one index will cause this document to be deleted from all the indexes contains it"

#### Example
```sql
FT.DEL idx doc1
```

#### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **doc_id**: the id of the document to be deleted. It does not actually delete the HASH key in which
  the document is stored. Use DEL to do that manually if needed.


#### Complexity

O(1)

#### Returns

Integer Reply: 1 if the document was deleted, 0 if not.

---

### FT.DROP

#### Format

```
FT.DROP {index} [KEEPDOCS]
```

#### Description

!!! warning "Deprecation warning"
    This command is deprecated, use FT.DROPINDEX instead.

Deletes the index and all the keys associated with it.

By default, DROP deletes the document hashes as well, but adding the KEEPDOCS option keeps the documents in place, ready for re-indexing.

If no other data is on the Redis instance, this is equivalent to FLUSHDB, apart from the fact
that the index specification is not deleted.

#### Example
```sql
FT.DROP idx KEEPDOCS
```

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **KEEPDOCS**: If set, the drop operation will not delete the actual document hashes.

#### Returns

Status Reply: OK on success.

---

### FT.GET

#### Format

```
FT.GET {index} {doc id}
```

#### Description

!!! warning "Deprecation warning"
    This command is deprecated. Use HGETALL instead.

Returns content of a document as inserted without attribute fields (score/language/payload).

If the document does not exist or is not a HASH object, we return a NULL reply

#### Example
```sql
FT.GET idx doc1
```

#### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **documentId**: The id of the document as inserted to the index

#### Returns

Array Reply: Key-value pairs of field names and values of the document

---

### FT.MGET

#### Format

```
FT.MGET {index} {docId} ...
```

#### Description

!!! warning "Deprecation warning"
    This command is deprecated. Use HGETALL instead.

Returns content of a document as inserted without attribute fields (score/language/payload).

In addition, it allows simpler implementation of fetching documents in clustered mode.

We return an array with exactly the same number of elements as the number of keys sent to the command.

Each element, in turn, is an array of key-value pairs representing the document.

If a document is not found or is not a valid HASH object, its place in the parent array is filled with a Null reply object.

#### Example
```sql
FT.MGET idx doc1 doc2
```

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **documentIds**: The ids of the requested documents as inserted to the index

#### Returns

Array Reply: An array with exactly the same number of elements as the number of keys sent to the command. Each element in it is either an array representing the document or Null if it was not found.

---

### FT.SYNADD

!!! warning "Deprecation warning"
    This command is not longer supported on versions 2.0 and above, use FT.SYNUPDATE directly.

#### Format

```
FT.SYNADD <index name> <term1> <term2> ...
```

#### Description

Adds a synonym group.

The command is used to create a new synonyms group. The command returns the synonym group id which can later be used to add additional terms to that synonym group. Only documents which were indexed after the adding operation will be affected.

---
