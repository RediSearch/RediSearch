Creates an index with the given spec.

!!! warning "Note on attribute number limits"
    RediSearch supports up to 1024 attributes per schema, out of which at most 128 can be TEXT attributes.
    On 32 bit builds, at most 64 attributes can be TEXT attributes.
    Note that the more attributes you have, the larger your index will be, as each additional 8 attributes require one extra byte per index record to encode.
    You can always use the `NOFIELDS` option and not encode attribute information into the index, for saving space, if you do not need filtering by text attributes. This will still allow filtering by numeric and geo attributes.

!!! info "Note on running in clustered databases"
    When having several indices in a clustered database, you need to make sure the documents you want to index reside on the same shard as the index. You can achieve this by having your documents tagged by the index name.

    ```
    HSET doc:1{idx} ...
    FT.CREATE idx ... PREFIX 1 doc: ...
    ```

    When Running RediSearch in a clustered database, there is the ability to span the index across shards with [RSCoordinator](https://github.com/RedisLabsModules/RSCoordinator). In this case the above does not apply.

#### Parameters

* **index**: the index name to create. If it exists the old spec will be overwritten

* **ON {data_type}** currently supports HASH (default) and JSON.

!!! info "ON JSON"
    To index JSON, you must have the [RedisJSON](https://redisjson.io) module installed.

* **PREFIX {count} {prefix}** tells the index which keys it should index. You can add several prefixes to index. Since the argument is optional, the default is * (all keys)

* **FILTER {filter}** is a filter expression with the full RediSearch aggregation expression language. It is possible to use @__key to access the key that was just added/changed. A field can be used to set field name by passing `'FILTER @indexName=="myindexname"'`

* **LANGUAGE {default_lang}**: If set indicates the default language for documents in the index. Default to English.
* **LANGUAGE_FIELD {lang_attribute}**: If set indicates the document attribute that should be used as the document language.

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
    checks it against a predefined dictionary. See [Stemming](/docs/stack/search/reference/stemming) for more
    information.

* **SCORE {default_score}**: If set indicates the default score for documents in the index. Default score is 1.0.
* **SCORE_FIELD {score_attribute}**: If set indicates the document attribute that should be used as the document's rank based on the user's ranking.
  Ranking must be between 0.0 and 1.0. If not set the default score is 1.

* **PAYLOAD_FIELD {payload_attribute}**: If set indicates the document attribute that should be used as a binary safe payload string to the document,
  that can be evaluated at query time by a custom scoring function, or retrieved to the client.

* **MAXTEXTFIELDS**: For efficiency, RediSearch encodes indexes differently if they are
  created with less than 32 text attributes. This option forces RediSearch to encode indexes as if
  there were more than 32 text attributes, which allows you to add additional attributes (beyond 32)
  using `FT.ALTER`.

* **NOOFFSETS**: If set, we do not store term offsets for documents (saves memory, does not
  allow exact searches or highlighting). Implies `NOHL`.

* **TEMPORARY**: Create a lightweight temporary index which will expire after the specified period of inactivity. The internal idle timer is reset whenever the index is searched or added to. Because such indexes are lightweight, you can create thousands of such indexes without negative performance implications and therefore you should consider using `SKIPINITIALSCAN` to avoid costly scanning.

!!! warning "Note about deleting a temporary index"
    When dropped, a temporary index does not delete the hashes as they may have been indexed in several indexes. Adding the `DD` flag will delete the hashes as well.

* **NOHL**: Conserves storage space and memory by disabling highlighting support. If set, we do
  not store corresponding byte offsets for term positions. `NOHL` is also implied by `NOOFFSETS`.

* **NOFIELDS**: If set, we do not store attribute bits for each term. Saves memory, does not allow
  filtering by specific attributes.

* **NOFREQS**: If set, we avoid saving the term frequencies in the index. This saves
  memory but does not allow sorting based on the frequencies of a given term within
  the document.

* **STOPWORDS**: If set, we set the index with a custom stopword list, to be ignored during
  indexing and search time. {num} is the number of stopwords, followed by a list of stopword
  arguments exactly the length of {num}.

    If not set, we take the default list of stopwords.

    If **{num}** is set to 0, the index will not have stopwords.

* **SKIPINITIALSCAN**: If set, we do not scan and index.

* **SCHEMA {identifier} AS {attribute} {attribute type} {options...}**: After the SCHEMA keyword, we declare which fields to index:

    * **{identifier}**

      For hashes, the identifier is a field name within the hash.
      For JSON, the identifier is a JSON Path expression.

    * **AS {attribute}**

      This optional parameter defines the attribute associated to the identifier.
      For example, you can use this feature to alias a complex JSONPath expression with more memorable (and easier to type) name

    #### Field Types

    * **TEXT**

      Allows full-text search queries against the value in this attribute.

    * **TAG**

      Allows exact-match queries, such as categories or primary keys, against the value in this attribute. For more information, see [Tag Fields](/docs/stack/search/reference/tags).

    * **NUMERIC**

      Allows numeric range queries against the value in this attribute. See [query syntax docs](/docs/stack/search/reference/query_syntax) for details on how to use numeric ranges.

    * **GEO**

      Allows geographic range queries against the value in this attribute. The value of the attribute must be a string containing a longitude (first) and latitude separated by a comma.

    * **VECTOR**

      Allows vector similarity queries against the value in this attribute. For more information, see [Vector Fields](/docs/stack/search/reference/vectors).

    #### Field Options

    * **SORTABLE**

        Numeric, tag (not supported with JSON) or text attributes can have the optional **SORTABLE** argument. As the user [sorts the results by the value of this attribute](/docs/stack/search/reference/sorting), the results will be available with very low latency. (this adds memory overhead so consider not to declare it on large text attributes).

    * **UNF**
        
        By default, SORTABLE applies a normalization to the indexed value (characters set to lowercase, removal of diacritics). When using UNF (un-normalized form) it is possible to disable the normalization and keep the original form of the value. 
  
    * **NOSTEM**

        Text attributes can have the NOSTEM argument which will disable stemming when indexing its values.
        This may be ideal for things like proper names.

    * **NOINDEX**

        Attributes can have the `NOINDEX` option, which means they will not be indexed.
        This is useful in conjunction with `SORTABLE`, to create attributes whose update using PARTIAL will not cause full reindexing of the document. If an attribute has NOINDEX and doesn't have SORTABLE, it will just be ignored by the index.

    * **PHONETIC {matcher}**

        Declaring a text attribute as `PHONETIC` will perform phonetic matching on it in searches by default. The obligatory {matcher} argument specifies the phonetic algorithm and language used. The following matchers are supported:

        * `dm:en` - Double Metaphone for English
        * `dm:fr` - Double Metaphone for French
        * `dm:pt` - Double Metaphone for Portuguese
        * `dm:es` - Double Metaphone for Spanish

        For more details see [Phonetic Matching](/docs/stack/search/reference/phonetic_matching).

    * **WEIGHT {weight}**

        For `TEXT` attributes, declares the importance of this attribute when
        calculating result accuracy. This is a multiplication factor, and
        defaults to 1 if not specified.

    * **SEPARATOR {sep}**

        For `TAG` attributes, indicates how the text contained in the attribute
        is to be split into individual tags. The default is `,`. The value
        must be a single character.

    * **CASESENSITIVE**

        For `TAG` attributes, keeps the original letter cases of the tags.
        If not specified, the characters are converted to lowercase.

@return

@simple-string-reply - `OK` if executed correctly, or @error-reply otherwise.

@examples

Creating an index that stores the title, publication date, and categories of blog post hashes whose keys start with `blog:post:` (e.g., `blog:post:1`):

```
FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE
```

Indexing the "sku" attribute from a hash as both a TAG and as TEXT:

```
FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA sku AS sku_text TEXT sku AS sku_tag TAG SORTABLE
```

Indexing two different hashes -- one containing author data and one containing books -- in the same index:

```
FT.CREATE author-books-idx ON HASH PREFIX 2 author:details: book:details: SCHEMA
author_id TAG SORTABLE author_ids TAG title TEXT name TEXT
```

!!! note
    In this example, keys for author data use the key pattern `author:details:<id>` while keys for book data use the pattern `book:details:<id>`.

Indexing only authors whose names start with "G":

```
FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, "G")' SCHEMA name TEXT
```

Indexing only books that have a subtitle:

```
FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != ""' SCHEMA title TEXT
```

Indexing books that have a "categories" attribute where each category is separated by a `;` character:

```
FT.CREATE books-idx ON HASH PREFIX 1 book:details FILTER SCHEMA title TEXT categories TAG SEPARATOR ";"
```

Indexing a JSON document using a JSON Path expression:

```
FT.CREATE idx ON JSON SCHEMA $.title AS title TEXT $.categories AS categories TAG
```