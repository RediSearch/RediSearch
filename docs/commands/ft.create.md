---
syntax: |
  FT.CREATE index 
    [ON HASH | JSON] 
    [PREFIX count prefix [prefix ...]] 
    [FILTER {filter}]
    [LANGUAGE default_lang] 
    [LANGUAGE_FIELD lang_attribute] 
    [SCORE default_score] 
    [SCORE_FIELD score_attribute] 
    [PAYLOAD_FIELD payload_attribute] 
    [MAXTEXTFIELDS] 
    [TEMPORARY seconds] 
    [NOOFFSETS] 
    [NOHL] 
    [NOFIELDS] 
    [NOFREQS] 
    [STOPWORDS count [stopword ...]] 
    [SKIPINITIALSCAN]
    SCHEMA field_name [AS alias] TEXT | TAG | NUMERIC | GEO | VECTOR | GEOSHAPE [ SORTABLE [UNF]] 
    [NOINDEX] [ field_name [AS alias] TEXT | TAG | NUMERIC | GEO | VECTOR | GEOSHAPE [ SORTABLE [UNF]] [NOINDEX] ...]
---

## Description

Create an index with the given specification. For usage, see [Examples](#examples).

## Required arguments

<a name="index"></a><details open>
<summary><code>index</code></summary>

is index name to create.
If such index already exists, returns an error reply `(error) Index already exists`.
</details>

<a name="SCHEMA"></a><details open>
<summary><code>SCHEMA {identifier} AS {attribute} {attribute type} {options...</code></summary> 

after the SCHEMA keyword, declares which fields to index:

 - `{identifier}` for hashes, is a field name within the hash.
      For JSON, the identifier is a JSON Path expression.

 - `AS {attribute}` defines the attribute associated to the identifier. For example, you can use this feature to alias a complex JSONPath expression with more memorable (and easier to type) name.

 Field types are:

 - `TEXT` - Allows full-text search queries against the value in this attribute.

 - `TAG` - Allows exact-match queries, such as categories or primary keys, against the value in this attribute. For more information, see [Tag Fields](/docs/interact/search-and-query/advanced-concepts/tags/).

 - `NUMERIC` - Allows numeric range queries against the value in this attribute. See [query syntax docs](/docs/interact/search-and-query/query/) for details on how to use numeric ranges.

 - `GEO` - Allows radius range queries against the value (point) in this attribute. The value of the attribute must be a string containing a longitude (first) and latitude separated by a comma.

 - `VECTOR` - Allows vector similarity queries against the value in this attribute. For more information, see [Vector Fields](/docs/interact/search-and-query/search/vectors/).

 - `GEOSHAPE`- Allows polygon queries against the value in this attribute. The value of the attribute must follow a [WKT notation](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) list of 2D points representing the polygon edges `POLYGON((x1 y1, x2 y2, ...)` separated by a comma. A `GEOSHAPE` field type can be followed by one of the following coordinate systems:
   - `SPHERICAL` for Geographic longitude and latitude coordinates
   - `FLAT` for Cartesian X Y coordinates
  
    The default coordinate system is `SPHERICAL`.
    
    Currently `GEOSHAPE` doesn't support JSON multi-value and `SORTABLE` option.

 Field options are:

 - `SORTABLE` - `NUMERIC`, `TAG`, `TEXT`, or `GEO` attributes can have an optional **SORTABLE** argument. As the user [sorts the results by the value of this attribute](/docs/interact/search-and-query/advanced-concepts/sorting/), the results are available with very low latency. Note that his adds memory overhead, so consider not declaring it on large text attributes. You can sort an attribute without the `SORTABLE` option, but the latency is not as good as with `SORTABLE`.

 - `UNF` - By default, for hashes (not with JSON) `SORTABLE` applies a normalization to the indexed value (characters set to lowercase, removal of diacritics). When using the unnormalized form (UNF), you can disable the normalization and keep the original form of the value. With JSON, `UNF` is implicit with `SORTABLE` (normalization is disabled).

 - `NOSTEM` - Text attributes can have the NOSTEM argument that disables stemming when indexing its values. This may be ideal for things like proper names.

 - `NOINDEX` - Attributes can have the `NOINDEX` option, which means they will not be indexed. This is useful in conjunction with `SORTABLE`, to create attributes whose update using PARTIAL will not cause full reindexing of the document. If an attribute has NOINDEX and doesn't have SORTABLE, it will just be ignored by the index.

 - `PHONETIC {matcher}` - Declaring a text attribute as `PHONETIC` will perform phonetic matching on it in searches by default. The obligatory {matcher} argument specifies the phonetic algorithm and language used. The following matchers are supported:

   - `dm:en` - Double metaphone for English
   - `dm:fr` - Double metaphone for French
   - `dm:pt` - Double metaphone for Portuguese
   - `dm:es` - Double metaphone for Spanish

   For more information, see [Phonetic Matching](/docs/interact/search-and-query/advanced-concepts/phonetic_matching).

  - `WEIGHT {weight}` for `TEXT` attributes, declares the importance of this attribute when calculating result accuracy. This is a multiplication factor, and defaults to 1 if not specified.

  - `SEPARATOR {sep}` for `TAG` attributes, indicates how the text contained in the attribute is to be split into individual tags. The default is `,`. The value must be a single character.

  - `CASESENSITIVE` for `TAG` attributes, keeps the original letter cases of the tags. If not specified, the characters are converted to lowercase.

  - `WITHSUFFIXTRIE` for `TEXT` and `TAG` attributes, keeps a suffix trie with all terms which match the suffix. It is used to optimize `contains` (*foo*) and `suffix` (*foo) queries. Otherwise, a brute-force search on the trie is performed. If suffix trie exists for some fields, these queries will be disabled for other fields.
</details>

## Optional arguments

<a name="ON"></a><details open>
<summary><code>ON {data_type}</code></summary>

currently supports HASH (default) and JSON. To index JSON, you must have the [RedisJSON](/docs/stack/json) module installed.
</details>

<a name="PREFIX"></a><details open>
<summary><code>PREFIX {count} {prefix}</code></summary> 

tells the index which keys it should index. You can add several prefixes to index. Because the argument is optional, the default is `*` (all keys).
</details>

<a name="FILTER"></a><details open>
<summary><code>FILTER {filter}</code></summary> 

is a filter expression with the full RediSearch aggregation expression language. It is possible to use `@__key` to access the key that was just added/changed. A field can be used to set field name by passing `'FILTER @indexName=="myindexname"'`.
</details>

<a name="LANGUAGE"></a><details open>
<summary><code>LANGUAGE {default_lang}</code></summary> 

if set, indicates the default language for documents in the index. Default is English.
</details>

<a name="LANGUAGE_FIELD"></a><details open>
<summary><code>LANGUAGE_FIELD {lang_attribute}</code></summary> 

is document attribute set as the document language.

A stemmer is used for the supplied language during indexing. If an unsupported language is sent, the command returns an error. The supported languages are Arabic, Basque, Catalan, Danish, Dutch, English, Finnish, French, German, Greek, Hungarian,
Indonesian, Irish, Italian, Lithuanian, Nepali, Norwegian, Portuguese, Romanian, Russian,
Spanish, Swedish, Tamil, Turkish, and Chinese.

When adding Chinese language documents, set `LANGUAGE chinese` for the indexer to properly tokenize the terms. If you use the default language, then search terms are extracted based on punctuation characters and whitespace. The Chinese language tokenizer makes use of a segmentation algorithm (via [Friso](https://github.com/lionsoul2014/friso)), which segments text and checks it against a predefined dictionary. See [Stemming](/docs/interact/search-and-query/advanced-concepts/stemming) for more information.
</details>

<a name="SCORE"></a><details open>
<summary><code>SCORE {default_score}</code></summary> 

is default score for documents in the index. Default score is 1.0.
</details>

<a name="SCORE_FIELD"></a><details open>
<summary><code>SCORE_FIELD {score_attribute}</code></summary> 

is document attribute that you use as the document rank based on the user ranking. Ranking must be between 0.0 and 1.0. If not set, the default score is 1.
</details>

<a name="PAYLOAD_FIELD"></a><details open>
<summary><code>PAYLOAD_FIELD {payload_attribute}</code></summary> 

is document attribute that you use as a binary safe payload string to the document that can be evaluated at query time by a custom scoring function or retrieved to the client.
</details>

<a name="MAXTEXTFIELDS"></a><details open>
<summary><code>MAXTEXTFIELDS</code></summary> 

forces RediSearch to encode indexes as if there were more than 32 text attributes, which allows you to add additional attributes (beyond 32) using `FT.ALTER`. For efficiency, RediSearch encodes indexes differently if they are created with less than 32 text attributes.
</details>

<a name="NOOFFSETS"></a><details open>
<summary><code>NOOFFSETS</code></summary> 

does not store term offsets for documents. It saves memory, but does not allow exact searches or highlighting. It implies `NOHL`.
</details>

<a name="TEMPORARY"></a><details open>
<summary><code>TEMPORARY {seconds}</code></summary> 

creates a lightweight temporary index that expires after a specified period of inactivity, in seconds. The internal idle timer is reset whenever the index is searched or added to. Because such indexes are lightweight, you can create thousands of such indexes without negative performance implications and, therefore, you should consider using `SKIPINITIALSCAN` to avoid costly scanning.

{{% alert title="Warning" color="warning" %}}
 
When temporary indexes expire, they drop all the records associated with them.
`FT.DROPINDEX` was introduced with a default of not deleting docs and a `DD` flag that enforced deletion.
However, for temporary indexes, documents are deleted along with the index.
Historically, RediSearch used an FT.ADD command, which made a connection between the document and the index. Then, FT.DROP, also a hystoric command, deleted documents by default.
In version 2.x, RediSearch indexes hashes and JSONs, and the dependency between the index and documents no longer exists. 

{{% /alert %}}

</details>

<a name="NOHL"></a><details open>
<summary><code>NOHL</code></summary> 

conserves storage space and memory by disabling highlighting support. If set, the corresponding byte offsets for term positions are not stored. `NOHL` is also implied by `NOOFFSETS`.
</details>

<a name="NOFIELDS"></a><details open>
<summary><code>NOFIELDS</code></summary> 

does not store attribute bits for each term. It saves memory, but it does not allow
  filtering by specific attributes.
</details>

<a name="NOFREQS"></a><details open>
<summary><code>NOFREQS</code></summary> 

avoids saving the term frequencies in the index. It saves memory, but does not allow sorting based on the frequencies of a given term within the document.
</details>

<a name="STOPWORDS"></a><details open>
<summary><code>STOPWORDS {count}</code></summary> 

sets the index with a custom stopword list, to be ignored during indexing and search time. `{count}` is the number of stopwords, followed by a list of stopword arguments exactly the length of `{count}`.

If not set, FT.CREATE takes the default list of stopwords. If `{count}` is set to 0, the index does not have stopwords.
</details>

<a name="SKIPINITIALSCAN"></a><details open>
<summary><code>SKIPINITIALSCAN</code></summary> 

if set, does not scan and index.
</details>
        
<note><b>Notes:</b>

 - **Attribute number limits:** RediSearch supports up to 1024 attributes per schema, out of which at most 128 can be TEXT attributes. On 32 bit builds, at most 64 attributes can be TEXT attributes. The more attributes you have, the larger your index, as each additional 8 attributes require one extra byte per index record to encode. You can always use the `NOFIELDS` option and not encode attribute information into the index, for saving space, if you do not need filtering by text attributes. This will still allow filtering by numeric and geo attributes.
 - **Running in clustered databases:** When having several indices in a clustered database, you need to make sure the documents you want to index reside on the same shard as the index. You can achieve this by having your documents tagged by the index name.
    
   {{< highlight bash >}}
   127.0.0.1:6379> HSET doc:1{idx} ...
   127.0.0.1:6379> FT.CREATE idx ... PREFIX 1 doc: ...
   {{< / highlight >}}

   When Running RediSearch in a clustered database, you can span the index across shards using [RSCoordinator](https://github.com/RedisLabsModules/RSCoordinator). In this case the above does not apply.

</note>

## Return

FT.CREATE returns a simple string reply `OK` if executed correctly, or an error reply otherwise.

## Examples

<details open>
<summary><b>Create an index</b></summary>

Create an index that stores the title, publication date, and categories of blog post hashes whose keys start with `blog:post:` (for example, `blog:post:1`).

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE
OK
{{< / highlight >}}

Index the `sku` attribute from a hash as both a `TAG` and as `TEXT`:

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA sku AS sku_text TEXT sku AS sku_tag TAG SORTABLE
{{< / highlight >}}

Index two different hashes, one containing author data and one containing books, in the same index:

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE author-books-idx ON HASH PREFIX 2 author:details: book:details: SCHEMA
author_id TAG SORTABLE author_ids TAG title TEXT name TEXT
{{< / highlight >}}

In this example, keys for author data use the key pattern `author:details:<id>` while keys for book data use the pattern `book:details:<id>`.
</details>

<details open>
<summary><b>Index a JSON document using a JSON Path expression</b></summary>

Index authors whose names start with G.

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, "G")' SCHEMA name TEXT
{{< / highlight >}}

Index only books that have a subtitle.

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != ""' SCHEMA title TEXT
{{< / highlight >}}

Index books that have a "categories" attribute where each category is separated by a `;` character.

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE books-idx ON HASH PREFIX 1 book:details FILTER SCHEMA title TEXT categories TAG SEPARATOR ";"
{{< / highlight >}}

Index a JSON document using a JSON Path expression.

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE idx ON JSON SCHEMA $.title AS title TEXT $.categories AS categories TAG
{{< / highlight >}}
</details>

## See also

`FT.ALTER` | `FT.DROPINDEX` 

## Related topics

- [RediSearch](/docs/stack/search)
- [RedisJSON](/docs/stack/json)
- [Friso](https://github.com/lionsoul2014/friso)
- [Stemming](/docs/interact/search-and-query/advanced-concepts/stemming)
- [Phonetic Matching](/docs/interact/search-and-query/advanced-concepts/phonetic_matching/)
- [RSCoordinator](https://github.com/RedisLabsModules/RSCoordinator)
