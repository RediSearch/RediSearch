
---
syntax: |
  FT.SEARCH index query 
    [NOCONTENT] 
    [VERBATIM] [NOSTOPWORDS] 
    [WITHSCORES] 
    [WITHPAYLOADS] 
    [WITHSORTKEYS] 
    [FILTER numeric_field min max [ FILTER numeric_field min max ...]] 
    [GEOFILTER geo_field lon lat radius m | km | mi | ft [ GEOFILTER geo_field lon lat radius m | km | mi | ft ...]] 
    [INKEYS count key [key ...]] [ INFIELDS count field [field ...]] 
    [RETURN count identifier [AS property] [ identifier [AS property] ...]] 
    [SUMMARIZE [ FIELDS count field [field ...]] [FRAGS num] [LEN fragsize] [SEPARATOR separator]] 
    [HIGHLIGHT [ FIELDS count field [field ...]] [ TAGS open close]] 
    [SLOP slop] 
    [TIMEOUT timeout] 
    [INORDER] 
    [LANGUAGE language] 
    [EXPANDER expander] 
    [SCORER scorer] 
    [EXPLAINSCORE] 
    [PAYLOAD payload] 
    [SORTBY sortby [ ASC | DESC] [WITHCOUNT]] 
    [LIMIT offset num] 
    [PARAMS nargs name value [ name value ...]] 
    [DIALECT dialect]
---

Search the index with a textual query, returning either documents or just ids

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is index name. You must first create the index using `FT.CREATE`.
</details>

<details open>
<summary><code>query</code></summary> 

is text query to search. If it's more than a single word, put it in quotes. Refer to [Query syntax](/docs/interact/search-and-query/query/) for more details.
</details>

## Optional arguments

<details open>
<summary><code>NOCONTENT</code></summary>

returns the document ids and not the content. This is useful if RediSearch is only an index on an external document collection.
</details>

<details open>
<summary><code>VERBATIM</code></summary>

does not try to use stemming for query expansion but searches the query terms verbatim.
</details>

<details open>
<summary><code>WITHSCORES</code></summary>

also returns the relative internal score of each document. This can be used to merge results from multiple instances.
</details>

<details open>
<summary><code>WITHPAYLOADS</code></summary>

retrieves optional document payloads. See `FT.CREATE`. The payloads follow the document id and, if `WITHSCORES` is set, the scores.
</details>

<details open>
<summary><code>WITHSORTKEYS</code></summary>

returns the value of the sorting key, right after the id and score and/or payload, if requested. This is usually not needed, and exists for distributed search coordination purposes. This option is relevant only if used in conjunction with `SORTBY`.
</details>

<details open>
<summary><code>FILTER numeric_attribute min max</code></summary>

limits results to those having numeric values ranging between `min` and `max`, if numeric_attribute is defined as a numeric attribute in `FT.CREATE`. 
  `min` and `max` follow `ZRANGE` syntax, and can be `-inf`, `+inf`, and use `(` for exclusive ranges. Multiple numeric filters for different attributes are supported in one query.
</details>

<details open>
<summary><code>GEOFILTER {geo_attribute} {lon} {lat} {radius} m|km|mi|ft</code></summary>

filter the results to a given `radius` from `lon` and `lat`. Radius is given as a number and units. See `GEORADIUS` for more details.
</details>

<details open>
<summary><code>INKEYS {num} {attribute} ...</code></summary>

limits the result to a given set of keys specified in the list. The first argument must be the length of the list and greater than zero. Non-existent keys are ignored, unless all the keys are non-existent.
</details>

<details open>
<summary><code>INFIELDS {num} {attribute} ...</code></summary>

filters the results to those appearing only in specific attributes of the document, like `title` or `URL`. You must include `num`, which is the number of attributes you're filtering by. For example, if you request `title` and `URL`, then `num` is 2.
</details>

<details open>
<summary><code>RETURN {num} {identifier} AS {property} ...</code></summary>

limits the attributes returned from the document. `num` is the number of attributes following the keyword. If `num` is 0, it acts like `NOCONTENT`.
  `identifier` is either an attribute name (for hashes and JSON) or a JSON Path expression (for JSON).
  `property` is an optional name used in the result. If not provided, the `identifier` is used in the result.
</details>

<details open>
<summary><code>SUMMARIZE ...</code></summary>

returns only the sections of the attribute that contain the matched text. See [Highlighting](/docs/interact/search-and-query/advanced-concepts/highlight/) for more information.
</details>

<details open>
<summary><code>HIGHLIGHT ...</code></summary>

formats occurrences of matched text. See [Highlighting](/docs/interact/search-and-query/advanced-concepts/highlight/) for more information.
</details>

<details open>
<summary><code>SLOP {slop}</code></summary>

is the number of intermediate terms allowed to appear between the terms of the query. 
Suppose you're searching for a phrase _hello world_.
If some terms appear in-between _hello_ and _world_, a `SLOP` greater than `0` allows for these text attributes to match.
By default, there is no `SLOP` constraint.
</details>

<details open>
<summary><code>INORDER</code></summary>

requires the terms in the document to have the same order as the terms in the query, regardless of the offsets between them. Typically used in conjunction with `SLOP`. Default is `false`.

</details>

<details open>
<summary><code>LANGUAGE {language}</code></summary>

use a stemmer for the supplied language during search for query expansion. If querying documents in Chinese, set to `chinese` to
  properly tokenize the query terms. Defaults to English. If an unsupported language is sent, the command returns an error.
  See `FT.CREATE` for the list of languages. 
</details>

<details open>
<summary><code>EXPANDER {expander}</code></summary>

uses a custom query expander instead of the stemmer. See [Extensions](/docs/interact/search-and-query/administration/extensions/).
</details>

<details open>
<summary><code>SCORER {scorer}</code></summary>

uses a [built-in](/docs/interact/search-and-query/advanced-concepts/scoring/) or a [user-provided](/docs/interact/search-and-query/administration/extensions/) scoring function.
</details>

<details open>
<summary><code>EXPLAINSCORE</code></summary>

returns a textual description of how the scores were calculated. Using this option requires `WITHSCORES`.
</details>

<details open>
<summary><code>PAYLOAD {payload}</code></summary>

adds an arbitrary, binary safe payload that is exposed to custom scoring functions. See [Extensions](/docs/interact/search-and-query/administration/extensions/).
</details>

<details open>
<summary><code>SORTBY {attribute} [ASC|DESC] [WITHCOUNT]</code></summary>

orders the results by the value of this attribute. This applies to both text and numeric attributes. Attributes needed for `SORTBY` should be declared as `SORTABLE` in the index, in order to be available with very low latency. Note that this adds memory overhead.

**Sorting Optimizations**: performance is optimized for sorting operations on `DIALECT 4` in different scenarios:
  - Skip Sorter - applied when there is no sort of any kind. The query can return once it reaches the `LIMIT` requested results.
  - Partial Range - applied when there is a `SORTBY` clause over a numeric field, with no filter or filter by the same numeric field, the query iterate on a range large enough to satisfy the `LIMIT` requested results.
  - Hybrid - applied when there is a `SORTBY` clause over a numeric field and another non-numeric filter. Some results will get filtered, and the initial range may not be large enough. The iterator is then rewinding with the following ranges, and an additional iteration takes place to collect the `LIMIT` requested results.
  - No optimization - If there is a sort by score or by non-numeric field, there is no other option but to retrieve all results and compare their values.

**Counts behavior**: optional`WITHCOUNT`argument returns accurate counts for the query results with sorting. This operation processes all results in order to get an accurate count, being less performant than the optimized option (default behavior on `DIALECT 4`)


</details>

<details open>
<summary><code>LIMIT first num</code></summary>

limits the results to the offset and number of results given. Note that the offset is zero-indexed. The default is 0 10, which returns 10 items starting from the first result. You can use `LIMIT 0 0` to count the number of documents in the result set without actually returning them.
</details>

<details open>
<summary><code>TIMEOUT {milliseconds}</code></summary>

overrides the timeout parameter of the module.
</details>

<details open>
<summary><code>PARAMS {nargs} {name} {value}</code></summary>

defines one or more value parameters. Each parameter has a name and a value. 

You can reference parameters in the `query` by a `$`, followed by the parameter name, for example, `$user`. Each such reference in the search query to a parameter name is substituted by the corresponding parameter value. For example, with parameter definition `PARAMS 4 lon 29.69465 lat 34.95126`, the expression `@loc:[$lon $lat 10 km]` is evaluated to `@loc:[29.69465 34.95126 10 km]`. You cannot reference parameters in the query string where concrete values are not allowed, such as in field names, for example, `@loc`. To use `PARAMS`, set `DIALECT` to `2` or greater than `2`.
</details>

<details open>
<summary><code>DIALECT {dialect_version}</code></summary>

selects the dialect version under which to execute the query. If not specified, the query will execute under the default dialect version set during module initial loading or via `FT.CONFIG SET` command.
</details>

## Return

FT.SEARCH returns an array reply, where the first element is an integer reply of the total number of results, and then array reply pairs of document ids, and array replies of attribute/value pairs.

{{% alert title="Notes" color="warning" %}}
 
- If `NOCONTENT` is given, an array is returned where the first element is the total number of results, and the rest of the members are document ids.
- If a hash expires after the query process starts, the hash is counted in the total number of results, but the key name and content return as null.

{{% /alert %}}

### Return multiple values

When the index is defined `ON JSON`, a reply for a single attribute or a single JSONPath may return multiple values when the JSONPath matches multiple values, or when the JSONPath matches an array.

Prior to RediSearch v2.6, only the first of the matched values was returned.
Starting with RediSearch v2.6, all values are returned, wrapped with a top-level array.

In order to maintain backward compatibility, the default behavior with RediSearch v2.6 is to return only the first value.

To return all the values, use `DIALECT` 3 (or greater, when available).

The `DIALECT` can be specified as a parameter in the FT.SEARCH command. If it is not specified, the `DEFAULT_DIALECT` is used, which can be set using `FT.CONFIG SET` or by passing it as an argument to the `redisearch` module when it is loaded.

For example, with the following document and index:


```sh
127.0.0.1:6379> JSON.SET doc:1 $ '[{"arr": [1, 2, 3]}, {"val": "hello"}, {"val": "world"}]'
OK
127.0.0.1:6379> FT.CREATE idx ON JSON PREFIX 1 doc: SCHEMA $..arr AS arr NUMERIC $..val AS val TEXT
OK
```
Notice the different replies, with and without `DIALECT 3`:

```sh
127.0.0.1:6379> FT.SEARCH idx * RETURN 2 arr val
1) (integer) 1
2) "doc:1"
3) 1) "arr"
   2) "[1,2,3]"
   3) "val"
   4) "hello"

127.0.0.1:6379> FT.SEARCH idx * RETURN 2 arr val DIALECT 3
1) (integer) 1
2) "doc:1"
3) 1) "arr"
   2) "[[1,2,3]]"
   3) "val"
   4) "[\"hello\",\"world\"]"
```

## Complexity

FT.SEARCH complexity is O(n) for single word queries. `n` is the number of the results in the result set. Finding all the documents that have a specific term is O(1), however, a scan on all those documents is needed to load the documents data from redis hashes and return them.

The time complexity for more complex queries varies, but in general it's proportional to the number of words, the number of intersection points between them and the number of results in the result set.

## Examples

<details open>
<summary><b>Search for a term in every text attribute</b></summary>

Search for the term "wizard" in every TEXT attribute of an index containing book data.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "wizard"
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a term in title attribute</b></summary>

Search for the term _dogs_ in the `title` attribute.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "@title:dogs"
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for books from specific years</b></summary>

Search for books published in 2020 or 2021.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "@published_at:[2020 2021]"
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a restaurant by distance from longitude/latitude</b></summary>

Search for Chinese restaurants within 5 kilometers of longitude -122.41, latitude 37.77 (San Francisco).

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH restaurants-idx "chinese @location:[-122.41 37.77 5 km]"
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a book by terms but boost specific term</b></summary>

Search for the term _dogs_ or _cats_ in the `title` attribute, but give matches of _dogs_ a higher relevance score (also known as _boosting_).

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "(@title:dogs | @title:cats) | (@title:dogs) => { $weight: 5.0; }"
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a book by a term and EXPLAINSCORE</b></summary>

Search for books with _dogs_ in any TEXT attribute in the index and request an explanation of scoring for each result.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "dogs" WITHSCORES EXPLAINSCORE
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a book by a term and TAG</b></summary>

Search for books with _space_ in the title that have `science` in the TAG attribute `categories`.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "@title:space @categories:{science}"
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a book by a term but limit the number</b></summary>

Search for books with _Python_ in any `TEXT` attribute, returning 10 results starting with the 11th result in the entire result set (the offset parameter is zero-based), and return only the `title` attribute for each result.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "python" LIMIT 10 10 RETURN 1 title
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a book by a term and price</b></summary>

Search for books with _Python_ in any `TEXT` attribute, returning the price stored in the original JSON document.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "python" RETURN 3 $.book.price AS price
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a book by title and distance</b></summary>

Search for books with semantically similar title to _Planet Earth_. Return top 10 results sorted by distance.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "*=>[KNN 10 @title_embedding $query_vec AS title_score]" PARAMS 2 query_vec <"Planet Earth" embedding BLOB> SORTBY title_score DIALECT 2
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a phrase using SLOP</b></summary>

Search for a phrase _hello world_.
First, create an index.

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE memes SCHEMA phrase TEXT
OK
{{< / highlight >}}

Add variations of the phrase _hello world_.

{{< highlight bash >}}
127.0.0.1:6379> HSET s1 phrase "hello world"
(integer) 1
127.0.0.1:6379> HSET s2 phrase "hello simple world"
(integer) 1
127.0.0.1:6379> HSET s3 phrase "hello somewhat less simple world"
(integer) 1
127.0.0.1:6379> HSET s4 phrase "hello complicated yet encouraging problem solving world"
(integer) 1
127.0.0.1:6379> HSET s5 phrase "hello complicated yet amazingly encouraging problem solving world"
(integer) 1
{{< / highlight >}}

Then, search for the phrase _hello world_. The result returns all documents that contain the phrase.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(hello world)' NOCONTENT 
1) (integer) 5
2) "s1"
3) "s2"
4) "s3"
5) "s4"
6) "s5"
{{< / highlight >}}

Now, return all documents that have one of fewer words between _hello_ and _world_.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(hello world)' NOCONTENT SLOP 1
1) (integer) 2
2) "s1"
3) "s2"
{{< / highlight >}}

Now, return all documents with three or fewer words between _hello_ and _world_.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(hello world)' NOCONTENT SLOP 3
1) (integer) 3
2) "s1"
3) "s2"
4) "s3"
{{< / highlight >}}

`s5` needs a higher `SLOP` to match, `SLOP 6` or higher, to be exact. See what happens when you set `SLOP` to `5`.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(hello world)' NOCONTENT SLOP 5
1) (integer) 4
2) "s1"
3) "s2"
4) "s3"
5) "s4"
{{< / highlight >}}

If you add additional terms (and stemming), you get these results.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(hello amazing world)' NOCONTENT 
1) (integer) 1
2) "s5"
{{< / highlight >}}

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(hello encouraged world)' NOCONTENT SLOP 5
1) (integer) 2
2) "s4"
3) "s5"
{{< / highlight >}}

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(hello encouraged world)' NOCONTENT SLOP 4
1) (integer) 1
2) "s4"
{{< / highlight >}}

If you swap the terms, you can still retrieve the correct phrase.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(amazing hello world)' NOCONTENT
1) (integer) 1
2) "s5"
{{< / highlight >}}

But, if you use `INORDER`, you get zero results.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(amazing hello world)' NOCONTENT INORDER
1) (integer) 0
{{< / highlight >}}

Likewise, if you use a query attribute `$inorder` set to `true`, `s5` is not retrieved.

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH memes '@phrase:(amazing hello world)=>{$inorder: true;}' NOCONTENT
1) (integer) 0
{{< / highlight >}}

To sum up, the `INORDER` argument or `$inorder` query attribute require the query terms to match terms with similar ordering.

</details>

<details open>
<summary><b>NEW!!! Polygon Search with WITHIN and CONTAINS operators</b></summary>

Query for polygons which contain a given geoshape or are within a given geoshape

First, create an index using `GEOSHAPE` type with a `FLAT` coordinate system:


{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE idx SCHEMA geom GEOSHAPE FLAT
OK
{{< / highlight >}}

Adding a couple of geometries using `HSET`:


{{< highlight bash >}}
127.0.0.1:6379> HSET small geom 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))'
(integer) 1
127.0.0.1:6379> HSET large geom 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))'
(integer) 1
{{< / highlight >}}

Query with `WITHIN` operator:

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH idx '@geom:[WITHIN $poly]' PARAMS 2 poly 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))' DIALECT 3

1) (integer) 1
2) "small"
3) 1) "geom"
   2) "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"
{{< / highlight >}}

Query with `CONTAINS` operator:


{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH idx '@geom:[CONTAINS $poly]' PARAMS 2 poly 'POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))' DIALECT 3

1) (integer) 2
2) "small"
3) 1) "geom"
   2) "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"
4) "large"
5) 1) "geom"
   2) "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))"
{{< / highlight >}}

</details>

## See also

`FT.CREATE` | `FT.AGGREGATE` 

## Related topics

- [Extensions](/docs/interact/search-and-query/administration/extensions/)
- [Highlighting](/docs/interact/search-and-query/advanced-concepts/highlight/)
- [Query syntax](/docs/interact/search-and-query/query/)
- [RediSearch](/docs/stack/search)
