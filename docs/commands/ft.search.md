
---
syntax: 
---

Search the index with a textual query, returning either documents or just ids

## Syntax

{{< highlight bash >}}
FT.SEARCH index query 
          [NOCONTENT] 
          [VERBATIM] [NOSTOPWORDS] 
          [WITHSCORES] 
          [WITHPAYLOADS] 
          [WITHSORTKEYS] 
          [ FILTER numeric_field min max [ FILTER numeric_field min max ...]] 
          [ GEOFILTER geo_field lon lat radius m | km | mi | ft [ GEOFILTER geo_field lon lat radius m | km | mi | ft ...]] 
          [ INKEYS count key [key ...]] [ INFIELDS count field [field ...]] 
          [ RETURN count identifier [AS property] [ identifier [AS property] ...]] 
          [ SUMMARIZE [ FIELDS count field [field ...]] [FRAGS num] [LEN fragsize] [SEPARATOR separator]] 
          [ HIGHLIGHT [ FIELDS count field [field ...]] [ TAGS open close]] 
          [SLOP slop] 
          [TIMEOUT timeout] 
          [INORDER] 
          [LANGUAGE language] 
          [EXPANDER expander] 
          [SCORER scorer] 
          [EXPLAINSCORE] 
          [PAYLOAD payload] 
          [ SORTBY sortby [ ASC | DESC]] 
          [ LIMIT offset num] 
          [ PARAMS nargs name value [ name value ...]] 
          [DIALECT dialect]
{{< / highlight >}}

[Examples](#examples)

## Required parameters

<details open>
<summary><code>index</code></summary>

is index name. You must first create the index using `FT.CREATE`.
</details>

<details open>
<summary><code>query</code></summary> 

is text query to search. If it's more than a single word, put it in quotes. Refer to [Query syntax](/redisearch/reference/query_syntax) for more details.
</details>

## Optional parameters

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

returns the value of the sorting key, right after the id and score and/or payload, if requested. This is usually not needed, and
  exists for distributed search coordination purposes. This option is relevant only if used in conjunction with `SORTBY`.
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

returns only the sections of the attribute that contain the matched text. See [Highlighting](/redisearch/reference/highlight) for more information.
</details>

<details open>
<summary><code>HIGHLIGHT ...</code></summary>

formats occurrences of matched text. See [Highlighting](/redisearch/reference/highlight) for more information.
</details>

<details open>
<summary><code>SLOP {slop}</code></summary>

allows a maximum of N intervening number of unmatched offsets between phrase terms. In other words, the slop for exact phrases is 0.
</details>

<details open>
<summary><code>INORDER</code></summary>

puts the query terms in the same order in the document as in the query, regardless of the offsets between them. Typically used in conjunction with `SLOP`.
</details>

<details open>
<summary><code>LANGUAGE {language}</code></summary>

use a stemmer for the supplied language during search for query expansion. If querying documents in Chinese, set to `chinese` to
  properly tokenize the query terms. Defaults to English. If an unsupported language is sent, the command returns an error.
  See `FT.CREATE` for the list of languages. 
</details>

<details open>
<summary><code>EXPANDER {expander}</code></summary>

uses a custom query expander instead of the stemmer. See [Extensions](/redisearch/reference/extensions).
</details>

<details open>
<summary><code>SCORER {scorer}</code></summary>

uses a custom scoring function you define. See [Extensions](/redisearch/reference/extensions).
</details>

<details open>
<summary><code>EXPLAINSCORE</code></summary>

returns a textual description of how the scores were calculated. Using this options requires the WITHSCORES option.
</details>

<details open>
<summary><code>PAYLOAD {payload}</code></summary>

adds an arbitrary, binary safe payload that is exposed to custom scoring functions. See [Extensions](/redisearch/reference/extensions).
</details>

<details open>
<summary><code>SORTBY {attribute} [ASC|DESC]</code></summary>

orders the results by the value of this attribute. This applies to both text and numeric attributes. Attributes needed for `SORTBY` should be declared as `SORTABLE` in the index, in order to be available with very low latency. Note that this adds memory overhead.
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

You can reference parameters in the `query` by a `$`, followed by the parameter name, for example, `$user`. Each such reference in the search query to a parameter name is substituted by the corresponding parameter value. For example, with parameter definition `PARAMS 4 lon 29.69465 lat 34.95126`, the expression `@loc:[$lon $lat 10 km]` is evaluated to `@loc:[29.69465 34.95126 10 km]`. You cannot reference parameters in the query string where concrete values are not allowed, such as in field names, for example, `@loc`. To use `PARAMS`, set `DIALECT` to 2.
</details>

<details open>
<summary><code>DIALECT {dialect_version}</code></summary>

selects the dialect version under which to execute the query. If not specified, the query will execute under the default dialect version set during module initial loading or via `FT.CONFIG SET` command.
</details>

## Return

FT.SEARCH returns an array reply, where the first element is an integer reply of the total number of results, and then array reply pairs of document ids, and array replies of attribute/value pairs.

<note><b>Notes:</b> 
- If `NOCONTENT` is given, an array is returned where the first element is the total number of results, and the rest of the members are document ids.
- If a hash expires after the query process starts, the hash is counted in the total number of results, but the key name and content return as null.
</note>

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

Searching for books with _space_ in the title that have `science` in the TAG attribute `categories`:

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "@title:space @categories:{science}"
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a book by a term but limit the number</b></summary>

Searching for books with _Python_ in any TEXT attribute, returning ten results starting with the eleventh result in the entire result set (the offset parameter is zero-based), and returning only the `title` attribute for each result:

{{< highlight bash >}}
127.0.0.1:6379> FT.SEARCH books-idx "python" LIMIT 10 10 RETURN 1 title
{{< / highlight >}}
</details>

<details open>
<summary><b>Search for a book by a term and price</b></summary>

Search for books with _Python_ in any TEXT attribute, returning the price stored in the original JSON document.

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

## See also

`FT.SEARCH` | `FT.AGGREGATE` 

## Related topics

- [Extensions](/redisearch/reference/extensions)
- [Highlighting](/redisearch/reference/highlight)
- [Query syntax](/redisearch/reference/query_syntax)
- [RediSearch](/docs/stack/search)
