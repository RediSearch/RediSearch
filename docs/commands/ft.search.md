#### Complexity

O(n) for single word queries. `n` is the number of the results in the result set. Finding all the documents that have a specific term is O(1), however, a scan on all those documents is needed to load the documents data from redis hashes and return them.

The time complexity for more complex queries varies, but in general it's proportional to the number of words, the number of intersection points between them and the number of results in the result set.

---

Searches the index with a textual query, returning either documents or just ids.

#### Parameters

- **index**: The index name. The index must be first created with `FT.CREATE`.
- **query**: the text query to search. If it's more than a single word, put it in quotes.
  Refer to [query syntax](/redisearch/reference/query_syntax) for more details.
- **NOCONTENT**: If it appears after the query, we only return the document ids and not
  the content. This is useful if RediSearch is only an index on an external document collection
- **VERBATIM**: if set, we do not try to use stemming for query expansion but search the query terms
  verbatim.
- **NOSTOPWORDS**: If set, we do not filter stopwords from the query.
- **WITHSCORES**: If set, we also return the relative internal score of each document. this can be
  used to merge results from multiple instances
- **WITHPAYLOADS**: If set, we retrieve optional document payloads (see `FT.CREATE`).
  the payloads follow the document id, and if `WITHSCORES` was set, follow the scores.
- **WITHSORTKEYS**: Only relevant in conjunction with **SORTBY**. Returns the value of the sorting key,
  right after the id and score and /or payload if requested. This is usually not needed by users, and
  exists for distributed search coordination purposes.

- **FILTER numeric_attribute min max**: If set, and numeric_attribute is defined as a numeric attribute in
  `FT.CREATE`, we will limit results to those having numeric values ranging between min and max.
  min and max follow ZRANGE syntax, and can be **-inf**, **+inf** and use `(` for exclusive ranges.
  Multiple numeric filters for different attributes are supported in one query.
- **GEOFILTER {geo_attribute} {lon} {lat} {radius} m|km|mi|ft**: If set, we filter the results to a given radius
  from lon and lat. Radius is given as a number and units. See `GEORADIUS`
  for more details.
- **INKEYS {num} {attribute} ...**: If set, we limit the result to a given set of keys specified in the
  list.
  the first argument must be the length of the list, and greater than zero.
  Non-existent keys are ignored - unless all the keys are non-existent.
- **INFIELDS {num} {attribute} ...**: If set, filter the results to ones appearing only in specific
  attributes of the document, like `title` or `URL`. You must include `num`, which is the number of attributes you're filtering by. For example, if you request `title` and `URL`, then `num` is 2.

- **RETURN {num} {identifier} AS {property} ...**: Use this keyword to limit which attributes from the document are returned.
  `num` is the number of attributes following the keyword. If `num` is 0, it acts like `NOCONTENT`.
  `identifier` is either an attribute name (for hashes and JSON) or a JSON Path expression for (JSON).
  `property` is an optional name used in the result. If not provided, the `identifier` is used in the result.
- **SUMMARIZE ...**: Use this option to return only the sections of the attribute which contain the
  matched text.
  See [Highlighting](/redisearch/reference/highlight) for more details
- **HIGHLIGHT ...**: Use this option to format occurrences of matched text. See [Highlighting](/redisearch/reference/highlight) for more
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
  See `FT.CREATE` for the list of languages.

- **EXPANDER {expander}**: If set, we will use a custom query expander instead of the stemmer. [See Extensions](/redisearch/reference/extensions).
- **SCORER {scorer}**: If set, we will use a custom scoring function defined by the user. [See Extensions](/redisearch/reference/extensions).
- **EXPLAINSCORE**: If set, will return a textual description of how the scores were calculated. Using this options requires the WITHSCORES option.
- **PAYLOAD {payload}**: Add an arbitrary, binary safe payload that will be exposed to custom scoring
  functions. [See Extensions](/redisearch/reference/extensions).

- **SORTBY {attribute} [ASC|DESC]**: If specified, the results are ordered by the value of this attribute. This applies to both text and numeric attributes. Attributes needed for **SORTBY** should be declared as **SORTABLE** in the index, in order to be available with very low latency (notice this adds memory overhead)

  If `SORTBY` is not specified, the order is by score.

  For similar scores or values, the relative order among them is undefined.
  

- **LIMIT first num**: Limit the results to the offset and number of results given.
  Note that the offset is zero-indexed. The default is 0 10, which returns 10 items starting from the first result.
  If a result key expires during the query, its content will be a null array.

{{% alert title="Tip" color="info" %}}
`LIMIT 0 0` can be used to count the number of documents in the result set without actually returning them.
{{% /alert %}}

- **TIMEOUT {milliseconds}**: If set, we will override the timeout parameter of the module.

* **PARAMS {nargs} {name} {value}**. Define one or more value parameters. Each parameter has a name and a value. Parameters can be referenced in the **query** by a `$`, followed by the parameter name, e.g., `$user`, and each such reference in the search query to a parameter name is substituted by the corresponding parameter value. For example, with parameter definition `PARAMS 4 lon 29.69465 lat 34.95126`, the expression `@loc:[$lon $lat 10 km]` would be evaluated to `@loc:[29.69465 34.95126 10 km]`. Parameters cannot be referenced in the query string where concrete values are not allowed, such as in field names, e.g., `@loc`. To use `PARAMS`, `DIALECT` must be set to 2.

- **DIALECT {dialect_version}**. Choose the dialect version to execute the query under. If not specified, the query will execute under the default dialect version set during module initial loading or via `FT.CONFIG SET` command.

@return

@array-reply, where the first element is an @integer-reply of the total number of results,and then @array-reply pairs of document IDs, and a @array-replies of attribute/value pairs.

If **NOCONTENT** was given, we return an array where the first element is the total number of results, and the rest of the members are document ids.

{{% alert title="Expiration of hashes during a search query" color="info" %}}
If a hash expires after the query process starts, the hash will be counted in the total number of results, but the key's name and content will return as null.
{{% /alert %}}

@examples

Searching for the term "wizard" in every TEXT attribute of an index containing book data:

```
FT.SEARCH books-idx "wizard"
```
Searching for the term "dogs" in only the "title" attribute:

```
FT.SEARCH books-idx "@title:dogs"
```

Searching for books published in 2020 or 2021:

```
FT.SEARCH books-idx "@published_at:[2020 2021]"
```

Searching for Chinese restaurants within 5 kilometers of longitude -122.41, latitude 37.77 (San Francisco):

```
FT.SEARCH restaurants-idx "chinese @location:[-122.41 37.77 5 km]"
```

Searching for the term "dogs" or "cats" in the "title" attribute, but giving matches of "dogs" a higher relevance score (also known as *boosting*):

```
FT.SEARCH books-idx "(@title:dogs | @title:cats) | (@title:dogs) => { $weight: 5.0; }"
```
Searching for books with "dogs" in any TEXT attribute in the index and requesting an explanation of scoring for each result:

```
FT.SEARCH books-idx "dogs" WITHSCORES EXPLAINSCORE
```

Searching for books with "space" in the title that have "science" in the TAG attribute "categories":

```
FT.SEARCH books-idx "@title:space @categories:{science}"
```

Searching for books with "Python" in any TEXT attribute, returning ten results starting with the eleventh result in the entire result set (the offset parameter is zero-based), and returning only the "title" attribute for each result:

```
FT.SEARCH books-idx "python" LIMIT 10 10 RETURN 1 title
```

Searching for books with "Python" in any TEXT attribute, returning the price stored in the original JSON document.

```
FT.SEARCH books-idx "python" RETURN 3 $.book.price AS price
```

Searching for books with semantically similar "title" to "Planet Earth", Return top 10 results sorted by distance.

```
FT.SEARCH books-idx "*=>[KNN 10 @title_embedding $query_vec AS title_score]" PARAMS 2 query_vec <"Planet Earth" embedding BLOB> SORTBY title_score DIALECT 2
```

{{% alert title="More examples" color="info" %}}
For more details and query examples, see [query syntax](/redisearch/reference/query_syntax).
{{% /alert %}}
