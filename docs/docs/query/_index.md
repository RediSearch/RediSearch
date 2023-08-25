---
title: "Query"
linkTitle: "Query"
weight: 5
description: >
    Learn how to use query syntax
aliases:
  - /docs/stack/search/reference/query_syntax/    
  - /redisearch/reference/query_syntax
---

You can use simple syntax for complex queries using these rules:

* Multiword phrases are lists of tokens, for example, `foo bar baz`, and imply intersection (AND) of the terms.
* Exact phrases are wrapped in quotes, for example, `"hello world"`.
* `OR` unions are expressed with a pipe (`|`), for example, `hello|hallo|shalom|hola`. 

   **Notes**:   
   
   Consider the differences in parser behavior in example `hello world | "goodbye" moon`:
   * In DIALECT 1, this query is interpreted as searching for `(hello world | "goodbye") moon`.
   * In DIALECT 2 or greater, this query is interpreted as searching for either `hello world` **OR** `"goodbye" moon`.
   
* `NOT` negation of expressions or subqueries is expressed with a subtraction symbol (`-`), for example, `hello -world`. Purely negative queries (for example, `-foo` or `-@title:(foo|bar)`) are also supported.

   **Notes**:
   
   Consider a simple query with negation `-hello world`:
   * In DIALECT 1, this query is interpreted as "find values in any field that does not contain `hello` **AND** does not contain `world`". The equivalent is `-(hello world)` or `-hello -world`.
   * In DIALECT 2 or greater, this query is interpreted `as -hello` **AND** `world` (only `hello` is negated).
   * In DIALECT 2 or greater, to achieve the default behavior of DIALECT 1, update your query to `-(hello world)`.
  
* Prefix/infix/suffix matches (all terms starting/containing/ending with a term) are expressed with a `*`. For performance reasons, a minimum term length is enforced (default is 2), but is configurable.
* In DIALECT 2 or greater, wildcard pattern matches are expressed as `"w'foo*bar?'"`. **Note the use of double quotes to sustain the _w_ pattern.** 
* A special _wildcard query_ that returns all results in the index, `*` (cannot be combined with other options).
* `DIALECT 3` returns JSON rather than scalars from multivalue attributes **(as of v2.6.1)**.
* Selection of specific fields using the syntax `hello @field:world`.
* Numeric range matches on numeric fields with the syntax `@field:[{min} {max}]`.
* Geo radius matches on geo fields with the syntax `@field:[{lon} {lat} {radius} {m|km|mi|ft}]`.
* Range queries on vector fields with the syntax `@field:[VECTOR_RANGE {radius} $query_vec]`, where `query_vec` is given as a query parameter **(as of v2.6)**.
* KNN queries on vector fields with or without pre-filtering with the syntax `{filter_query}=>[KNN {num} @field $query_vec]` **(as of v2.4)**.
* Tag field filters with the syntax `@field:{tag | tag | ...}`. See the full documentation on [tags](/docs/interact/search-and-query/advanced-concepts/tags/).
* Optional terms or clauses: `foo ~bar` means bar is optional but documents containing `bar` will rank higher.
* Fuzzy matching on terms: `%hello%` means all terms with Levenshtein distance of 1 from it.
* An expression in a query can be wrapped in parentheses to disambiguate, for example, `(hello|hella) (world|werld)`.
* Query attributes can be applied to individual clauses, for example, `(foo bar) => { $weight: 2.0; $slop: 1; $inorder: false; }`.
* Combinations of the above can be used together, for example, `hello (world|foo) "bar baz" bbbb`.

## Pure negative queries

As of v0.19.3, it is possible to have a query consisting of just a negative expression, for example, `-hello` or `-(@title:(foo|bar))`. The results are all the documents *NOT* containing the query terms.

{{% alert title="Warning" color="warning" %}}
Any complex expression can be negated this way, however, caution should be taken here: if a negative expression has little or no results, this is equivalent to traversing and ranking all the documents in the index, which can be slow and cause high CPU consumption.
{{% /alert %}}

## Field modifiers

You can specify field modifiers in a query, and not just by using the `INFIELDS` global keyword.

To specify which fields the query matches, prepend the expression with the `@` symbol, the field name, and a `:` (colon) symbol, per query expression or subexpression.

If a field modifier precedes multiple words or expressions, it applies only to the adjacent expression with DIALECT 1. With DIALECT 2 or greater, you extend the query to other fields.

Consider this simple query: `@name:James Brown`. Here, the field modifier `@name` is followed by two words: `James` and `Brown`.

* In DIALECT 1, this query would be interpreted as "find `James Brown` in the `@name` field".
* In DIALECT 2 or greater, this query will be interpreted as "find `James` in the `@name` field **AND** `Brown` in **ANY** text field. In other words, it would be interpreted as `(@name:James) Brown`.
* In DIALECT 2 or greater, to achieve the default behavior of DIALECT 1, update your query to `@name:(James Brown)`.

If a field modifier precedes an expression in parentheses, it applies only to the expression inside the parentheses. The expression should be valid for the specified field, otherwise it is skipped.

To create complex filtering on several fields, you can combine multiple modifiers. For example, if you have an index of car models, with a vehicle class, country of origin, and engine type, you can search for SUVs made in Korea with hybrid or diesel engines using the following query:

```
FT.SEARCH cars "@country:korea @engine:(diesel|hybrid) @class:suv"
```

You can apply multiple modifiers to the same term or grouped terms:

```
FT.SEARCH idx "@title|body:(hello world) @url|image:mydomain"
```

Now, you search for documents that have `"hello"` and `"world"` either in the body or the title and the term `mydomain` in their `url` or `image` fields.

## Numeric filters in query

If a field in the schema is defined as NUMERIC, it is possible to either use the FILTER argument in the Redis request or filter with it by specifying filtering rules in the query. The syntax is `@field:[{min} {max}]`, for example, `@price:[100 200]`.

### A few notes on numeric predicates

1. It is possible to specify a numeric predicate as the entire query, whereas it is impossible to do it with the `FILTER` argument.

2. It is possible to intersect or union multiple numeric filters in the same query, be it for the same field or different ones.

3. `-inf`, `inf` and `+inf` are acceptable numbers in a range. Therefore, _greater than 100_ is expressed as `[(100 inf]`.

4. Numeric filters are inclusive. Exclusive min or max are expressed with `(` prepended to the number, for example, `[(100 (200]`.

5. It is possible to negate a numeric filter by prepending a `-` sign to the filter. For example, returning a result where price differs from 100 is expressed as: `@title:foo -@price:[100 100]`.

## Tag filters

As of v0.91, you can use a special field type called _tag field_, with simpler tokenization and encoding in the index. You can't access the values in these fields using general fieldless search. Instead, you use special syntax:

```
@field:{ tag | tag | ...}
```

Example:

```
@cities:{ New York | Los Angeles | Barcelona }
```

Tags can have multiple words or include other punctuation marks other than the field's separator (`,` by default). Punctuation marks in tags should be escaped with a backslash (`\`). 

{{% alert title="Note" color="warning" %}}
Before RediSearch 2.6, it was also recommended to escape spaces. The reason was that, if a multiword tag included stopwords, a syntax error was returned. So tags, like "to be or not to be" needed be escaped as "to\ be\ or\ not\ to\ be". For good measure, you also could escape all spaces within tags. Starting with RediSearch 2.6, using `DIALECT 2` or greater you can use spaces in a `tag` query, even with stopwords.
{{% /alert %}}

Notice that multiple tags in the same clause create a union of documents containing either tags. To create an intersection of documents containing *all* tags, you should repeat the tag filter several times, for example:

```
# Return all documents containing all three cities as tags
@cities:{ New York } @cities:{Los Angeles} @cities:{ Barcelona }

# Now, return all documents containing either city
@cities:{ New York | Los Angeles | Barcelona }
```

Tag clauses can be combined into any subclause, used as negative expressions, optional expressions, and so on.

## Geo filters

As of v0.21, it is possible to add geo radius queries directly into the query language  with the syntax `@field:[{lon} {lat} {radius} {m|km|mi|ft}]`. This filters the result to a given radius from a lon,lat point, defined in meters, kilometers, miles or feet. See Redis' own `GEORADIUS` command for more details as it is used internally for that).

Radius filters can be added into the query just like numeric filters. For example, in a database of businesses, looking for Chinese restaurants near San Francisco (within a 5km radius) would be expressed as: `chinese restaurant @location:[-122.41 37.77 5 km]`.

## Polygon search

Geospatial databases are essential for managing and analyzing location-based data in a variety of industries. They help organizations make data-driven decisions, optimize operations, and achieve their strategic goals more efficiently. Polygon search extends Redis's geospatial search capabilities to be able to query against a value in a `GEOSHAPE` attribute. This value must follow a ["well-known text"](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) (WKT) representation of geometry. Two such geometries are supported:

- `POINT`, for example `POINT(2 4)`.
- `POLYGON`, for example `POLYGON((2 2, 2 8, 6 11, 10 8, 10 2, 2 2))`.

There is a new schema field type called `GEOSHAPE`, which can be specified as either:

- `FLAT` for Cartesian X Y coordinates
- `SPHERICAL` for geographic longitude and latitude coordinates. This is the default coordinate system.

Finally, there's new `FT.SEARCH` syntax that allows you to query for polygons that either contain or are within a given geoshape.

`@field:[{WITHIN|CONTAINS} $geometry] PARAMS 2 geometry {geometry}`

Here's an example using two stacked polygons that represent a box contained within a house.

![two stacked polygons](../img/polygons.png)

First, create an index using a `FLAT` `GEOSHAPE`, representing a 2D X Y coordinate system.

`FT.CREATE polygon_idx PREFIX 1 shape: SCHEMA g GEOSHAPE FLAT t TEXT`

Next, create the data structures that represent the geometries in the picture.

```bash
HSET shape:1 t "this is my house" g "POLYGON((2 2, 2 8, 6 11, 10 8, 10 2, 2 2))"
HSET shape:2 t "this is a square in my house" g "POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))"
```
Finally, use `FT.SEARCH` to query the geometries. Note the use of `DIALECT 3`, which is required. Here are a few examples.

Search for a polygon that contains a specified point:

```bash
FT.SEARCH polygon_idx "@g:[CONTAINS $point]" PARAMS 2 point 'POINT(8 8)' DIALECT 3
1) (integer) 1
2) "shape:1"
3) 1) "t"
   2) "this is my house"
   3) "g"
   4) "POLYGON((2 2, 2 8, 6 11, 10 8, 10 2, 2 2))"
```

Search for geometries contained in a specified polygon:

```bash
FT.SEARCH polygon_idx "@g:[WITHIN $poly]" PARAMS 2 poly 'POLYGON((0 0, 0 100, 100 100, 100 0, 0 0))' DIALECT 3
1) (integer) 2
2) "shape:2"
3) 1) "t"
   2) "this is a square in my house"
   3) "g"
   4) "POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))"
4) "shape:1"
5) 1) "t"
   2) "this is my house"
   3) "g"
   4) "POLYGON((2 2, 2 8, 6 11, 10 8, 10 2, 2 2))"
```

Search for a polygon that is not contained in the indexed geometries:

```bash
FT.SEARCH polygon_idx "@g:[CONTAINS $poly]" PARAMS 2 poly 'POLYGON((14 4, 14 6, 16 6, 16 4, 14 4))' DIALECT 3
1) (integer) 0
```

Search for a polygon that is known to be contained within the geometries (the box):

```bash
FT.SEARCH polygon_idx "@g:[CONTAINS $poly]" PARAMS 2 poly 'POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))' DIALECT 3
1) (integer) 2
2) "shape:1"
3) 1) "t"
   2) "this is my house"
   3) "g"
   4) "POLYGON((2 2, 2 8, 6 11, 10 8, 10 2, 2 2))"
4) "shape:2"
5) 1) "t"
   2) "this is a square in my house"
   3) "g"
   4) "POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))"
```

Note that both the house and box shapes were returned.

{{< alert title="Note" >}}
GEOSHAPE does not support JSON multi-value or SORTABLE options.
{{< /alert >}}

For more examples, see the `FT.CREATE` and `FT.SEARCH` command pages.

## Vector similarity search

You can add vector similarity queries directly into the query language by:

1. Using a **range** query with the syntax of `@vector:[VECTOR_RANGE {radius} $query_vec]`, which filters the results to a given radius from a given query vector. The distance metric derives from the definition of @vector field in the index schema, for example, Cosine or L2 **(as of v2.6.1)**.

2. Running a **KNN** (K Nearest Neighbors) query on @vector field. The basic syntax is `"*=>[ KNN {num|$num} @vector $query_vec ]"`.
It is also possible to run a hybrid query on filtered results. A hybrid query allows the user to specify a filter criteria that all results in a KNN query must satisfy. The filter criteria can include any type of field (e.g., indexes created on both vectors and other values such as TEXT, PHONETIC, NUMERIC, GEO, etc.).
The general syntax for hybrid query is `{some filter query}=>[ KNN {num|$num} @vector $query_vec]`, where `=>` separates the filter query from the vector KNN query. 


**Examples:**

* Return 10 nearest neighbors entities in which `query_vec` is closest to the vector stored in `@vector_field`: 
  
  `*=>[KNN 10 @vector_field $query_vec]`

* Among entities published between 2020 and 2022, return 10 "nearest neighbors" entities in which `query_vec` is closest to the vector stored in `@vector_field`: 

  `@published_year:[2020 2022]=>[KNN 10 @vector_field $query_vec]`

* Return every entity for which the distance between the vector stored under its @vector_field and `query_vec` is at most 0.5, in terms of @vector_field distance metric:

  `@vector_field:[VECTOR_RANGE 0.5 $query_vec]`

As of v2.4, the KNN vector search can be used **once** in the query, while, as of v2.6, the vector range filter can be used **multiple** times in a query. For more information on vector similarity syntax, see [Querying vector fields](/docs/stack/search/reference/vectors/#querying-vector-fields), and [Vector search examples](/docs/stack/search/reference/vectors/#vector-search-examples) sections.

## Prefix matching

On index updating, we maintain a dictionary of all terms in the index. This can be used to match all terms starting with a given prefix. Selecting prefix matches is done by appending `*` to a prefix token. For example:

```
hel* world
```

Will be expanded to cover `(hello|help|helm|...) world`.

### A few notes on prefix searches

1. As prefixes can be expanded into many many terms, use them with caution. There is no magic going on, the expansion will create a Union operation of all suffixes.

2. As a protective measure to avoid selecting too many terms, and block redis, which is single threaded, there are two limitations on prefix matching:

  * Prefixes are limited to 2 letters or more. You can change this number by using the `MINPREFIX` setting on the module command line.

  * Expansion is limited to 200 terms or less. You can change this number by using the `MAXEXPANSIONS` setting on the module command line.

3. Prefix matching fully supports Unicode and is case insensitive.

4. Currently, there is no sorting or bias based on suffix popularity, but this is on the near-term roadmap.

## Infix/suffix matching

As of v2.6.0, the dictionary can be used for infix (contains) or suffix queries by appending `*` to the token. For example:

```
*sun* *ing 
```

These queries are CPU intensive because they require iteration over the whole dictionary.

{{% alert title="Note" color="warning" %}}
All notes about prefix searches also apply to infix/suffix queries.
{{% /alert %}}

### Using a suffix trie

A suffix trie maintains a list of terms which match the suffix. If you add a suffix trie to a field using the `WITHSUFFIXTRIE` keyword, you can create more efficient infix and suffix queries because it eliminates the need to iterate over the whole dictionary. However, the iteration on the union does not change. 

Suffix queries create a union of the list of terms from the suffix term node. Infix queries use the suffix terms as prefixes to the trie and create a union of all terms from all matching nodes.

## Wildcard matching

As of v2.6.0, you can use the dictionary for wildcard matching queries with these parameters.

* `?` - for any single character
* `*` - for any character repeating zero or more times
* ' and \ - for escaping; other special characters are ignored

An example of the syntax is `"w'foo*bar?'"`.

### Using a suffix trie

A _suffix trie_ maintains a list of terms which match the suffix. If you add a suffix trie to a field using the `WITHSUFFIXTRIE` keyword, you can create more efficient wildcard matching queries because it eliminates the need to iterate over the whole dictionary. However, the iteration on the union does not change. 

With a suffix trie, the wildcard pattern is broken into tokens at every `*` character. A heuristic is used to choose the token with the least terms, and each term is matched with the wildcard pattern.

## Fuzzy matching

As of v1.2.0, the dictionary of all terms in the index can also be used to perform [Fuzzy Matching](https://en.wikipedia.org/wiki/Approximate_string_matching). 
Fuzzy matches are performed based on [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) (LD). 
Fuzzy matching on a term is performed by surrounding the term with '%', for example:

```
%hello% world
```

This performs fuzzy matching on `hello` for all terms where LD is 1.

As of v1.4.0, the LD of the fuzzy match can be set by the number of '%' surrounding it, so that `%%hello%%` will perform fuzzy matching on 'hello' for all terms where LD is 2.

The maximum LD for fuzzy matching is 3.

## Wildcard queries

As of v1.1.0, you can use a special query to retrieve all the documents in an index. This is meant mostly for the aggregation engine. You can call it by specifying only a single star sign as the query string, in other words, `FT.SEARCH myIndex *`.

You can't combine this with any other filters, field modifiers, or anything inside the query. It is technically possible to use the deprecated `FILTER` and `GEOFILTER` request parameters outside the query string in conjunction with a wildcard, but this makes the wildcard meaningless and only hurts performance.

## Query attributes

As of v1.2.0, you can apply specific query modifying attributes to specific clauses of the query.

The syntax is `(foo bar) => { $attribute: value; $attribute:value; ...}`:

```
(foo bar) => { $weight: 2.0; $slop: 1; $inorder: true; }
~(bar baz) => { $weight: 0.5; }
```

The supported attributes are:

1. **$weight**: determines the weight of the sub-query or token in the overall ranking on the result (default: 1.0).
2. **$slop**: determines the maximum allowed "slop" (space between terms) in the query clause (default: 0).
3. **$inorder**: whether or not the terms in a query clause must appear in the same order as in the query, usually set alongside with `$slop` (default: false).
4. **$phonetic**: whether or not to perform phonetic matching (default: true). Note: setting this attribute on for fields which were not creates as `PHONETIC` will produce an error.

As of v2.6.1, the query attributes syntax supports these additional attributes:

* **$yield_distance_as**: specify the distance field name for later sorting by it and/or returning it, for clauses that yield some distance metric. It is currently supported for vector queries only (both KNN and range).   
* **vector query params**: pass optional parameters for [vector queries](/docs/stack/search/reference/vectors/#querying-vector-fields) in key-value format.

## A few query examples

* Simple phrase query - `hello` _AND_ `world`:

        hello world

* Exact phrase query - `hello` _FOLLOWED BY_ `world`:

        "hello world"

* Union - documents containing either `hello` _OR_ `world`:

        hello|world

* Not - documents containing `hello` _BUT NOT_ `world`:

        hello -world

* Intersection of unions:

        (hello|halo) (world|werld)

* Negation of union:

        hello -(world|werld)

* Union inside phrase:

        (barack|barrack) obama

* Optional terms with higher priority to ones containing more matches:

        obama ~barack ~michelle

* Exact phrase in one field, one word in another field:

        @title:"barack obama" @job:president

* Combined _AND_, _OR_ with field specifiers:

        @title:"hello world" @body:(foo bar) @category:(articles|biographies)

* Prefix/infix/suffix queries:

        hello worl*

        hel* *worl

        hello -*worl*

* Wildcard matching queries:

        "w'foo??bar??baz'"

        "w'???????'"

        "w'hello*world'"

* Numeric filtering - products named `tv` with a price range of 200 to 500:

        @name:tv @price:[200 500]

* Numeric filtering - users with age greater than 18:

        @age:[(18 +inf]

## Mapping common SQL predicates to Search and Query

| SQL Condition | Search and Query Equivalent | Comments |
|---------------|-----------------------|----------|
| WHERE x='foo' AND y='bar' | @x:foo @y:bar | for less ambiguity use (@x:foo) (@y:bar) |
| WHERE x='foo' AND y!='bar' | @x:foo -@y:bar |
| WHERE x='foo' OR y='bar' | (@x:foo)\|(@y:bar) |
| WHERE x IN ('foo', 'bar','hello world') | @x:(foo\|bar\|"hello world") | quotes mean exact phrase |
| WHERE y='foo' AND x NOT IN ('foo','bar') | @y:foo (-@x:foo) (-@x:bar) |
| WHERE x NOT IN ('foo','bar') | -@x:(foo\|bar) |
| WHERE num BETWEEN 10 AND 20 | @num:[10 20] |
| WHERE num >= 10 | @num:[10 +inf] |
| WHERE num > 10 | @num:[(10 +inf] |
| WHERE num < 10 | @num:[-inf (10] |
| WHERE num <= 10 | @num:[-inf 10] |
| WHERE num < 10 OR num > 20 | @num:[-inf (10] \| @num:[(20 +inf] |
| WHERE name LIKE 'john%' | @name:john* |

## Technical notes

The query parser is built using the Lemon Parser Generator and a Ragel based lexer. You can see the `dialect 2` grammar definition [at the git repo](https://github.com/RediSearch/RediSearch/blob/master/src/query_parser/v2/parser.y).

You can also see the [DEFAULT_DIALECT](/docs/stack/search/configuring/#default_dialect) configuration parameter.
