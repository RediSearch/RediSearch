---
title: "Query syntax"
linkTitle: "Query syntax"
weight: 1
description: >
    Details of the query syntax
---

# Search Query Syntax

We support a simple syntax for complex queries with the following rules:

* Multi-word phrases simply a list of tokens, e.g. `foo bar baz`, and imply intersection (AND) of the terms.
* Exact phrases are wrapped in quotes, e.g `"hello world"`.
* OR Unions (i.e `word1 OR word2`), are expressed with a pipe (`|`), e.g. `hello|hallo|shalom|hola`.
* NOT negation (i.e. `word1 NOT word2`) of expressions or sub-queries. e.g. `hello -world`. As of version 0.19.3, purely negative queries (i.e. `-foo` or `-@title:(foo|bar)`) are supported.
* Prefix/Infix/Suffix matches (all terms starting/containing/ending with a term) are expressed with a `*`. For performance reasons, a minimum term length is enforced (2 by default, but is configurable).
* Wildcard pattern matches: `w'foo*bar?'`.
* A special "wildcard query" that returns all results in the index - `*` (cannot be combined with anything else).
* Selection of specific fields using the syntax `hello @field:world`.
* Numeric Range matches on numeric fields with the syntax `@field:[{min} {max}]`.
* Geo radius matches on geo fields with the syntax `@field:[{lon} {lat} {radius} {m|km|mi|ft}]`.
* Range queries on vector fields (as of v2.6) with the syntax `@field:[VECTOR_RANGE {radius} $query_vec]`, where `query_vec` is given as a query parameter.
* KNN queries on vector fields with or without pre-filtering (as of v2.4) with the syntax `{filter_query}=>[KNN {num} @field $query_vec]`.
* Tag field filters with the syntax `@field:{tag | tag | ...}`. See the full documentation on [tag fields|/Tags].
* Optional terms or clauses: `foo ~bar` means bar is optional but documents with bar in them will rank higher.
* Fuzzy matching on terms (as of v1.2.0): `%hello%` means all terms with Levenshtein distance of 1 from it.
* An expression in a query can be wrapped in parentheses to disambiguate, e.g. `(hello|hella) (world|werld)`.
* Query attributes can be applied to individual clauses, e.g. `(foo bar) => { $weight: 2.0; $slop: 1; $inorder: false; }`.
* Combinations of the above can be used together, e.g `hello (world|foo) "bar baz" bbbb`.

## Pure negative queries

As of version 0.19.3 it is possible to have a query consisting of just a negative expression, e.g. `-hello` or `-(@title:(foo|bar))`. The results will be all the documents *NOT* containing the query terms.

{{% alert title="Warning" color="warning" %}}
Any complex expression can be negated this way, however, caution should be taken here: if a negative expression has little or no results, this is equivalent to traversing and ranking all the documents in the index, which can be slow and cause high CPU consumption.
{{% /alert %}}

## Field modifiers

As of version 0.12 it is possible to specify field modifiers in the query and not just using the INFIELDS global keyword.

Per query expression or sub-expression, it is possible to specify which fields it matches, by prepending the expression with the `@` symbol, the field name and a `:` (colon) symbol.

If a field modifier precedes multiple words or expressions, it applies **only** to the adjacent expression.

If a field modifier precedes an expression in parentheses, it applies only to the expression inside the parentheses. The expression should be valid for the specified field, otherwise it is skipped.

Multiple modifiers can be combined to create complex filtering on several fields. For example, if we have an index of car models, with a vehicle class, country of origin and engine type, we can search for SUVs made in Korea with hybrid or diesel engines - with the following query:

```
FT.SEARCH cars "@country:korea @engine:(diesel|hybrid) @class:suv"
```

Multiple modifiers can be applied to the same term or grouped terms. e.g.:

```
FT.SEARCH idx "@title|body:(hello world) @url|image:mydomain"
```

This will search for documents that have "hello" and "world" either in the body or the title, and the term "mydomain" in their url or image fields.

## Numeric filters in query

If a field in the schema is defined as NUMERIC, it is possible to either use the FILTER argument in the Redis request or filter with it by specifying filtering rules in the query. The syntax is `@field:[{min} {max}]` - e.g. `@price:[100 200]`.

### A few notes on numeric predicates

1. It is possible to specify a numeric predicate as the entire query, whereas it is impossible to do it with the FILTER argument.

2. It is possible to intersect or union multiple numeric filters in the same query, be it for the same field or different ones.

3. `-inf`, `inf` and `+inf` are acceptable numbers in a range. Thus greater-than 100 is expressed as `[(100 inf]`.

4. Numeric filters are inclusive. Exclusive min or max are expressed with `(` prepended to the number, e.g. `[(100 (200]`.

5. It is possible to negate a numeric filter by prepending a `-` sign to the filter, e.g. returning a result where price differs from 100 is expressed as: `@title:foo -@price:[100 100]`.

## Tag filters

RediSearch (starting with version 0.91) allows a special field type called "tag field", with simpler tokenization and encoding in the index. The values in these fields cannot be accessed by general field-less search, and can be used only with a special syntax:

```
@field:{ tag | tag | ...}

e.g.

@cities:{ New York | Los Angeles | Barcelona }
```

Tags can have multiple words or include other punctuation marks other than the field's separator (`,` by default). Punctuation marks in tags should be escaped with a backslash (`\`). It is also recommended (but not mandatory) to escape spaces; The reason is that if a multi-word tag includes stopwords, it will create a syntax error. So tags like "to be or not to be" should be escaped as "to\ be\ or\ not\ to\ be". For good measure, you can escape all spaces within tags.

Notice that multiple tags in the same clause create a union of documents containing either tags. To create an intersection of documents containing *all* tags, you should repeat the tag filter several times, e.g.:

```
# This will return all documents containing all three cities as tags:
@cities:{ New York } @cities:{Los Angeles} @cities:{ Barcelona }

# This will return all documents containing either city:
@cities:{ New York | Los Angeles | Barcelona }
```

Tag clauses can be combined into any sub-clause, used as negative expressions, optional expressions, etc.

## Geo filters in query

As of version 0.21, it is possible to add geo radius queries directly into the query language  with the syntax `@field:[{lon} {lat} {radius} {m|km|mi|ft}]`. This filters the result to a given radius from a lon,lat point, defined in meters, kilometers, miles or feet. See Redis' own `GEORADIUS` command for more details as it is used internally for that).

Radius filters can be added into the query just like numeric filters. For example, in a database of businesses, looking for Chinese restaurants near San Francisco (within a 5km radius) would be expressed as: `chinese restaurant @location:[-122.41 37.77 5 km]`.

## Vector Similarity search in query

It is possible to add vector similarity queries directly into the query language. There are two ways of performing vector similarity search:
1. Using a **range** query (as of v2.6.1) with the syntax of `@vector:[VECTOR_RANGE {radius} $query_vec]`, which filters the results to a given radius from a given query vector. The distance metric derives from the definition of @vector field in the index schema (e.g., Cosine, L2).


2. By running a **KNN** (K Nearest Neighbors) query on @vector field. The basic syntax is `"*=>[ KNN {num|$num} @vector $query_vec ]"`.
It is also possible to run a hybrid query on filtered results. A hybrid query allows the user to specify a filter criteria that all results in a KNN query must satisfy. The filter criteria can include any type of field (e.g., indexes created on both vectors and other values such as TEXT, PHONETIC, NUMERIC, GEO, etc.).
The general syntax for hybrid query is `{some filter query}=>[ KNN {num|$num} @vector $query_vec]`, where `=>` separates the filter query from the vector KNN query. 


**Examples:**
* `*=>[KNN 10 @vector_field $query_vec]` - Return 10 nearest neighbors entities in which `query_vec` is closest to the vector stored in `@vector_field`.
* `@published_year:[2020 2022]=>[KNN 10 @vector_field $query_vec]` - Among entities published between 2020 and 2022, return 10 "nearest neighbors" entities in which `query_vec` is closest to the vector stored in `@vector_field`.
* `@vector_field:[VECTOR_RANGE 0.5 $query_vec]` - Return every entity for which the distance between the vector stored under its @vector_field and `query_vec` is at most 0.5, in terms of @vector_field distance metric.


As of version 2.4, the KNN vector search can be used **once** in the query, while, as of version 2.6, the vector range filter can be used **multiple** times in a query. For more information on vector similarity syntax, see [Querying vector fields](/docs/stack/search/reference/vectors/#querying-vector-fields), and [Vector search examples](/docs/stack/search/reference/reference/vectors/#vector-search-examples) sections.

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

## Infix/Suffix matching

Since version v2.6.0, the dictionary can be used for infix (contains) or suffix queries by appending `*` to the token. For example:

```
*sun* *ing 
```

These queries are CPU intensive because they require iteration over the whole dictionary.

Note: all notes about prefix searches also apply to infix/suffix queries.

### Using a suffix trie

A suffix trie maintains a list of terms which match the suffix. If you add a suffix trie to a field using the `WITHSUFFIXTRIE` keyword, you can create more efficient infix and suffix queries because it eliminates the need to iterate over the whole dictionary. However, the iteration on the union does not change. 

Suffix queries create a union of the list of terms from the suffix term node. Infix queries use the suffix terms as prefixes to the trie and create a union of all terms from all matching nodes.

## Wildcard matching

Since version v2.6.0, the dictionary can be used for wildcard matching queries with 2 parameters.

* `?` - for any single character
* `*` - for any character repeating zero or more times

An example of the syntax is `w'foo*bar?'`.

### Using a Suffix Trie

A suffix trie maintains a list of terms which match the suffix. If you add a suffix trie to a field using the `WITHSUFFIXTRIE` keyword, you can create more efficient wildcard matching queries because it eliminates the need to iterate over the whole dictionary. However, the iteration on the union does not change. 

With a suffix trie, the wildcard pattern is broken into tokens at every `*` character. A heuristic is used to choose the token with the least terms, and each term is matched with the wildcard pattern.

## Fuzzy matching

As of v1.2.0, the dictionary of all terms in the index can also be used to perform [Fuzzy Matching](https://en.wikipedia.org/wiki/Approximate_string_matching). Fuzzy matches are performed based on [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) (LD). Fuzzy matching on a term is performed by surrounding the term with '%', for example:

```
%hello% world
```

Will perform fuzzy matching on 'hello' for all terms where LD is 1.

As of v1.4.0, the LD of the fuzzy match can be set by the number of '%' surrounding it, so that `%%hello%%` will perform fuzzy matching on 'hello' for all terms where LD is 2.

The maximal LD for fuzzy matching is 3.

## Wildcard queries

As of version 1.1.0, we provide a special query to retrieve all the documents in an index. This is meant mostly for the aggregation engine. You can call it by specifying only a single star sign as the query string - i.e. `FT.SEARCH myIndex *`.

This cannot be combined with any other filters, field modifiers or anything inside the query. It is technically possible to use the deprecated FILTER and GEOFILTER request parameters outside the query string in conjunction with a wildcard, but this makes the wildcard meaningless and only hurts performance.

## Query attributes

As of version 1.2.0, it is possible to apply specific query modifying attributes to specific clauses of the query.

The syntax is `(foo bar) => { $attribute: value; $attribute:value; ...}`, e.g:

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

* Simple phrase query - hello AND world

        hello world

* Exact phrase query - **hello** FOLLOWED BY **world**

        "hello world"

* Union: documents containing either **hello** OR **world**

        hello|world

* Not: documents containing **hello** but not **world**

        hello -world

* Intersection of unions

        (hello|halo) (world|werld)

* Negation of union

        hello -(world|werld)

* Union inside phrase

        (barack|barrack) obama

* Optional terms with higher priority to ones containing more matches:

        obama ~barack ~michelle

* Exact phrase in one field, one word in another field:

        @title:"barack obama" @job:president

* Combined AND, OR with field specifiers:

        @title:"hello world" @body:(foo bar) @category:(articles|biographies)

* Prefix/Infix/Suffix queries:

        hello worl*

        hel* *worl

        hello -*worl*

* Wildcard matching queries:

        w'foo??bar??baz'

        w'???????'

        w'hello*world'

* Numeric Filtering - products named "tv" with a price range of 200-500:

        @name:tv @price:[200 500]

* Numeric Filtering - users with age greater than 18:

        @age:[(18 +inf]

## Mapping common SQL predicates to RediSearch

| SQL Condition | RediSearch Equivalent | Comments |
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

## Technical note

The query parser is built using the Lemon Parser Generator and a Ragel based lexer. You can see the `dialect 2` grammar definition [at the git repo](https://github.com/RediSearch/RediSearch/blob/master/src/query_parser/v2/parser.y).

You can also see the [DEFAULT_DIALECT](/docs/stack/search/configuring/#default_dialect) configuration parameter.
