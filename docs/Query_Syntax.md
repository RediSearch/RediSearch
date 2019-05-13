# Search Query Syntax

We support a simple syntax for complex queries with the following rules:

* Multi-word phrases simply a list of tokens, e.g. `foo bar baz`, and imply intersection (AND) of the terms.
* Exact phrases are wrapped in quotes, e.g `"hello world"`.
* OR Unions (i.e `word1 OR word2`), are expressed with a pipe (`|`), e.g. `hello|hallo|shalom|hola`.
* NOT negation (i.e. `word1 NOT word2`) of expressions or sub-queries. e.g. `hello -world`. As of version 0.19.3, purely negative queries (i.e. `-foo` or `-@title:(foo|bar)`) are supported.
* Prefix matches (all terms starting with a prefix) are expressed with a `*`. For performance reasons, a minimum prefix length is enforced (2 by default, but is configurable)
* A special "wildcard query" that returns all results in the index - `*` (cannot be combined with anything else).
* Selection of specific fields using the syntax `@field:hello world`.
* Numeric Range matches on numeric fields with the syntax `@field:[{min} {max}]`.
* Geo radius matches on geo fields with the syntax `@field:[{lon} {lat} {radius} {m|km|mi|ft}]`
* Tag field filters with the syntax `@field:{tag | tag | ...}`. See the full documentation on [tag fields|/Tags].
* Optional terms or clauses: `foo ~bar` means bar is optional but documents with bar in them will rank higher.
* Fuzzy matching on terms (as of v1.2.0): `%hello%` means all terms with Levenshtein distance of 1 from it.
* An expression in a query can be wrapped in parentheses to disambiguate, e.g. `(hello|hella) (world|werld)`.
* Query attributes can be applied to individual clauses, e.g. `(foo bar) => { $weight: 2.0; $slop: 1; $inorder: false; }`
* Combinations of the above can be used together, e.g `hello (world|foo) "bar baz" bbbb`

## Pure negative queries

As of version 0.19.3 it is possible to have a query consisting of just a negative expression, e.g. `-hello` or `-(@title:foo|bar)`. The results will be all the documents *NOT* containing the query terms.

!!! warning
    Any complex expression can be negated this way, however, caution should be taken here: if a negative expression has little or no results, this is equivalent to traversing and ranking all the documents in the index, which can be slow and cause high CPU consumption.

## Field modifiers

As of version 0.12 it is possible to specify field modifiers in the query and not just using the INFIELDS global keyword.

Per query expression or sub-expression, it is possible to specify which fields it matches, by prepending the expression with the `@` symbol, the field name and a `:` (colon) symbol.

If a field modifier precedes multiple words, they are considered to be a phrase with the same modifier.

If a field modifier precedes an expression in parentheses, it applies only to the expression inside the parentheses.

Multiple modifiers can be combined to create complex filtering on several fields. For example, if we have an index of car models, with a vehicle class, country of origin and engine type, we can search for SUVs made in Korea with hybrid or diesel engines - with the following query:

```
FT.SEARCH cars "@country:korea @engine:(diesel|hybrid) @class:suv"
```

Multiple modifiers can be applied to the same term or grouped terms. e.g.:

```
FT.SEARCH idx "@title|body:(hello world) @url|image:mydomain"
```

This will search for documents that have "hello world" either in the body or the title, and the term "mydomain" in their url or image fields.

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

As of version 0.21, it is possible to add geo radius queries directly into the query language  with the syntax `@field:[{lon} {lat} {radius} {m|km|mi|ft}]`. This filters the result to a given radius from a lon,lat point, defined in meters, kilometers, miles or feet. See Redis' own [`GEORADIUS`](https://redis.io/commands/georadiys) command for more details as it is used internally for that).

Radius filters can be added into the query just like numeric filters. For example, in a database of businesses, looking for Chinese restaurants near San Francisco (within a 5km radius) would be expressed as: `chinese restaurant @location:[-122.41 37.77 5 km]`.

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

* **$weight**: determines the weight of the sub-query or token in the overall ranking on the result (default: 1.0).
2. **$slop**: determines the maximum allowed "slop" (space between terms) in the query clause (default: 0).
3. **$inorder**: whether or not the terms in a query clause must appear in the same order as in the query, usually set alongside with `$slop` (default: false).
4. **$phonetic**: whether or not to perform phonetic matching (default: true). Note: setting this attribute on for fields which were not creates as `PHONETIC` will produce an error.

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

        @title:hello world @body:(foo bar) @category:(articles|biographies)

* Prefix Queries:

        hello worl*

        hel* worl*

        hello -worl*

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

The query parser is built using the Lemon Parser Generator and a Ragel based lexer. You can see the grammar definition [at the git repo](https://github.com/RediSearch/RediSearch/blob/master/src/query_parser/parser.y).
