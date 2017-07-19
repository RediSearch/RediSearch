# Search Query Syntax

RediSearch supports a simple syntax for complex queries, adhering to the following rules:

* Multi-word phrases are simply a list of tokens, e.g. `foo bar baz`, and imply intersection (AND) of the terms
* Exact phrases are wrapped in quotes, e.g `"hello world"`
* OR Unions (i.e `word1 OR word2`) are expressed with a pipe (`|`), e.g. `hello|hallo|shalom|hola`
* NOT negation of expressions or sub-queries (i.e. `word1 NOT word2`) are expressed with a minus sign (`-`), e.g. `hello -world`
* Prefix matches (all terms starting with a prefix) are expressed with a `*` following a prefix of three or more letters
* Selection of specific fields uses the syntax `@field:hello world`
* Matching numeric range on numeric fields uses the syntax `@field:[{min} {max}]`
* Optional terms or clauses are prefixed with a tilde ('~'), e.g. `foo ~bar` means "bar" is optional but documents with "bar" in them will rank higher
* An expression in a query can be wrapped in parentheses to resolve ambiguity, e.g. `(hello|hella) (world|werld)`
* Combinations of the above can be used together, e.g `hello (world|foo) "bar baz" bbbb`

## Field Modifiers

As of version 0.12 it is possible to specify field modifiers in the query, and not just using the INFIELDS global keyword. 

It is possible to specify which fields a query expression or sub expression matches by prepending the expression with the `@` symbol, the field name and a `:` (colon) symbol. 

If a field modifier precedes multiple words, those words are treated as a phrase with the same modifier. 

If a field modifier precedes an expression in parentheses, it modifies only to the expression inside the parentheses.

Multiple modifiers can be combined to create complex filtering on several fields. For example, if we have an index of car models, with a vehicle class, country of origin and engine type, we can search for SUVs made in Korea with hybrid or diesel engines - with the following query:

```
FT.SEARCH cars "@country:korea @engine:(diesel|hybrid) @class:suv"
```

Multiple modifiers can be applied to the same term or grouped terms, e.g.:

```
FT.SEARCH idx "@title|body:(hello world) @url|image:mydomain"
```

This will search for documents that have "hello world" either in the body or the title, and the term "mydomain" in their url or image fields.

## Numeric Filters in Query (Since v0.16)

If a field in the schema is defined as NUMERIC, it is possible to filter by that field using either the FILTER argument in the Redis request or specified filtering rules in the query. The syntax is `@field:[{min} {max}]` e.g. `@price:[100 200]`.

### A few Notes on Numeric Predicates:

1. It is possible to specify a numeric predicate as the entire query, whereas it is impossible to do so with the FILTER argument.

2. It is possible to interesect or unionize multiple numeric filters in the same query, be it for the same field or different ones.

3. `-inf`, `inf` and `+inf` are acceptable numbers in range. Thus "greater than 100" is expressed as `[(100 inf]`.

4. Numeric filters are inclusive. Exclusive minimums or maximums are expressed by prepending `(` to the number, e.g. `[(100 (200]`.

5. It is possible to negate a numeric filter by prepending a minus sign (`-`) to the filter. For example, returning a result where price is not "100" is expressed as: `@title:foo -@price:[100 100]`. However a boolean-negative numeric filter cannot be the only predicate in the query.

## Prefix Matching (>= v0.14)

Upon index updating, RediSearch maintains a dictionary of all terms in the index. This can be used to match all terms that start with a specified prefix. Selecting prefix matches is done by appending `*` to a prefix token. For example:

```
hel* world
```

Will be expanded to cover `(hello|help|helm|...) world`. 



### A few notes on Prefix Searches:

1. As prefixes can be expanded into many many terms, so use them with caution. There is no magic going on--the expansion will create a union operation of all suffixes!

2. As a protective measure to avoid selecting too many terms and thereby blocking Redis, which is single-threaded, there are two limitations on prefix matching:

  * Prefixes are limited to three letters or more

  * Expansion is limited to 200 terms or less

3. Prefix matching fully supports unicode and is case insensitive.

4. Currently there is no sorting or bias based on suffix popularity, but this is on the near-term roadmap. 



## A Few Query Examples

* Simple phrase query - hello AND world

        hello world

* Exact phrase query - **hello** FOLLOWED BY **world**

        "hello world"

* Union - documents containing either **hello** OR **world**

        hello|world

* Not - documents containing **hello** but not **world**

        hello -world

* Intersection of unions

        (hello|halo) (world|werld)

* Negation of a union

        hello -(world|werld)

* Union inside phrase

        (barack|barrack) obama

* Optional terms that raise priority 

        obama ~barack ~michelle

* Exact phrase in one field, one word in another field

        @title:"barack obama" @job:president

* Combined AND, OR with field specifiers

        @title:hello world @body:(foo bar) @category:(articles|biographies)

* Prefix queries

        hello worl*

        hel* worl*

        hello -worl*

* Numeric filtering - products named "tv" with a price range of 200-500
        
        @name:tv @price:[200 500]

* Numeric filtering - users with age greater than 18

        @age:[(18 +inf]


### Technical Note

The query parser is built using the Lemon Parser Generator and a Ragel based lexer. You can see the grammar definition [at the git repo.](https://github.com/RedisLabsModules/RediSearch/blob/master/src/query_parser/parser.y)
