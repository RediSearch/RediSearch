# Search Query Syntax:

We support a simple syntax for complex queries with the following rules:

* Multi-word phrases simply a list of tokens, e.g. `foo bar baz`, and imply intersection (AND) of the terms.
* Exact phrases are wrapped in quotes, e.g `"hello world"`.
* OR Unions (i.e `word1 OR word2`), are expressed with a pipe (`|`), e.g. `hello|hallo|shalom|hola`.
* Selection of specific fields using the syntax `@field:hello world`.
* An expression in a query can be wrapped in parentheses to resolve disambiguity, e.g. `(hello|hella) (world|werld)`.
* Combinations of the above can be used together, e.g `hello (world|foo) "bar baz" bbbb`

## Field modifiers

As of version 0.12 it is possible to specify field modifiers in the query and not just using the INFIELDS global keyword. 

Per query expression or sub expression, it is possible to specify which fields it matches, by prepending the experssion with the `@` symbol, the field name and a `:` (colon) symbol. 

If a field modifier precedes multiple words, they are considered to be a phrase with the same modifier. 

If a field modifier preceds an expression in parentheses, it applies only to the expression inside the parentheses.

Multiple modifiers can be combined to create complex filtering on several fields. For example, if we have an index of car models, with a vehicle class, country of origin and engine type, we can search for SUVs made in Korea with hybrid or diesel engines - with the following query:

```
FT.SEARCH cars "@country:korea @engine:(diesel|hybrid) @class:suv"
```

Multiple modifiers can be applied to the same term or grouped terms. e.g.:

```
FT.SEARCH idx "@title|body:(hello world) @url|image:mydomain"
```

This will search for documents that have "hello world" either in the body or the title, and the term "mydomain" in their url or image fields.

## A few examples


* Simple phrase query - hello AND world

        hello world

* Exact phrase query - **hello** FOLLOWED BY **world**

        "hello world"

* Union: documents containing either **hello** OR **world**

        hello|world

* Intersection of unions

        (hello|halo) (world|werld)

* Union inside phrase

        (barack|barrack) obama

* Exact phrase in one field, one word in aonther field:

        @title:"barack obama" @job:president

* Combined AND, OR with field specifiers:

        @title:hello world @body:(foo bar) @category:(articles|biographies)


### Technical Note

The query parser is built using the Lemon Parser Generator. You can see the grammar definition [at the git repo.](https://github.com/RedisLabsModules/RediSearch/blob/master/src/query_parser/parser.y)
