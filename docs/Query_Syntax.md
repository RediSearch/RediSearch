# Search Query Syntax:

We support a simple syntax for complex queries with the following rules:

* Multi-word phrases simply a list of tokens, e.g. `foo bar baz`, and imply intersection (AND) of the terms.
* Exact phrases are wrapped in quotes, e.g `"hello world"`.
* OR Unions (i.e `word1 OR word2`), are expressed with a pipe (`|`), e.g. `hello|hallo|shalom|hola`.
* An expression in a query can be wrapped in parentheses to resolve disambiguity, e.g. `(hello|hella) (world|werld)`.
* Combinations of the above can be used together, e.g `hello (world|foo) "bar baz" bbbb`

## A few examples


* Simple phrase query - hello AND world
  	hello world

* Exact phrase query - **hello** FOLLOWED BY **world**
  	"hello world"

* Union: documents containing either **hello** OR **world**
  	hello|world

* Intersection of unions
  	(hello\|halo) (world\|werld)

* Union inside phrase
  	(barack\|barrack) obama

### Technical Note

The query parser is built using the Lemon Parser Generator. You can see the grammar definition [at the git repo.](https://github.com/RedisLabsModules/RediSearch/blob/master/src/query_parser/parser.y)