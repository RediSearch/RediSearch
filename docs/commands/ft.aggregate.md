#### Complexity

Non-deterministic. Depends on the query and aggregations performed, but it is usually linear to the number of results returned.

---

Runs a search query on an index, and performs aggregate transformations on the results, extracting statistics etc from them. See [the full documentation on aggregations](/redisearch/reference/aggregations) for further details.

#### Parameters

* **index_name**: The index the query is executed against.

* **query**: The base filtering query that retrieves the documents. It follows
  **the exact same syntax** as the search query, including filters, unions, not, optional, etc.

- **VERBATIM**: if set, we do not try to use stemming for query expansion but search the query terms verbatim.

* **LOAD {nargs} {identifier} AS {property} …**: Load document attributes from the source document.
  `identifier` is either an attribute name (for hashes and JSON) or a JSON Path expression for (JSON).
  `property` is the optional name used in the result. It is not provided, the `identifier` is used.
  This should be avoided as a general rule of thumb.
  If `*` is used as `nargs`, all attributes in a document are loaded.
  Attributes needed for aggregations should be stored as **SORTABLE**,
  where they are available to the aggregation pipeline with very low latency. LOAD hurts the
  performance of aggregate queries considerably, since every processed record needs to execute the
  equivalent of HMGET against a Redis key, which when executed over millions of keys, amounts to very
  high processing times.

* **GROUPBY {nargs} {property}**: Group the results in the pipeline based on one or more properties.
  Each group should have at least one reducer (See below), a function that handles the group entries,
  either counting them, or performing multiple aggregate operations (see below).
    * **REDUCE {func} {nargs} {arg} … [AS {name}]**: Reduce the matching results in each group into a single record, using a reduction function. For example COUNT will count the number of records in the group. See the Reducers section below for more details on available reducers.

          The reducers can have their own property names using the `AS {name}` optional argument. If a name is not given, the resulting name will be the name of the reduce function and the group properties. For example, if a name is not given to COUNT_DISTINCT by property `@foo`, the resulting name will be `count_distinct(@foo)`.

* **SORTBY {nargs} {property} {ASC|DESC} [MAX {num}]**: Sort the pipeline up until the point of SORTBY,
  using a list of properties. By default, sorting is ascending, but `ASC` or `DESC ` can be added for
  each property. `nargs` is the number of sorting parameters, including ASC and DESC. for example:
  `SORTBY 4 @foo ASC @bar DESC`.

  Attributes needed for **SORTBY** should be stored as **SORTABLE** in order to be available with very low latency.

    `MAX` is used to optimized sorting, by sorting only for the n-largest elements. Although it is not connected to `LIMIT`, you usually need just `SORTBY … MAX` for common queries.

* **APPLY {expr} AS {name}**: Apply a 1-to-1 transformation on one or more properties, and either
  store the result as a new property down the pipeline, or replace any property using this
  transformation. `expr` is an expression that can be used to perform arithmetic operations on numeric
  properties, or functions that can be applied on properties depending on their types (see below), or
  any combination thereof. For example: `APPLY "sqrt(@foo)/log(@bar) + 5" AS baz` will evaluate this
  expression dynamically for each record in the pipeline and store the result as a new property called
  baz, that can be referenced by further APPLY / SORTBY / GROUPBY / REDUCE operations down the
  pipeline.

* **LIMIT {offset} {num}**. Limit the number of results to return just `num` results starting at index
  `offset` (zero-based). AS mentioned above, it is much more efficient to use `SORTBY … MAX` if you
  are interested in just limiting the output of a sort operation.
  If a key expires during the query, an attempt to `load` its content, will return a null-array. 

    However, limit can be used to limit results without sorting, or for paging the n-largest results as determined by `SORTBY MAX`. For example, getting results 50-100 of the top 100 results is most efficiently expressed as `SORTBY 1 @foo MAX 100 LIMIT 50 50`. Removing the MAX from SORTBY will result in the pipeline sorting _all_ the records and then paging over results 50-100.

* **FILTER {expr}**. Filter the results using predicate expressions relating to values in each result.
  They are is applied post-query and relate to the current state of the pipeline.

* **TIMEOUT {milliseconds}**: If set, we will override the timeout parameter of the module.

* **PARAMS {nargs} {name} {value}**. Define one or more value parameters. Each parameter has a name and a value. Parameters can be referenced in the **query** by a `$`, followed by the parameter name, e.g., `$user`, and each such reference in the search query to a parameter name is substituted by the corresponding parameter value. For example, with parameter definition `PARAMS 4 lon 29.69465 lat 34.95126`, the expression `@loc:[$lon $lat 10 km]` would be evaluated to `@loc:[29.69465 34.95126 10 km]`. Parameters cannot be referenced in the query string where concrete values are not allowed, such as in field names, e.g., `@loc`. To use `PARAMS`, `DIALECT` must be set to 2.

* **DIALECT {dialect_version}**. Choose the dialect version to execute the query under. If not specified, the query will execute under the default dialect version set during module initial loading or via `FT.CONFIG SET` command.

@return

@array-reply where each row is an @array-reply and represents a single aggregate result.
The @integer-reply at position `1` does not represent a valid value.

@examples

Finding visits to the page "about.html", grouping them by the day of the visit, counting the number of visits, and sorting them by day:

```sql
FT.AGGREGATE idx "@url:\"about.html\""
    APPLY "day(@timestamp)" AS day
    GROUPBY 2 @day @country
      REDUCE count 0 AS num_visits
    SORTBY 4 @day
```

Finding the most books ever published in a single year:

```sql
FT.AGGREGATE books-idx *
    GROUPBY 1 @published_year
      REDUCE COUNT 0 AS num_published
    GROUPBY 0
      REDUCE MAX 1 @num_published AS max_books_published_per_year
```

{{% alert title="Reducing all results" color="info" %}}
The last example used `GROUPBY 0`. Use `GROUPBY 0` to apply a `REDUCE` function over all results from the last step of an aggregation pipeline -- this works on both the  initial query and subsequent `GROUPBY` operations.
{{% /alert %}}

Searching for libraries within 10 kilometers of the longitude -73.982254 and latitude 40.753181 then annotating them with the distance between their location and those coordinates:

```sql
 FT.AGGREGATE libraries-idx "@location:[-73.982254 40.753181 10 km]"
    LOAD 1 @location
    APPLY "geodistance(@location, -73.982254, 40.753181)"
```

Here, we needed to use `LOAD` to pre-load the @location attribute because it is a GEO attribute.

{{% alert title="More examples" color="info" %}}
For more details on aggregations and detailed examples of aggregation queries, see [aggregations](/redisearch/reference/aggregations).
{{% /alert %}}    

Here we are counting GitHub events by user (actor), to produce the most active users:

```sh
127.0.0.1:6379> FT.AGGREGATE gh "*" GROUPBY 1 @actor REDUCE COUNT 0 AS num SORTBY 2 @num DESC MAX 10
 1) (integer) 284784
 2) 1) "actor"
    2) "lombiqbot"
    3) "num"
    4) "22197"
 3) 1) "actor"
    2) "codepipeline-test"
    3) "num"
    4) "17746"
 4) 1) "actor"
    2) "direwolf-github"
    3) "num"
    4) "10683"
 5) 1) "actor"
    2) "ogate"
    3) "num"
    4) "6449"
 6) 1) "actor"
    2) "openlocalizationtest"
    3) "num"
    4) "4759"
 7) 1) "actor"
    2) "digimatic"
    3) "num"
    4) "3809"
 8) 1) "actor"
    2) "gugod"
    3) "num"
    4) "3512"
 9) 1) "actor"
    2) "xdzou"
    3) "num"
    4) "3216"
[10](10)) 1) "actor"
    2) "opstest"
    3) "num"
    4) "2863"
11) 1) "actor"
    2) "jikker"
    3) "num"
    4) "2794"
(0.59s)
```
