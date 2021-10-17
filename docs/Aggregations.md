# RediSearch Aggregations

Aggregations are a way to process the results of a search query, group, sort and transform them - and extract analytic insights from them. Much like aggregation queries in other databases and search engines, they can be used to create analytics reports, or perform [Faceted Search](https://en.wikipedia.org/wiki/Faceted_search) style queries. 

For example, indexing a web-server's logs, we can create a report for unique users by hour, country or any other breakdown; or create different reports for errors, warnings, etc. 

## Core concepts

The basic idea of an aggregate query is this:

* Perform a search query, filtering for records you wish to process.
* Build a pipeline of operations that transform the results by zero or more steps of:
  * **Group and Reduce**: grouping by fields in the results, and applying reducer functions on each group.
  * **Sort**: sort the results based on one or more fields.
  * **Apply Transformations**: Apply mathematical and string functions on fields in the pipeline, optionally creating new fields or replacing existing ones
  * **Limit**: Limit the result, regardless of sorting the result. 
  * **Filter**: Filter the results (post-query) based on predicates relating to its values. 

The pipeline is dynamic and re-entrant, and every operation can be repeated. For example, you can group by property X, sort the top 100 results by group size, then group by property Y and sort the results by some other property, then apply a transformation on the output. 

Figure 1: Aggregation Pipeline Example
![Aggregation Pipeline](https://docs.google.com/drawings/d/e/2PACX-1vRFyP17ingsG86OYNaienojHHA8DwnlVVv67-WlKxv7a7xTJCluWvs3SzXYQSS6QqwB9QZ1vqDuoJ-0/pub?w=518&h=163)

## Aggregate request format

The aggregate request's syntax is defined as follows:

```sql
FT.AGGREGATE
  {index_name:string}
  {query_string:string}
  [VERBATIM]
  [LOAD {nargs:integer} {property:string} ...]
  [GROUPBY
    {nargs:integer} {property:string} ...
    REDUCE
      {FUNC:string}
      {nargs:integer} {arg:string} ...
      [AS {name:string}]
    ...
  ] ...
  [SORTBY
    {nargs:integer} {string} ...
    [MAX {num:integer}] ...
  ] ...
  [APPLY
    {EXPR:string}
    AS {name:string}
  ] ...
  [FILTER {EXPR:string}] ...
  [LIMIT {offset:integer} {num:integer} ] ...
```

#### Parameters in detail

Parameters which may take a variable number of arguments are expressed in the
form of `param {nargs} {property_1... property_N}`. The first argument to the
parameter is the number of arguments following the parameter. This allows
RediSearch to avoid a parsing ambiguity in case one of your arguments has the
name of another parameter. For example, to sort by first name, last name, and
country, one would specify `SORTBY 6 firstName ASC lastName DESC country ASC`.

* **index_name**: The index the query is executed against.

* **query_string**: The base filtering query that retrieves the documents. It follows **the exact same syntax** as the search query, including filters, unions, not, optional, etc.

* **LOAD {nargs} {property} …**: Load document fields from the document HASH objects. This should be avoided as a general rule of thumb. Fields needed for aggregations should be stored as SORTABLE (and optionally UNF to avoid any normalization), where they are available to the aggregation pipeline with very low latency. LOAD hurts the performance of aggregate queries considerably since every processed record needs to execute the equivalent of HMGET against a redis key, which when executed over millions of keys, amounts to very high processing times.
The document ID can be loaded using `@__key`.

* **GROUPBY {nargs} {property}**: Group the results in the pipeline based on one or more properties. Each group should have at least one reducer (See below), a function that handles the group entries, either counting them or performing multiple aggregate operations (see below).
  
* **REDUCE {func} {nargs} {arg} … [AS {name}]**: Reduce the matching results in each group into a single record, using a reduction function. For example, COUNT will count the number of records in the group. See the Reducers section below for more details on available reducers.

    The reducers can have their own property names using the `AS {name}` optional argument. If a name is not given, the resulting name will be the name of the reduce function and the group properties. For example, if a name is not given to COUNT_DISTINCT by property `@foo`, the resulting name will be `count_distinct(@foo)`. 

* **SORTBY {nargs} {property} {ASC|DESC} [MAX {num}]**: Sort the pipeline up until the point of SORTBY, using a list of properties. By default, sorting is ascending, but `ASC` or `DESC ` can be added for each property. `nargs` is the number of sorting parameters, including ASC and DESC. for example: `SORTBY 4 @foo ASC @bar DESC`. 

    `MAX` is used to optimized sorting, by sorting only for the n-largest elements. Although it is not connected to `LIMIT`, you usually need just `SORTBY … MAX` for common queries. 

* **APPLY {expr} AS {name}**: Apply a 1-to-1 transformation on one or more properties, and either store the result as a new property down the pipeline, or replace any property using this transformation. `expr` is an expression that can be used to perform arithmetic operations on numeric properties, or functions that can be applied on properties depending on their types (see below), or any combination thereof. For example: `APPLY "sqrt(@foo)/log(@bar) + 5" AS baz` will evaluate this expression dynamically for each record in the pipeline and store the result as a new property called baz, that can be referenced by further APPLY / SORTBY / GROUPBY / REDUCE operations down the pipeline. 

* **LIMIT {offset} {num}**. Limit the number of results to return just `num` results starting at index `offset` (zero based). AS mentioned above, it is much more efficient to use `SORTBY … MAX` if you are interested in just limiting the output of a sort operation.

     However, limit can be used to limit results without sorting, or for paging the n-largest results as determined by `SORTBY MAX`. For example, getting results 50-100 of the top 100 results is most efficiently expressed as `SORTBY 1 @foo MAX 100 LIMIT 50 50`. Removing the MAX from SORTBY will result in the pipeline sorting _all_ the records and then paging over results 50-100. 

* **FILTER {expr}**. Filter the results using predicate expressions relating to values in each result. They are is applied post-query and relate to the current state of the pipeline. See FILTER Expressions below for full details.

## Quick example

Let's assume we have log of visits to our website, each record containing the following fields/properties:

* **url** (text, sortable)
* **timestamp** (numeric, sortable) - unix timestamp of visit entry. 
* **country** (tag, sortable)
* **user_id** (text, sortable, not indexed)

### Example 1: unique users by hour, ordered chronologically.

First of all, we want _all_ records in the index, because why not. The first step is to determine the index name and the filtering query. A filter query of `*` means "get all records":

```
FT.AGGREGATE myIndex "*"
```

Now we want to group the results by hour. Since we have the visit times as unix timestamps in second resolution, we need to extract the hour component of the timestamp. So we first add an APPLY step, that strips the sub-hour information from the timestamp and stores is as a new property, `hour`:

```
FT.AGGREGATE myIndex "*"
  APPLY "@timestamp - (@timestamp % 3600)" AS hour
```

Now we want to group the results by hour, and count the distinct user ids in each hour. This is done by a GROUPBY/REDUCE  step:

```
FT.AGGREGATE myIndex "*"
  APPLY "@timestamp - (@timestamp % 3600)" AS hour
  
  GROUPBY 1 @hour
  	REDUCE COUNT_DISTINCT 1 @user_id AS num_users
```

Now we'd like to sort the results by hour, ascending:

```
FT.AGGREGATE myIndex "*"
  APPLY "@timestamp - (@timestamp % 3600)" AS hour
  
  GROUPBY 1 @hour
  	REDUCE COUNT_DISTINCT 1 @user_id AS num_users
  	
  SORTBY 2 @hour ASC
```

And as a final step, we can format the hour as a human readable timestamp. This is done by calling the transformation function `timefmt` that formats unix timestamps. You can specify a format to be passed to the system's `strftime` function ([see documentation](http://strftime.org/)), but not specifying one  is equivalent to specifying `%FT%TZ` to `strftime`.

```
FT.AGGREGATE myIndex "*"
  APPLY "@timestamp - (@timestamp % 3600)" AS hour
  
  GROUPBY 1 @hour
  	REDUCE COUNT_DISTINCT 1 @user_id AS num_users
  	
  SORTBY 2 @hour ASC
  
  APPLY timefmt(@hour) AS hour
```

### Example 2: Sort visits to a specific URL by day and country:

In this example we filter by the url, transform the timestamp to its day part, and group by the day and country, simply counting the number of visits per group. sorting by day ascending and country descending. 

```
FT.AGGREGATE myIndex "@url:\"about.html\""
    APPLY "@timestamp - (@timestamp % 86400)" AS day
    GROUPBY 2 @day @country
    	REDUCE count 0 AS num_visits 
    SORTBY 4 @day ASC @country DESC
```

## GROUPBY reducers

`GROUPBY` step work similarly to SQL `GROUP BY` clauses, and create groups of results based on one or more properties in each record. For each group, we return the "group keys", or the values common to all records in the group, by which they were grouped together - along with the results of zero or more `REDUCE` clauses.

Each `GROUPBY` step in the pipeline may be accompanied by zero or more `REDUCE` clauses. Reducers apply some accumulation function to each record in the group and reduce them into a single record representing the group. When we are finished processing all the records upstream of the `GROUPBY` step, each group emits its reduced record. 

For example, the simplest reducer is COUNT, which simply counts the number of records in each group. 

If multiple `REDUCE` clauses exist for a single `GROUPBY` step, each reducer works independently on each result and writes its final output once. Each reducer may have its own alias determined using the `AS` optional parameter. If `AS` is not specified, the alias is the reduce function and its parameters, e.g. `count_distinct(foo,bar)`.

### Supported GROUPBY reducers

#### COUNT

**Format**

```
REDUCE COUNT 0
```

**Description**

Count the number of records in each group 

#### COUNT_DISTINCT

**Format**

````
REDUCE COUNT_DISTINCT 1 {property}
````

**Description**

Count the number of distinct values for `property`. 

!!! note
    The reducer creates a hash-set per group, and hashes each record. This can be memory heavy if the groups are big.

#### COUNT_DISTINCTISH

**Format** 

```
REDUCE COUNT_DISTINCTISH 1 {property}
```

**Description**

Same as COUNT_DISTINCT - but provide an approximation instead of an exact count, at the expense of less memory and CPU in big groups. 

!!! note
    The reducer uses [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) counters per group, at ~3% error rate, and 1024 Bytes of constant space allocation per group. This means it is ideal for few huge groups and not ideal for many small groups. In the former case, it can be an order of magnitude faster and consume much less memory than COUNT_DISTINCT, but again, it does not fit every user case. 

#### SUM

**Format**

```
REDUCE SUM 1 {property}
```

**Description**

Return the sum of all numeric values of a given property in a group. Non numeric values if the group are counted as 0.

#### MIN

**Format**

```
REDUCE MIN 1 {property}
```

**Description**

Return the minimal value of a property, whether it is a string, number or NULL.

#### MAX

**Format**

```
REDUCE MAX 1 {property}
```

**Description**

Return the maximal value of a property, whether it is a string, number or NULL.

#### AVG

**Format**

```
REDUCE AVG 1 {property}
```

**Description**

Return the average value of a numeric property. This is equivalent to reducing by sum and count, and later on applying the ratio of them as an APPLY step.

#### STDDEV

**Format**

```
REDUCE STDDEV 1 {property}
```

**Description**

Return the [standard deviation](https://en.wikipedia.org/wiki/Standard_deviation) of a numeric property in the group.

#### QUANTILE

**Format**

```
REDUCE QUANTILE 2 {property} {quantile}
```

**Description**

Return the value of a numeric property at a given quantile of the results. Quantile is expressed as a number between 0 and 1. For example, the median can be expressed as the quantile at 0.5, e.g. `REDUCE QUANTILE 2 @foo 0.5 AS median` .

If multiple quantiles are required, just repeat  the QUANTILE reducer for each quantile. e.g. `REDUCE QUANTILE 2 @foo 0.5 AS median REDUCE QUANTILE 2 @foo 0.99 AS p99` 

#### TOLIST

**Format**

```
REDUCE TOLIST 1 {property}
```

**Description**

Merge all **distinct** values of a given property into a single array. 

#### FIRST_VALUE

**Format**

```
REDUCE FIRST_VALUE {nargs} {property} [BY {property} [ASC|DESC]]
```

**Description**

Return the first or top value of a given property in the group, optionally by comparing that or another property. For example, you can extract the name of the oldest user in the group:

```
REDUCE FIRST_VALUE 4 @name BY @age DESC
```

If no `BY` is specified, we return the first value we encounter in the group.

If you with to get the top or bottom value in the group sorted by the same value, you are better off using the `MIN/MAX` reducers, but the same effect will be achieved by doing `REDUCE FIRST_VALUE 4 @foo BY @foo DESC`.

#### RANDOM_SAMPLE

**Format**

```
REDUCE RANDOM_SAMPLE {nargs} {property} {sample_size}
```

**Description**

Perform a reservoir sampling of the group elements with a given size, and return an array of the sampled items with an even distribution.

## APPLY expressions

`APPLY` performs a 1-to-1 transformation on one or more properties in each record. It either stores the result as a new property down the pipeline, or replaces any property using this transformation. 

The transformations are expressed as a combination of arithmetic expressions and built in functions. Evaluating functions and expressions is recursively nested and can be composed without limit. For example: `sqrt(log(foo) * floor(@bar/baz)) + (3^@qaz % 6)` or simply `@foo/@bar`.

If an expression or a function is applied to values that do not match the expected types, no error is emitted but a NULL value is set as the result. 

APPLY steps must have an explicit alias determined by the `AS` parameter.

### Literals inside expressions

* Numbers are expressed as integers or floating point numbers, i.e. `2`, `3.141`, `-34`, etc. `inf` and `-inf` are acceptable as well.
* Strings are quoted with either single or double quotes. Single quotes are acceptable inside strings quoted with double quotes and vice versa. Punctuation marks can be escaped with backslashes. e.g. `"foo's bar"` ,`'foo\'s bar'`, `"foo \"bar\""` .
* Any literal or sub expression can be wrapped in parentheses to resolve ambiguities of operator precedence.

### Arithmetic operations

For numeric expressions and properties, we support addition (`+`), subtraction (`-`), multiplication (`*`), division (`/`), modulo (`%`) and power (`^`). We currently do not support bitwise logical operators.  

Note that these operators apply only to numeric values and numeric sub expressions. Any attempt to multiply a string by a number, for instance, will result in a NULL output.

### List of field APPLY functions

| Function | Description                                                  | Example            |
| -------- | ------------------------------------------------------------ | ------------------ |
| exists(s)| Checks whether a field exists in a document.                 | `exists(@field)`   |

### List of numeric APPLY functions

| Function | Description                                                  | Example            |
| -------- | ------------------------------------------------------------ | ------------------ |
| log(x)   | Return the logarithm of a number, property or sub-expression | `log(@foo)`        |
| abs(x)   | Return the absolute number of a numeric expression           | `abs(@foo-@bar)`   |
| ceil(x)  | Round to the smallest value not less than x                  | `ceil(@foo/3.14)`  |
| floor(x) | Round to largest value not greater than x                    | `floor(@foo/3.14)` |
| log2(x)  | Return the  logarithm of x to base 2                         | `log2(2^@foo)`     |
| exp(x)   | Return the exponent of x, i.e. `e^x`                         | `exp(@foo)`        |
| sqrt(x)  | Return the square root of x                                  | `sqrt(@foo)`       |

### List of string APPLY functions

| Function                         |                                                              |                                                          |
| -------------------------------- | ------------------------------------------------------------ | -------------------------------------------------------- |
| upper(s)                         | Return the uppercase conversion of s                         | `upper('hello world')`                                   |
| lower(s)                         | Return the lowercase conversion of s                         | `lower("HELLO WORLD")`                                   |
| startswith(s1,s2)                | Return `1` if s2 is the prefix of s1, `0` otherwise.         | `startswith(@field, "company")`                          |
| contains(s1,s2)                  | Return the number of occurrences of s2 in s1, `0` otherwise.  | `contains(@field, "pa")`                                 |
| substr(s, offset, count)         | Return the substring of s, starting at _offset_ and having _count_ characters. <br />If offset is negative, it represents the distance from the end of the string. <br />If count is -1, it means "the rest of the string starting at offset". | `substr("hello", 0, 3)` <br> `substr("hello", -2, -1)`  |
| format( fmt, ...)                | Use the arguments following `fmt` to format a string. <br />Currently the only format argument supported is `%s` and it applies to all types of arguments. | `format("Hello, %s, you are %s years old", @name, @age)` |
| matched_terms([max_terms=100])   | Return the query terms that matched for each record (up to 100), as a list. If a limit is specified, we will return the first N matches we find - based on query order. | `matched_terms()`                                        |
| split(s, [sep=","], [strip=" "]) | Split a string by any character in the string sep, and strip any characters in strip. If only s is specified, we split by commas and strip spaces. The output is an array. | split("foo,bar")                                         |

### List of date/time APPLY functions

| Function            | Description                                                  |
| ------------------- | ------------------------------------------------------------ |
| timefmt(x, [fmt])      | Return a formatted time string based on a numeric timestamp value x. <br /> See [strftime](http://strftime.org/) for formatting options. <br />Not specifying `fmt` is equivalent to `%FT%TZ`. |
| parsetime(timesharing, [fmt]) | The opposite of timefmt() - parse a time format using a given format string |
| day(timestamp) | Round a Unix timestamp to midnight (00:00) start of the current day. |
| hour(timestamp) | Round a Unix timestamp to the beginning of the current hour. |
| minute(timestamp) | Round a Unix timestamp to the beginning of the current minute. |
| month(timestamp) | Round a unix timestamp to the beginning of the current month. |
| dayofweek(timestamp) | Convert a Unix timestamp to the day number (Sunday = 0). |
| dayofmonth(timestamp) | Convert a Unix timestamp to the day of month number (1 .. 31). |
| dayofyear(timestamp) | Convert a Unix timestamp to the day of year number (0 .. 365). |
| year(timestamp) | Convert a Unix timestamp to the current year (e.g. 2018). |
| monthofyear(timestamp) | Convert a Unix timestamp to the current month (0 .. 11). |

### List of geo APPLY functions

| Function | Description                                                  | Example            |
| -------- | ------------------------------------------------------------ | ------------------ |
| geodistance(field,field)        | Return distance in meters.    | `geodistance(@field1,@field2)`       |
| geodistance(field,"lon,lat")    | Return distance in meters.    | `geodistance(@field,"1.2,-3.4")`     |
| geodistance(field,lon,lat)      | Return distance in meters.    | `geodistance(@field,1.2,-3.4)`       |
| geodistance("lon,lat",field)    | Return distance in meters.    | `geodistance("1.2,-3.4",@field)`     |
| geodistance("lon,lat","lon,lat")| Return distance in meters.    | `geodistance("1.2,-3.4","5.6,-7.8")` |
| geodistance("lon,lat",lon,lat)  | Return distance in meters.    | `geodistance("1.2,-3.4",5.6,-7.8)`   |
| geodistance(lon,lat,field)      | Return distance in meters.    | `geodistance(1.2,-3.4,@field)`       |
| geodistance(lon,lat,"lon,lat")  | Return distance in meters.    | `geodistance(1.2,-3.4,"5.6,-7.8")`   |
| geodistance(lon,lat,lon,lat)    | Return distance in meters.    | `geodistance(1.2,-3.4,5.6,-7.8)`     |

```
FT.AGGREGATE myIdx "*"  LOAD 1 location  APPLY "geodistance(@location,\"-1.1,2.2\")" AS dist
```

To print out the distance:

```
FT.AGGREGATE myIdx "*"  LOAD 1 location  APPLY "geodistance(@location,\"-1.1,2.2\")" AS dist
```

**Note:** Geo field must be preloaded using `LOAD`.

Results can also be sorted by distance:

```
FT.AGGREGATE idx "*" LOAD 1 @location FILTER "exists(@location)" APPLY "geodistance(@location,-117.824722,33.68590)" AS dist SORTBY 2 @dist DESC
```

**Note:** Make sure no location is missing, otherwise the SORTBY will not return any result.
Use FILTER to make sure you do the sorting on all valid locations.

## FILTER expressions

FILTER expressions filter the results using predicates relating to values in the result set.

The FILTER expressions are evaluated post-query and relate to the current state of the pipeline. Thus they can be useful to prune the results based on group calculations. Note that the filters are not indexed and will not speed the processing per se. 

Filter expressions follow the syntax of APPLY expressions, with the addition of the conditions `==`, `!=`, `<`, `<=`, `>`, `>=`. Two or more predicates can be combined with logical AND (`&&`) and OR (`||`). A single predicate can be negated with a NOT prefix (`!`).  

For example, filtering all results where the user name is 'foo' and the age is less than 20 is expressed  as:

```
FT.AGGREGATE 
  ...
  FILTER "@name=='foo' && @age < 20"
  ...
```

Several filter steps can be added, although at the same stage in the pipeline, it is more efficient to combine several predicates into a single filter step.

## Cursor API

```
FT.AGGREGATE ... WITHCURSOR [COUNT {read size} MAXIDLE {idle timeout}]
FT.CURSOR READ {idx} {cid} [COUNT {read size}]
FT.CURSOR DEL {idx} {cid}
```

You can use cursors with `FT.AGGREGATE`, with the `WITHCURSOR` keyword. Cursors allow you to
consume only part of the response, allowing you to fetch additional results as needed.
This is much quicker than using `LIMIT` with offset, since the query is executed only
once, and its state is stored on the server.

To use cursors, specify the `WITHCURSOR` keyword in `FT.AGGREGATE`, e.g.

```
FT.AGGREGATE idx * WITHCURSOR
```

This will return a response of an array with two elements. The first element is
the actual (partial) results, and the second is the cursor ID. The cursor ID
can then be fed to `FT.CURSOR READ` repeatedly, until the cursor ID is 0, in
which case all results have been returned.

To read from an existing cursor, use `FT.CURSOR READ`, e.g.

```
FT.CURSOR READ idx 342459320
```

Assuming `342459320` is the cursor ID returned from the `FT.AGGREGATE` request.

Here is an example in pseudo-code:

```
response, cursor = FT.AGGREGATE "idx" "redis" "WITHCURSOR";
while (1) {
  processResponse(response)
  if (!cursor) {
    break;
  }
  response, cursor = FT.CURSOR read "idx" cursor
}
```

Note that even if the cursor is 0, a partial result may still be returned.

### Cursor settings

#### Read size

You can control how many rows are read per each cursor fetch by using the
`COUNT` parameter. This parameter can be specified both in `FT.AGGREGATE`
(immediately after `WITHCURSOR`) or in `FT.CURSOR READ`.

```
FT.AGGREGATE idx query WITHCURSOR COUNT 10
```

Will read 10 rows at a time.

You can override this setting by also specifying `COUNT` in `CURSOR READ`, e.g.

```
FT.CURSOR READ idx 342459320 COUNT 50
```

Will return at most 50 results.

The default read size is 1000


#### Timeouts and limits

Because cursors are stateful resources which occupy memory on the server, they
have a limited lifetime. In order to safeguard against orphaned/stale cursors,
cursors have an idle timeout value. If no activity occurs on the cursor before
the idle timeout, the cursor is deleted. The idle timer resets to 0 whenever
the cursor is read from using `CURSOR READ`.

The default idle timeout is 300000 milliseconds (or 300 seconds). You can modify
the idle timeout using the `MAXIDLE` keyword when creating the cursor. Note that
the value cannot exceed the default 300s.

```
FT.AGGREGATE idx query WITHCURSOR MAXIDLE 10000
```

Will set the limit for 10 seconds.

### Other cursor commands

Cursors can be explicitly deleted using the `CURSOR DEL` command, e.g.

```
FT.CURSOR DEL idx 342459320
```

Note that cursors are automatically deleted if all their results have been
returned, or if they have been timed out.

All idle cursors can be forcefully purged at once using `FT.CURSOR GC idx 0` command.
By default, RediSearch uses a lazy throttled approach to garbage collection, which
collects idle cursors every 500 operations, or every second - whichever is later.
