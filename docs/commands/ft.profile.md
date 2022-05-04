Performs a `FT.SEARCH` or `FT.AGGREGATE` command and collects performance information.
Return value has an array with two elements:

  * **Results** - The normal reply from RediSearch, similar to a cursor.
  * **Profile** - The details in the profile are:
    * **Total profile time** - The total runtime of the query.
    * **Parsing time** - Parsing time of the query and parameters into an execution plan.
    * **Pipeline creation time** - Creation time of execution plan including iterators,
  result processors and reducers creation.
    * **Iterators profile** - Index iterators information including their type, term, count and time data.
  Inverted-index iterators have in addition the number of elements they contain. Hybrid vector iterators returning the top results from the vector index in batches, include the number of batches.
    * **Result processors profile** - Result processors chain with type, count and time data.

#### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **SEARCH,AGGREGATE**: Differ between `FT.SEARCH` and `FT.AGGREGATE`
- **LIMITED**: Removes details of `reader` iterator
- **QUERY {query}**: The query string, as if sent to FT.SEARCH

@return

@array-reply - with the first @array-reply identical to the reply of FT.SEARCH and FT.AGGREGATE and a second @array-reply with information of time used to create the query and time and count of calls of iterators and result-processors.

!!! tip
    To reduce the size of the output, use `NOCONTENT` or `LIMIT 0 0` to reduce results reply or `LIMITED` to not reply with details of `reader iterators` inside builtin-unions such as `fuzzy` or `prefix`.


@examples

```sh
FT.PROFILE idx SEARCH QUERY "hello world"
1) 1) (integer) 1
   2) "doc1"
   3) 1) "t"
      2) "hello world"
2) 1) 1) Total profile time
      2) "0.47199999999999998"
   2) 1) Parsing time
      2) "0.218"
   3) 1) Pipeline creation time
      2) "0.032000000000000001"
   4) 1) Iterators profile
      2) 1) Type
         2) INTERSECT
         3) Time
         4) "0.025000000000000001"
         5) Counter
         6) (integer) 1
         7) Child iterators
         8)  1) Type
             2) TEXT
             3) Term
             4) hello
             5) Time
             6) "0.0070000000000000001"
             7) Counter
             8) (integer) 1
             9) Size
            10) (integer) 1
         9)  1) Type
             2) TEXT
             3) Term
             4) world
             5) Time
             6) "0.0030000000000000001"
             7) Counter
             8) (integer) 1
             9) Size
            10) (integer) 1
   5) 1) Result processors profile
      2) 1) Type
         2) Index
         3) Time
         4) "0.036999999999999998"
         5) Counter
         6) (integer) 1
      3) 1) Type
         2) Scorer
         3) Time
         4) "0.025000000000000001"
         5) Counter
         6) (integer) 1
      4) 1) Type
         2) Sorter
         3) Time
         4) "0.013999999999999999"
         5) Counter
         6) (integer) 1
      5) 1) Type
         2) Loader
         3) Time
         4) "0.10299999999999999"
         5) Counter
         6) (integer) 1
```