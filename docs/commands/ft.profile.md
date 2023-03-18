---
syntax: |
  FT.PROFILE index SEARCH | AGGREGATE [LIMITED] QUERY query
---

Apply `FT.SEARCH` or `FT.AGGREGATE` command to collect performance details

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is index name, created using `FT.CREATE`.
</details>

<details open>
<summary><code>SEARCH | AGGREGATE</code></summary>

is difference between `FT.SEARCH` and `FT.AGGREGATE`.
</details>

<details open>
<summary><code>LIMITED</code></summary>

removes details of `reader` iterator.
</details>

<details open>
<summary><code>QUERY {query}</code></summary>

is query string, sent to `FT.SEARCH`.
</details>

<note><b>Note:</b> To reduce the size of the output, use `NOCONTENT` or `LIMIT 0 0` to reduce the reply results or `LIMITED` to not reply with details of `reader iterators` inside built-in unions such as `fuzzy` or `prefix`.</note>

## Return

`FT.PROFILE` returns an array reply, with the first array reply identical to the reply of `FT.SEARCH` and `FT.AGGREGATE` and a second array reply with information of time in milliseconds (ms) used to create the query and time and count of calls of iterators and result-processors.

Return value has an array with two elements:

- Results - The normal reply from RediSearch, similar to a cursor.
- Profile - The details in the profile are:
  - Total profile time - The total runtime of the query, in ms.
  - Parsing time - Parsing time of the query and parameters into an execution plan, in ms.
  - Pipeline creation time - Creation time of execution plan including iterators,
  result processors, and reducers creation, in ms.
  - Iterators profile - Index iterators information including their type, term, count, and time data.
  Inverted-index iterators have in addition the number of elements they contain. Hybrid vector iterators returning the top results from the vector index in batches, include the number of batches.
  - Result processors profile - Result processors chain with type, count, and time data.

## Examples

<details open>
<summary><b>Collect performance information about an index</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.PROFILE idx SEARCH QUERY "hello world"
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
{{< / highlight >}}
</details>

## See also

`FT.SEARCH` | `FT.AGGREGATE` 

## Related topics

[RediSearch](/docs/stack/search)

