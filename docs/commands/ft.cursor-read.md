---
syntax: |
  FT.CURSOR READ index cursor_id [COUNT read_size]
---

Read next results from an existing cursor

[Examples](#examples)

See [Cursor API](/docs/stack/search/reference/aggregations/#cursor-api) for more details.

## Required arguments

<details open>
<summary><code>index</code></summary>

is index name.
</details>

<details open>
<summary><code>cursor_id</code></summary>

is id of the cursor.
</details>

<details open>
<summary><code>[COUNT read_size]</code></summary>

is number of results to read. This parameter overrides `COUNT` specified in `FT.AGGREGATE`.
</details>

## Return

FT.CURSOR READ returns an array reply where each row is an array reply and represents a single aggregate result.

## Examples

<details open>
<summary><b>Read next results from a cursor</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.CURSOR READ idx 342459320 COUNT 50
{{< / highlight >}}
</details>

## See also

`FT.CURSOR DEL` | `FT.AGGREGATE`

## Related topics

[RediSearch](/docs/stack/search)
