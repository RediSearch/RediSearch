---
syntax: |
  FT.TAGVALS index field_name
---

Return a distinct set of values indexed in a Tag field

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is full-text index name. You must first create the index using `FT.CREATE`.
</details>

<details open>
<summary><code>field_name</code></summary>

is name of a Tag file defined in the schema.
</details>

Use FT.TAGVALS if your tag indexes things like cities, categories, and so on.

## Limitations

FT.TAGVALS provides no paging or sorting, and the tags are not alphabetically sorted. FT.TAGVALS only operates on [tag fields](/docs/stack/search/reference/tags).
The returned strings are lowercase with whitespaces removed, but otherwise unchanged.

## Return

FT.TAGVALS returns an array reply of all distinct tags in the tag index.

## Examples

<details open>
<summary><b>Return a set of values indexed in a Tag field</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.TAGVALS idx myTag
1) "Hello"
2) "World"
{{< / highlight >}}
</details>

## See also

`FT.CREATE` 

## Related topics

- [Tag fields](/docs/stack/search/reference/tags)
- [RediSearch](/docs/stack/search)
