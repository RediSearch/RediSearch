---
syntax: |
  FT.SUGDEL key string
---

Delete a string from a suggestion index

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

is suggestion dictionary key.
</details>

<details open>
<summary><code>string</code></summary> 

is suggestion string to index.
</details>

## Return

FT.SUGDEL returns an integer reply, 1 if the string was found and deleted, 0 otherwise.

## Examples

<details open>
<summary><b>Delete a string from a suggestion index</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.SUGDEL sug "hello"
(integer) 1
127.0.0.1:6379> FT.SUGDEL sug "hello"
(integer) 0
{{< / highlight >}}
</details>

## See also

`FT.SUGGET` | `FT.SUGADD` | `FT.SUGLEN` 

## Related topics

[RediSearch](/docs/stack/search)