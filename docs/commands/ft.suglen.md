---
syntax: |
  FT.SUGLEN key
---

Get the size of an auto-complete suggestion dictionary

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

is suggestion dictionary key.
</details>

## Return

FT.SUGLEN returns an integer reply, which is the current size of the suggestion dictionary.

## Examples

<details open>
<summary><b>Get the size of an auto-complete suggestion dictionary</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.SUGLEN sug
(integer) 2
{{< / highlight >}}
</details>

## See also

`FT.SUGADD` | `FT.SUGDEL` | `FT.SUGGET` 

## Related topics

[RediSearch](/docs/stack/search)