---
syntax: |
  FT.DICTDUMP dict
---

Dump all terms in the given dictionary

[Examples](#examples)

## Required argumemts

<details open>
<summary><code>dict</code></summary>

is dictionary name.
</details>

## Return

FT.DICTDUMP returns an array, where each element is term (string).

## Examples

<details open>
<summary><b>Dump all terms in the dictionary</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.DICTDUMP dict
1) "foo"
2) "bar"
3) "hello world"
{{< / highlight >}}
</details>

## See also

`FT.DICTADD` | `FT.DICTDEL`

## Related topics

[RediSearch](/docs/stack/search)


