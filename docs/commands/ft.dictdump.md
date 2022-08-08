---
syntax: 
---

Dump all terms in the given dictionary

## Syntax

{{< highlight bash >}}
FT.DICTDUMP dict
{{< / highlight >}}

[Examples](#examples)

## Required parameters

<details open>
<summary><code>dict</code></summary>

is dictionary name.
</details>

## Return

FT.DICTDUMP returns an array, where each element is term (string).

## Examples

<details open>
<summary><b>Add terms to a dictionary</b></summary>

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


