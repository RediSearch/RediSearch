---
syntax: |
  FT.DICTADD dict term [term ...]
---

Add terms to a dictionary

[Examples](#examples)

## Required arguments

<details open>
<summary><code>dict</code></summary>

is dictionary name.
</details>

<details open>
<summary><code>term</code></summary>

term to add to the dictionary.
</details>

## Return

FT.DICTADD returns an integer reply, the number of new terms that were added.

## Examples

<details open>
<summary><b>Add terms to a dictionary</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.DICTADD dict foo bar "hello world"
(integer) 3
{{< / highlight >}}
</details>

## See also

`FT.DICTDEL` | `FT.DICTDUMP`

## Related topics

[RediSearch](/docs/stack/search)