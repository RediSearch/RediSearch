---
syntax: |
  FT.DICTDEL dict term [term ...]
---

Delete terms from a dictionary

[Examples](#examples)

## Required arguments

<details open>
<summary><code>dict</code></summary>

is dictionary name.
</details>

<details open>
<summary><code>term</code></summary>

term to delete from the dictionary.
</details>

## Return

FT.DICTDEL returns an integer reply, the number of new terms that were deleted.

## Examples

<details open>
<summary><b>Delete terms from a dictionary</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.DICTDEL dict foo bar "hello world"
(integer) 3
{{< / highlight >}}
</details>

## See also

`FT.DICTADD` | `FT.DICTDUMP`

## Related topics

[RediSearch](/docs/stack/search)
