---
syntax: |
  FT.ALIASUPDATE alias index
---

Add an alias to an index. If the alias is already associated with another
index, FT.ALIASUPDATE removes the alias association with the previous index.

[Examples](#examples)

## Required arguments

<details open>
<summary><code>alias index</code></summary>

is alias to be added to an index.
</details>

## Return

FT.ALIASUPDATE returns a simple string reply `OK` if executed correctly, or an error reply otherwise.

## Examples

<details open>
<summary><b>Update an index alias</b></summary>

Update the alias of an index.

{{< highlight bash >}}
127.0.0.1:6379> FT.ALIASUPDATE alias idx
OK
{{< / highlight >}}

## See also

`FT.ALIASADD` | `FT.ALIASDEL` 

## Related topics

[RediSearch](/docs/stack/search)