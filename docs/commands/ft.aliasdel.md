---
syntax: 
---

Remove an alias from an index

## Syntax

{{< highlight bash >}}
FT.ALIASDEL alias
{{< / highlight >}}

[Examples](#examples)

## Required parameters

<details open>
<summary><code>alias</code></summary>

is index alias to be removed.
</details>

## Returns

FT.ALIASSADD returns a simple string reply `OK` if executed correctly, or an error reply otherwise.

## Examples

<details open>
<summary><b>Remove an alias from an index</b></summary>

Remove an alias from an index.

{{< highlight bash >}}
127.0.0.1:6379> FT.ALIASDEL alias
OK
{{< / highlight >}}
</details>

## See also

`FT.ALIASADD` | `FT.ALIASUPDATE` 

## Related topics

[RediSearch](/docs/stack/search)