---
syntax: |
  FT.SYNDUMP index
---

Dump the contents of a synonym group

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is index name.
</details>

Use FT.SYNDUMP to dump the synonyms data structure. This command returns a list of synonym terms and their synonym group ids.

## Return

FT.SYNDUMP returns an array reply, with a pair of `term` and an array of synonym groups.

## Examples

<details open>
<summary><b>Return the contents of a synonym group</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.SYNDUMP idx
1) "shalom"
2) 1) "synonym1"
   2) "synonym2"
3) "hi"
4) 1) "synonym1"
5) "hello"
6) 1) "synonym1"
{{< / highlight >}}
</details>

## See also

`FT.SYNUPDATE` 

## Related topics

[RediSearch](/docs/stack/search)