---
syntax: 
---

Delete an index

## Syntax

{{< highlight bash >}}
FT.DROPINDEX index 
          [ DD]
{{< / highlight >}}

[Examples](#examples)

## Required parameters

<details open>
<summary><code>index</code></summary>

is full-text index name. You must first create the index using `FT.CREATE`.
</details>

## Optional parameters

<details open>
<summary><code>DD</code></summary>

drop operation that, if set, deletes the actual document hashes.
</details>

<note><b>Notes:</b> 

- By default, FT.DROPINDEX does not delete the document hashes associated with the index. Adding the DD option deletes the hashes as well.
- When using FT.DROPINDEX with the parameter `DD`, if an index creation is still running (`FT.CREATE` is running asynchronously),
only the document hashes that have already been indexed are deleted. The document hashes left to be indexed remain in the database.
You can use FT.INFO to check the completion of the indexing.
</note>

## Return

FT.DROPINDEX returns a simple string reply `OK` if executed correctly, or an error reply otherwise.

## Examples

<details open>
<summary><b>Delete an index</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.DROPINDEX idx DD
OK
{{< / highlight >}}
</details>

## See also

`FT.CREATE` | `FT.INFO`

## Related topics

[RediSearch](/docs/stack/search)

