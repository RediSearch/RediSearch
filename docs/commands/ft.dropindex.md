---
syntax: |
  FT.DROPINDEX index 
    [DD]
---

Delete an index

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is full-text index name. You must first create the index using `FT.CREATE`.
</details>

## Optional arguments

<details open>
<summary><code>DD</code></summary>

drop operation that, if set, deletes the actual document hashes.

By default, FT.DROPINDEX does not delete the documents associated with the index. Adding the `DD` option deletes the documents as well. 
If an index creation is still running (`FT.CREATE` is running asynchronously), only the document hashes that have already been indexed are deleted. 
The document hashes left to be indexed remain in the database.
To check the completion of the indexing, use `FT.INFO`.

</details>

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

