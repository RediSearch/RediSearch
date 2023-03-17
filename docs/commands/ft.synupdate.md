---
syntax: |
  FT.SYNUPDATE index synonym_group_id 
    [SKIPINITIALSCAN] term [term ...]
---

Update a synonym group

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is index name.
</details>

<details open>
<summary><code>synonym_group_id</code></summary>

is synonym group to return.
</details>

Use FT.SYNUPDATE to create or update a synonym group with additional terms. The command triggers a scan of all documents.

## Optional parameters

<details open>
<summary><code>SKIPINITIALSCAN</code></summary>

does not scan and index, and only documents that are indexed after the update are affected.
</details>

## Return

FT.SYNUPDATE returns a simple string reply `OK` if executed correctly, or an error reply otherwise.

## Examples

<details open>
<summary><b>Update a synonym group</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.SYNUPDATE idx synonym hello hi shalom
OK
{{< / highlight >}}

{{< highlight bash >}}
127.0.0.1:6379> FT.SYNUPDATE idx synonym SKIPINITIALSCAN hello hi shalom
OK
{{< / highlight >}}
</details>

## See also

`FT.SYNDUMP` 

## Related topics

[RediSearch](/docs/stack/search)