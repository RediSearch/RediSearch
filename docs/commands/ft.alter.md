---
syntax: |
  FT.ALTER {index} [SKIPINITIALSCAN] SCHEMA ADD {attribute} {options} ...
---

Add a new attribute to the index. Adding an attribute to the index causes any future document updates to use the new attribute when indexing and reindexing existing documents.

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary> 

is index name to create. 
</details>

<details open>
<summary><code>SKIPINITIALSCAN</code></summary> 

if set, does not scan and index.
</details>

<details open>
<summary><code>SCHEMA ADD {attribute} {options} ...</code></summary>

after the SCHEMA keyword, declares which fields to add:

- `attribute` is attribute to add.
- `options` are attribute options. Refer to `FT.CREATE` for more information.

<note><b>Note:</b>

Depending on how the index was created, you may be limited by the number of additional text
attributes which can be added to an existing index. If the current index contains fewer than 32
text attributes, then `SCHEMA ADD` will only be able to add attributes up to 32 total attributes (meaning that the
index will only ever be able to contain 32 total text attributes). If you wish for the index to
contain more than 32 attributes, create it with the `MAXTEXTFIELDS` option.
</note>
</details>

## Return

FT.CREATE returns a simple string reply `OK` if executed correctly, or an error reply otherwise.

## Examples

<details open>
<summary><b>Alter an index</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.ALTER idx SCHEMA ADD id2 NUMERIC SORTABLE
OK
{{< / highlight >}}
</details>

## See also

`FT.CREATE` 

## Related topics

- [RediSearch](/docs/stack/search)



