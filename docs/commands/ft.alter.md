
### FT.ALTER SCHEMA ADD

#### Format

```
FT.ALTER {index} SCHEMA ADD {attribute} {options} ...
```

#### Description

Adds a new attribute to the index.

Adding an attribute to the index will cause any future document updates to use the new attribute when
indexing and reindexing of existing documents.

{{% alert title="max text fields" color="info" %}}
Depending on how the index was created, you may be limited by the number of additional text
attributes which can be added to an existing index. If the current index contains fewer than 32
text attributes, then `SCHEMA ADD` will only be able to add attributes up to 32 total attributes (meaning that the
index will only ever be able to contain 32 total text attributes). If you wish for the index to
contain more than 32 attributes, create it with the `MAXTEXTFIELDS` option.
{{% /alert %}}

#### Parameters

* **index**: the index name.
* **attribute**: the attribute name.
* **options**: the attribute options - refer to `FT.CREATE` for more information.

@return

@simple-string-reply - `OK` if executed correctly, or @error-reply otherwise.

@examples

```sql
redis> FT.ALTER idx SCHEMA ADD id2 NUMERIC SORTABLE
OK
```