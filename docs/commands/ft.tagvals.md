Returns the distinct set of values indexed in a Tag field.

This is useful if your tag indexes things like cities, categories, etc.

{{% alert title="Limitations" color="warning" %}}
There is no paging or sorting, the tags are not alphabetically sorted.
This command only operates on [Tag fields](/redisearch/reference/tags).
The strings return lower-cased and stripped of whitespaces, but otherwise unchanged.
{{% /alert %}}

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **filed_name**: The name of a Tag file defined in the schema.

@return

@array-reply of all the distinct tags in the tag index.

@examples

```sql
FT.TAGVALS idx myTag
1) "Hello"
2) "World"
```