Deletes the index.

By default, FT.DROPINDEX does not delete the document hashes associated with the index. Adding the DD option deletes the hashes as well.

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE
- **DD**: If set, the drop operation will delete the actual document hashes.

@return

@simple-string-reply - `OK` if executed correctly, or @error-reply otherwise.


{{% alert title="Partial FT.DROPINDEX" color="info" %}}
When using FT.DROPINDEX with the parameter DD, if an index creation is still running (FT.CREATE is running asynchronously),
only the document hashes that have already been indexed are deleted. The document hashes left to be indexed will remain in the database.
You can use FT.INFO to check the completion of the indexing.
{{% /alert %}}

@examples

```sql
redis> FT.DROPINDEX idx DD
OK
```