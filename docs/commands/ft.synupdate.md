Updates a synonym group.

The command is used to create or update a synonym group with additional terms. The command triggers a scan of all documents.

#### Parameters

* **SKIPINITIALSCAN**: If set, we do not scan and index and only documents which were indexed after the update will be affected.

@return

@simple-string-reply - `OK` if executed correctly, or @error-reply otherwise.

@examples

```
redis> FT.SYNUPDATE idx synonym hello hi shalom
OK
```
```
redis> FT.SYNUPDATE idx synonym SKIPINITIALSCAN hello hi shalom
OK
```