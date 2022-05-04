Add an alias an index. If the alias is already associated with another
index, FT.ALIASUPDATE will remove the alias association with the previous index.

@return

@simple-string-reply - `OK` if executed correctly, or @error-reply otherwise.

```
redis> FT.ALIASADD alias idx
OK
```
```
redis> FT.ALIASADD alias idx
OK
```