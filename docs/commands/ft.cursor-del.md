Delete a cursor.

#### Parameters

* **index**: the index name.
* **cursorId**: the id of the cursor.

@return

@simple-string-reply - `OK` if executed correctly, or @error-reply otherwise.

```
redis> FT.CURSOR DEL idx 342459320
OK
```
```
redis> FT.CURSOR DEL idx 342459320
(error) Cursor does not exist
```