Reads next results from an existing cursor.

#### Parameters

* **index**: the index name.
* **cursorId**: the id of the cursor.
* **readSize**: number of result to read. This parameters override the `COUNT` specified in `FT.AGGREGATE`.

@return

@array-reply where each row is an @array-reply and represents a single aggregate result.

```
redis> FT.CURSOR READ idx 342459320 COUNT 50
```