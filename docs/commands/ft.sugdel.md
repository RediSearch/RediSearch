Deletes a string from a suggestion index.

#### Parameters

- **key**: the suggestion dictionary key.
- **string**: the string to delete

#### Returns

@integer-reply: 1 if the string was found and deleted, 0 otherwise.

@examples

```sql
redis> FT.SUGDEL sug "hello"
(integer) 1
redis> FT.SUGDEL sug "hello"
(integer) 0
```