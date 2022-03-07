
### FT.DICTDEL

#### Format
```
  FT.DICTDEL {dict} {term} [{term} ...]
```

#### Description

Deletes terms from a dictionary.

#### Parameters

* **dict**: the dictionary name.

* **term**: the term to delete from the dictionary.

@return

@integer-reply - the number of terms that were deleted.


```
redis> FT.DICTDEL dict foo bar "hello world"
(integer) 3
```