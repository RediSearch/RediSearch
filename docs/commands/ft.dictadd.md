Adds terms to a dictionary.

#### Parameters

* **dict**: the dictionary name.

* **term**: the term to add to the dictionary.

@return

@integer-reply - the number of new terms that were added.

@examples

```
redis> FT.DICTADD dict foo bar "hello world"
(integer) 3
```