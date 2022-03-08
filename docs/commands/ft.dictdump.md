Dumps all terms in the given dictionary.

#### Parameters

* **dict**: the dictionary name.

@return

Returns an array, where each element is term (string).

@examples

```
redis> FT.DICTDUMP dict
1) "foo"
2) "bar"
3) "hello world"
```