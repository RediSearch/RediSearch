Adds a suggestion string to an auto-complete suggestion dictionary. This is disconnected from the
index definitions, and leaves creating and updating suggestions dictionaries to the user.

#### Parameters

- **key**: the suggestion dictionary key.
- **string**: the suggestion string we index
- **score**: a floating point number of the suggestion string's weight
- **INCR**: if set, we increment the existing entry of the suggestion by the given score, instead of
  replacing the score. This is useful for updating the dictionary based on user queries in real time
- **PAYLOAD {payload}**: If set, we save an extra payload with the suggestion, that can be fetched by
  adding the `WITHPAYLOADS` argument to `FT.SUGGET`.

@returns

@integer-reply: the current size of the suggestion dictionary.

@examples

```sql
FT.SUGADD sug "hello world" 1
(integer) 3
```