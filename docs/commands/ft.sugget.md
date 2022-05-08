Gets completion suggestions for a prefix.

#### Parameters

- **key**: the suggestion dictionary key.
- **prefix**: the prefix to complete on
- **FUZZY**: if set, we do a fuzzy prefix search, including prefixes at Levenshtein distance of 1 from
  the prefix sent
- **MAX num**: If set, we limit the results to a maximum of `num` (default: 5).
- **WITHSCORES**: If set, we also return the score of each suggestion. this can be used to merge
  results from multiple instances
- **WITHPAYLOADS**: If set, we return optional payloads saved along with the suggestions. If no
  payload is present for an entry, we return a Null Reply.

#### Returns

@array-reply: a list of the top suggestions matching the prefix, optionally with score after each entry.

@examples

```sql
redis> FT.SUGGET sug hell FUZZY MAX 3 WITHSCORES
1) "hell"
2) "2147483648"
3) "hello"
4) "0.70710676908493042"
```
