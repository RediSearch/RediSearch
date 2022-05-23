Dumps the contents of a synonym group.

The command is used to dump the synonyms data structure. Returns a list of synonym terms and their synonym group ids.

@return

@array-reply - with pair of `term` and an array of synonym groups.

@examples

```
127.0.0.1:6379> FT.SYNDUMP idx
1) "shalom"
2) 1) "synonym1"
   2) "synonym2"
3) "hi"
4) 1) "synonym1"
5) "hello"
6) 1) "synonym1"
```