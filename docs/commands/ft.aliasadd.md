Add an alias to an index.
This allows an administrator to transparently redirect application queries to alternative indexes.

Indexes can have more than one alias, though an alias cannot refer to another
alias.

@returns

@simple-string-reply - `OK` if executed correctly, or @error-reply otherwise.

@examples

```
redis> FT.ALIASADD alias idx
OK
```
```
redis> FT.ALIASADD alias idx
(error) Alias already exists
```