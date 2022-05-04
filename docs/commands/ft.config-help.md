Describes configuration options.

#### Parameters

* **option**: the name of the configuration option, or '*' for all.

@return

@array-reply of the configuration name and description and value.

@examples

```
redis> FT.CONFIG HELP TIMEOUT
1) 1) TIMEOUT
   2) Description
   3) Query (search) timeout
   4) Value
   5) "42"
```