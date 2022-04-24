Sets runtime configuration options.

#### Parameters

* **option**: the name of the configuration option, or '*' for all.
* **value**: a value for the configuration option.

@return

@simple-string-reply - `OK` if executed correctly, or @error-reply otherwise.

@examples

```
redis> FT.CONFIG SET TIMEOUT 42
OK
```