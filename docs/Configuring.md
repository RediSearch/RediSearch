# Run-Time Configuration

RediSearch supports a few run-time configuration options that should be determined when loading the module. In time more options will be added. 

!!! tip "Passing Configuration Options"
    In general, passing configuration options is done by appending arguments after the `--loadmodule` argument in command line, `loadmodule` configurtion directive in a redis config file, or `MODULE LOAD` when loading modules in command line. For example:
    
    In redis.conf:

    ```
    loadmodule redisearch.so OPT1 OPT2
    ```

    In redis-cli:

    ```
    127.0.0.6379> MODULE load redisearch.so OPT1 OPT2
    ```

    In command-line:

    ```
    $ redis-server --loadmodule ./redisearch.so OPT1 OPT2
    ```

# RediSearch Configuration Options

## SAFEMODE

If present in the argument list, RediSearch will turn off concurrency for query processing, and work in a single thread.

This is useful if data consistency is extremely important, and avoids a situation where deletion of documents while querying them can cause momentarily incosistent results (i.e. documents that were valid during the the invokation of the query are not returned because they were deleted durin query processing).

### Default:
Off (not present)

### Example

```
$ redis-server --loadmodule ./redisearch.so SAFEMODE
```

---

## EXTLOAD {file_name}

If present, we try to load a redisearch extension dynamic library from the specified file path. See [Extensions](/Extensions) for details.

### Default:

None

### Example:

```
$ redis-server --loadmodule ./redisearch.so EXTLOAD ./ext/my_extension.so
```

---

## NOGC

If set, we turn off Garbage Collection for all indexes. This is used mainly for debugging and testing, and should not be set by users.

### Default:

Not set

### Example:

```
$ redis-server --loadmodule ./redisearch.so NOGC
```