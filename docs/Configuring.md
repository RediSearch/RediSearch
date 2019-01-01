# Run-time configuration

RediSearch supports a few run-time configuration options that should be determined when loading the module. In time more options will be added. 

## Passing Configuration Options During Loading

In general, passing configuration options is done by appending arguments after the `--loadmodule` argument in the command line, `loadmodule` configuration directive in a Redis config file, or the `MODULE LOAD` command. For example:

In redis.conf:

```
loadmodule redisearch.so OPT1 OPT2
```

From redis-cli:

```
127.0.0.6379> MODULE load redisearch.so OPT1 OPT2
```

From command line:

```
$ redis-server --loadmodule ./redisearch.so OPT1 OPT2
```

## Setting Configuration Options In Run-Time

As of v1.4.1, the [`FT.CONFIG`](Commands.md#ftconfig) allows setting some options during runtime. In addition, the command can be used to view the current run-time configuration options.

# RediSearch configuration options

## TIMEOUT

The maximum amount of time **in milliseconds** that a search query is allowed to run. If this time is exceeded we return the top results accumulated so far, or an error depending on the policy set with `ON_TIMEOUT`. The timeout can be disabled by setting it to 0.

!!! note
    This works only in concurrent mode, so enabling `SAFEMODE` disables this option.

### Default

500

### Example

```
$ redis-server --loadmodule ./redisearch.so TIMEOUT 100
```

---

## ON_TIMEOUT {policy}

The response policy for queries that exceed the `TIMEOUT` setting.

The policy can be one of the following:

* **RETURN**: this policy will return the top results accumulated by the query until it timed out.
* **FAIL**: will return an error when the query exeeds the timeout value.

### Default

RETURN

### Example

```
$ redis-server --loadmodule ./redisearch.so ON_TIMEOUT fail
```

---

## SAFEMODE

If present in the argument list, RediSearch will turn off concurrency for query processing, and work in a single thread.

This is useful if data consistency is extremely important, and avoids a situation where deletion of documents while querying them can cause momentarily inconsistent results (i.e. documents that were valid during the invocation of the query are not returned because they were deleted during query processing).

### Default
Off (not present)

### Example

```
$ redis-server --loadmodule ./redisearch.so SAFEMODE
```

---

## EXTLOAD {file_name}

If present, we try to load a RediSearch extension dynamic library from the specified file path. See [Extensions](Extensions.md) for details.

### Default

None

### Example

```
$ redis-server --loadmodule ./redisearch.so EXTLOAD ./ext/my_extension.so
```

---

## NOGC

If set, we turn off Garbage Collection for all indexes. This is used mainly for debugging and testing, and should not be set by users.

### Default

Not set

### Example

```
$ redis-server --loadmodule ./redisearch.so NOGC
```

---

## MINPREFIX

The minimum number of characters we allow for prefix queries (e.g. `hel*`). Setting it to 1 can hurt performance.

### Default

2

### Example

```
$ redis-server --loadmodule ./redisearch.so MINPREFIX 3
```

---

## MAXEXPANSIONS

The maximum number of expansions we allow for query prefixes. Setting it too high can cause performance issues.

### Default

200

### Example

```
$ redis-server --loadmodule ./redisearch.so MAXEXPANSIONS 1000
```

---

## MAXDOCTABLESIZE

The maximum size of the internal hash table used for storing the documents.

### Default

1000000

### Example

```
$ redis-server --loadmodule ./redisearch.so MAXDOCTABLESIZE 3000000
```

---

## FRISOINI {file_name}

If present, we load the custom Chinese dictionary from the specified path. See [Using custom dictionaries](Chinese.md#using_custom_dictionaries) for more details.

### Default

Not set

### Example

```
$ redis-server --loadmodule ./redisearch.so FRISOINI /opt/dict/friso.ini
```

---

## GC_SCANSIZE 

The garbage collection bulk size of the internal gc used for cleaning up the indexes.

### Default

100

### Example

```
$ redis-server --loadmodule ./redisearch.so GC_SCANSIZE 10
```

---

## GC_POLICY

The policy for the garbage collector. Supported policies are:

* **DEFAULT**: the default policy.
* **FORK**: uses a forked thread for garbage collection (v1.4.1 and above).

!!! warning "The `FORK` garbage collection policy is considered an experimental feature, and should be used responsibly."

### Default

"default"

### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY DEFAULT
```
