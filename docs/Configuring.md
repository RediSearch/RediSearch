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

## Setting Configuration Options At Run-Time

As of v1.4.1, the [`FT.CONFIG`](Commands.md#ftconfig) allows setting some options during runtime. In addition, the command can be used to view the current run-time configuration options.

# RediSearch configuration options

## TIMEOUT

The maximum amount of time **in milliseconds** that a search query is allowed to run. If this time is exceeded we return the top results accumulated so far, or an error depending on the policy set with `ON_TIMEOUT`. The timeout can be disabled by setting it to 0.

!!! note
    Timeout refers to query time only. 
    Parsing the query is not counted towards `timeout`. 
    If timeout was not reached during the search, finalizing operation such as loading documents' content or reducers, continue. 

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
* **FAIL**: will return an error when the query exceeds the timeout value.

### Default

RETURN

### Example

```
$ redis-server --loadmodule ./redisearch.so ON_TIMEOUT fail
```

---

## SAFEMODE

!! Deprecated in v1.6.  From this version, SAFEMODE is the default.  If you still like to re-enable the concurrent mode for writes, use [CONCURRENT_WRITE_MODE](#CONCURRENT_WRITE_MODE) !!

If present in the argument list, RediSearch will turn off concurrency for query processing, and work in a single thread.

This is useful if data consistency is extremely important, and avoids a situation where deletion of documents while querying them can cause momentarily inconsistent results (i.e. documents that were valid during the invocation of the query are not returned because they were deleted during query processing).

### Default
Off (not present)

### Example

```
$ redis-server --loadmodule ./redisearch.so SAFEMODE
```

### Notes

* deprecated in v1.6

## CONCURRENT_WRITE_MODE

If enabled, write queries will be performed concurrently. For now only the tokenization part is executed concurrently. The actual write operation still requires holding the Redis Global Lock.

### Default

Not set - "disabled"

### Example

```
$ redis-server --loadmodule ./redisearch.so CONCURRENT_WRITE_MODE
```

### Notes

* added in v1.6

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

## MINPREFIX

The minimum number of characters we allow for prefix queries (e.g. `hel*`). Setting it to 1 can hurt performance.

### Default

2

### Example

```
$ redis-server --loadmodule ./redisearch.so MINPREFIX 3
```

---

## MAXPREFIXEXPANSIONS

The maximum number of expansions we allow for query prefixes. Setting it too high can cause performance issues. If MAXPREFIXEXPANSIONS is reached, the query will continue with the first acquired results.

### Default

200

### Example

```
$ redis-server --loadmodule ./redisearch.so MAXPREFIXEXPANSIONS 1000
```
!!! Note "MAXPREFIXEXPANSIONS replaces the deprecated config word MAXEXPANSIONS."
    
    RediSearch considers these two configurations as synonyms.  The synonym was added to be more descriptive.


---

## MAXDOCTABLESIZE

The maximum size of the internal hash table used for storing the documents. 
Notice, this configuration doesn't limit the amount of documents that can be stored but only the hash table internal array max size.
Decreasing this property can decrease the memory overhead in case the index holds a small amount of documents that are constantly updated.

### Default

1000000

### Example

```
$ redis-server --loadmodule ./redisearch.so MAXDOCTABLESIZE 3000000
```

---

## MAXSEARCHRESULTS

The maximum number of results to be returned by FT.SEARCH command if LIMIT is used.
Setting value to `-1` will remove the limit. 

### Default

1000000

### Example

```
$ redis-server --loadmodule ./redisearch.so MAXSEARCHRESULTS 3000000
```

---

## MAXAGGREGATERESULTS

The maximum number of results to be returned by FT.AGGREGATE command if LIMIT is used.
Setting value to `-1` will remove the limit. 

### Default

unlimited

### Example

```
$ redis-server --loadmodule ./redisearch.so MAXAGGREGATERESULTS 3000000
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

## CURSOR_MAX_IDLE

The maximum idle time (in ms) that can be set to the [cursor api](Aggregations.md#cursor_api).

### Default

"300000"

### Example

```
$ redis-server --loadmodule ./redisearch.so CURSOR_MAX_IDLE 500000
```

### Notes

* added in v1.6

---

## PARTIAL_INDEXED_DOCS

Enable/disable Redis command filter. The filter optimizes partial updates of hashes
and may avoid reindexing of the hash if changed fields are not part of schema. 

### Considerations

The Redis command filter will be executed upon each Redis Command.  Though the filter is
optimised, this will introduce a small increase in latency on all commands.  
This configuration is therefore best used with partial indexed documents where the non-
indexed fields are updated frequently.

### Default

"0"

### Example

```
$ redis-server --loadmodule ./redisearch.so PARTIAL_INDEXED_DOCS 1
```

### Notes

* added in v2.0.0

---

## GC_SCANSIZE

The garbage collection bulk size of the internal gc used for cleaning up the indexes.

### Default

100

### Example

```
$ redis-server --loadmodule ./redisearch.so GC_SCANSIZE 10
```

## GC_POLICY

The policy for the garbage collector (GC). Supported policies are:

* **FORK**:   uses a forked thread for garbage collection (v1.4.1 and above).
              This is the default GC policy since version 1.6.1 and is ideal
              for general purpose workloads.
* **LEGACY**: Uses a synchronous, in-process fork. This is ideal for read-heavy
              and append-heavy workloads with very few updates/deletes

### Default

"FORK"

### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY LEGACY
```

### Notes

* When the `GC_POLICY` is `FORK` it can be combined with the options below.

## NOGC

If set, we turn off Garbage Collection for all indexes. This is used mainly for debugging and testing, and should not be set by users.

### Default

Not set

### Example

```
$ redis-server --loadmodule ./redisearch.so NOGC
```

## FORK_GC_RUN_INTERVAL

Interval (in seconds) between two consecutive `fork GC` runs.

### Default

"30"

### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY FORK FORK_GC_RUN_INTERVAL 60
```

### Notes

* only to be combined with `GC_POLICY FORK`

## FORK_GC_RETRY_INTERVAL

Interval (in seconds) in which RediSearch will retry to run `fork GC` in case of a failure. Usually, a failure could happen when the redis fork api does not allow for more than one fork to be created at the same time.

### Default

"5"

### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY FORK FORK_GC_RETRY_INTERVAL 10
```

### Notes

* only to be combined with `GC_POLICY FORK`
* added in v1.4.16

## FORK_GC_CLEAN_THRESHOLD

The `fork GC` will only start to clean when the number of not cleaned documents is exceeding this threshold, otherwise it will skip this run. While the default value is 100, it's highly recommended to change it to a higher number.

### Default

"100"

### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 10000
```

### Notes

* only to be combined with `GC_POLICY FORK`
* added in v1.4.16

## UPGRADE_INDEX

This configuration is a special configuration introduced to upgrade indices from v1.x RediSearch versions, further referred to as 'legacy indices.' This configuration option needs to be given for each legacy index, followed by the index name and all valid option for the index description ( also referred to as the `ON` arguments for following hashes) as described on [ft.create api](Commands.md#ftcreate). See [Upgrade to 2.0](Upgrade_to_2.0.md) for more information.

### Default

There is no default for index name, and the other arguments have the same defaults as on [ft.create api](Commands.md#ftcreate)

### Example

```
$ redis-server --loadmodule ./redisearch.so UPGRADE_INDEX idx PREFIX 1 tt LANGUAGE french LANGUAGE_FIELD MyLang SCORE 0.5 SCORE_FIELD MyScore PAYLOAD_FIELD MyPayload UPGRADE_INDEX idx1
```

### Notes

* If the RDB file does not contain a legacy index that's specified in the configuration, a warning message will be added to the log file and loading will continue.
* If the RDB file contains a legacy index that wasn't specified in the configuration loading will fail and the server won't start.
