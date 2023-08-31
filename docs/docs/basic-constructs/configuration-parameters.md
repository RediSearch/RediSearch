---
title: "Configuration parameters"
linkTitle: "Configuration parameters"
weight: 4
description: >
    Querying and searching in Redis can be tuned through multiple configuration parameters. Some of these parameters can only be set at load-time, while other parameters can be set either on load-time or on run-time.
aliases: 
    - /docs/stack/search/configuring/    
---

## Set configuration parameters on module load

Setting configuration parameters at load-time is done by appending arguments after the `--loadmodule` argument when starting a server from the command line or after the `loadmodule` directive in a Redis config file. For example:

In [redis.conf](/docs/manual/config/):

```sh
loadmodule ./redisearch.so [OPT VAL]...
```

From the [Redis CLI](/docs/manual/cli/), using the [MODULE LOAD](/commands/module-load/) command:

```
127.0.0.6379> MODULE LOAD redisearch.so [OPT VAL]...
```

From the command line:

```sh
$ redis-server --loadmodule ./redisearch.so [OPT VAL]...
```

## Set configuration parameters at run-time (for supported parameters)

RediSearch exposes the `FT.CONFIG` endpoint to allow for the setting and retrieval of configuration parameters at run-time.

To set the value of a configuration parameter at run-time (for supported parameters), simply run:

```sh
FT.CONFIG SET OPT1 VAL1
```

Similarly, you can retrieve current configuration parameter values using:

```sh
FT.CONFIG GET OPT1
FT.CONFIG GET *
```

Values set using `FT.CONFIG SET` are not persisted after server restart.

## RediSearch configuration parameters

The following table summarizes which configuration parameters can be set at module load-time and run-time:

| Configuration Parameter                             | Load-time          | Run-time             |
| :-------                                            | :-----             | :-----------         |
| [TIMEOUT](#timeout)                                 | :white_check_mark: | :white_check_mark:   |
| [ON_TIMEOUT](#on_timeout)                           | :white_check_mark: | :white_check_mark:   |
| [SAFEMODE](#safemode) deprecated in v1.6            | :white_check_mark: | :white_check_mark:   |
| [CONCURRENT_WRITE_MODE](#concurrent_write_mode)     | :white_check_mark: | :white_check_mark:   |
| [EXTLOAD](#extload)                                 | :white_check_mark: | :white_check_mark:   |
| [MINPREFIX](#minprefix)                             | :white_check_mark: | :white_check_mark:   |
| [MAXPREFIXEXPANSIONS](#maxprefixexpansions)         | :white_check_mark: | :white_check_mark:   |
| [MAXDOCTABLESIZE](#maxdoctablesize)                 | :white_check_mark: | :white_check_mark:   |
| [MAXSEARCHRESULTS](#maxsearchresults)               | :white_check_mark: | :white_check_mark:   |
| [MAXAGGREGATERESULTS](#maxaggregateresults)         | :white_check_mark: | :white_check_mark:   |
| [FRISOINI](#frisoini)                               | :white_check_mark: | :white_check_mark:   |
| [CURSOR_MAX_IDLE](#cursor_max_idle)                 | :white_check_mark: | :white_check_mark:   |
| [PARTIAL_INDEXED_DOCS](#partial_indexed_docs)       | :white_check_mark: | :white_check_mark:   |
| [GC_SCANSIZE](#gc_scansize)                         | :white_check_mark: | :white_large_square: | 
| [GC_POLICY](#gc_policy)                             | :white_check_mark: | :white_check_mark:   |
| [NOGC](#nogc)                                       | :white_check_mark: | :white_check_mark:   |
| [FORK_GC_RUN_INTERVAL](#fork_gc_run_interval)       | :white_check_mark: | :white_check_mark:   |
| [FORK_GC_RETRY_INTERVAL](#fork_gc_retry_interval)   | :white_check_mark: | :white_check_mark:   |
| [FORK_GC_CLEAN_THRESHOLD](#fork_gc_clean_threshold) | :white_check_mark: | :white_check_mark:   |
| [UPGRADE_INDEX](#upgrade_index)                     | :white_check_mark: | :white_check_mark:   |
| [OSS_GLOBAL_PASSWORD](#oss_global_password)         | :white_check_mark: | :white_large_square: |
| [DEFAULT_DIALECT](#default_dialect)                 | :white_check_mark: | :white_check_mark:   |
| [VSS_MAX_RESIZE](#vss_max_resize)                   | :white_check_mark: | :white_check_mark:   |

---

### TIMEOUT

The maximum amount of time **in milliseconds** that a search query is allowed to run. If this time is exceeded we return the top results accumulated so far, or an error depending on the policy set with `ON_TIMEOUT`. The timeout can be disabled by setting it to 0.

{{% alert title="Notes" color="info" %}}

* Timeout refers to query time only.
* Parsing the query is not counted towards `timeout`.
* If timeout was not reached during the search, finalizing operation such as loading documents' content or reducers, continue.

{{% /alert %}}

#### Default

500

#### Example

```
$ redis-server --loadmodule ./redisearch.so TIMEOUT 100
```

---

### ON_TIMEOUT

The response policy for queries that exceed the `TIMEOUT` setting.

The policy can be one of the following:

* **RETURN**: this policy will return the top results accumulated by the query until it timed out.
* **FAIL**: will return an error when the query exceeds the timeout value.

#### Default

RETURN

#### Example

```
$ redis-server --loadmodule ./redisearch.so ON_TIMEOUT fail
```

---

### SAFEMODE

{{% alert title="Deprecated" color="info" %}}

Deprecated in v1.6. From this version, SAFEMODE is the default.  If you still like to re-enable the concurrent mode for writes, use [CONCURRENT_WRITE_MODE](#concurrent_write_mode).

{{% /alert %}}

If present in the argument list, RediSearch will turn off concurrency for query processing, and work in a single thread.

This is useful if data consistency is extremely important, and avoids a situation where deletion of documents while querying them can cause momentarily inconsistent results (i.e. documents that were valid during the invocation of the query are not returned because they were deleted during query processing).

#### Default
Off (not present)

#### Example

```
$ redis-server --loadmodule ./redisearch.so SAFEMODE
```

{{% alert title="Note" color="info" %}}

* Deprecated in v1.6

{{% /alert %}}

___

### CONCURRENT_WRITE_MODE

If enabled, write queries will be performed concurrently. For now only the tokenization part is executed concurrently. The actual write operation still requires holding the Redis Global Lock.

#### Default

Not set - "disabled"

#### Example

```
$ redis-server --loadmodule ./redisearch.so CONCURRENT_WRITE_MODE
```

{{% alert title="Note" color="info" %}}

* Added in v1.6

{{% /alert %}}

---

### EXTLOAD

If present, we try to load a RediSearch extension dynamic library from the specified file path. See [Extensions](/docs/interact/search-and-query/administration/extensions/) for details.

#### Default

None

#### Example

```
$ redis-server --loadmodule ./redisearch.so EXTLOAD ./ext/my_extension.so
```

---

### MINPREFIX

The minimum number of characters we allow for prefix queries (e.g. `hel*`). Setting it to 1 can hurt performance.

#### Default

2

#### Example

```
$ redis-server --loadmodule ./redisearch.so MINPREFIX 3
```

---

### MAXPREFIXEXPANSIONS

The maximum number of expansions we allow for query prefixes. Setting it too high can cause performance issues. If MAXPREFIXEXPANSIONS is reached, the query will continue with the first acquired results. The configuration is applicable for all affix queries including prefix, suffix and infix (contains) queries.

#### Default

200

#### Example

```
$ redis-server --loadmodule ./redisearch.so MAXPREFIXEXPANSIONS 1000
```

---

### MAXDOCTABLESIZE

The maximum size of the internal hash table used for storing the documents. 
Notice, this configuration doesn't limit the amount of documents that can be stored but only the hash table internal array max size.
Decreasing this property can decrease the memory overhead in case the index holds a small amount of documents that are constantly updated.

#### Default

1000000

#### Example

```
$ redis-server --loadmodule ./redisearch.so MAXDOCTABLESIZE 3000000
```

---

### MAXSEARCHRESULTS

The maximum number of results to be returned by FT.SEARCH command if LIMIT is used.
Setting value to `-1` will remove the limit. 

#### Default

1000000

#### Example

```
$ redis-server --loadmodule ./redisearch.so MAXSEARCHRESULTS 3000000
```

---

### MAXAGGREGATERESULTS

The maximum number of results to be returned by FT.AGGREGATE command if LIMIT is used.
Setting value to `-1` will remove the limit. 

#### Default

unlimited

#### Example

```
$ redis-server --loadmodule ./redisearch.so MAXAGGREGATERESULTS 3000000
```

---

### FRISOINI

If present, we load the custom Chinese dictionary from the specified path. See [Using custom dictionaries](/docs/interact/search-and-query/advanced-concepts/chinese/#using-custom-dictionaries) for more details.

#### Default

Not set

#### Example

```
$ redis-server --loadmodule ./redisearch.so FRISOINI /opt/dict/friso.ini
```

---

### CURSOR_MAX_IDLE

The maximum idle time (in ms) that can be set to the [cursor api](/docs/interact/search-and-query/search/aggregations/#cursor-api).

#### Default

"300000"

#### Example

```
$ redis-server --loadmodule ./redisearch.so CURSOR_MAX_IDLE 500000
```

{{% alert title="Note" color="info" %}}

* Added in v1.6

{{% /alert %}}

---

### PARTIAL_INDEXED_DOCS

Enable/disable Redis command filter. The filter optimizes partial updates of hashes
and may avoid reindexing of the hash if changed fields are not part of schema. 

#### Considerations

The Redis command filter will be executed upon each Redis Command.  Though the filter is
optimized, this will introduce a small increase in latency on all commands.  
This configuration is therefore best used with partial indexed documents where the non-
indexed fields are updated frequently.

#### Default

"0"

#### Example

```
$ redis-server --loadmodule ./redisearch.so PARTIAL_INDEXED_DOCS 1
```

{{% alert title="Note" color="info" %}}

* Added in v2.0.0

{{% /alert %}}


---

### GC_SCANSIZE

The garbage collection bulk size of the internal gc used for cleaning up the indexes.

#### Default

100

#### Example

```
$ redis-server --loadmodule ./redisearch.so GC_SCANSIZE 10
```

---

### GC_POLICY

The policy for the garbage collector (GC). Supported policies are:

* **FORK**:   uses a forked thread for garbage collection (v1.4.1 and above).
              This is the default GC policy since version 1.6.1 and is ideal
              for general purpose workloads.
* **LEGACY**: Uses a synchronous, in-process fork. This is ideal for read-heavy
              and append-heavy workloads with very few updates/deletes.
              Deprecated in v2.6.0.

#### Default

"FORK"

#### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY FORK
```

{{% alert title="Note" color="info" %}}

* When the `GC_POLICY` is `FORK` it can be combined with the options below.

{{% /alert %}}

---

### NOGC

If set, we turn off Garbage Collection for all indexes. This is used mainly for debugging and testing, and should not be set by users.

#### Default

Not set

#### Example

```
$ redis-server --loadmodule ./redisearch.so NOGC
```

---

### FORK_GC_RUN_INTERVAL

Interval (in seconds) between two consecutive `fork GC` runs.

#### Default

"30"

#### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY FORK FORK_GC_RUN_INTERVAL 60
```

{{% alert title="Note" color="info" %}}

* Can only be combined with `GC_POLICY FORK`

{{% /alert %}}

---

### FORK_GC_RETRY_INTERVAL

Interval (in seconds) in which RediSearch will retry to run `fork GC` in case of a failure. Usually, a failure could happen when the redis fork api does not allow for more than one fork to be created at the same time.

#### Default

"5"

#### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY FORK FORK_GC_RETRY_INTERVAL 10
```

{{% alert title="Notes" color="info" %}}

* Can only be combined with `GC_POLICY FORK`
* Added in v1.4.16

{{% /alert %}}

---

### FORK_GC_CLEAN_THRESHOLD

The `fork GC` will only start to clean when the number of not cleaned documents is exceeding this threshold, otherwise it will skip this run. While the default value is 100, it's highly recommended to change it to a higher number.

#### Default

"100"

#### Example

```
$ redis-server --loadmodule ./redisearch.so GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 10000
```

{{% alert title="Notes" color="info" %}}

* Can only be combined with `GC_POLICY FORK`
* Added in v1.4.16

{{% /alert %}}

---

### UPGRADE_INDEX

This configuration is a special configuration introduced to upgrade indices from v1.x RediSearch versions, further referred to as 'legacy indices.' This configuration option needs to be given for each legacy index, followed by the index name and all valid option for the index description ( also referred to as the `ON` arguments for following hashes) as described on [ft.create api](/commands/ft.create). 

#### Default

There is no default for index name, and the other arguments have the same defaults as on `FT.CREATE` api

#### Example

```
$ redis-server --loadmodule ./redisearch.so UPGRADE_INDEX idx PREFIX 1 tt LANGUAGE french LANGUAGE_FIELD MyLang SCORE 0.5 SCORE_FIELD MyScore PAYLOAD_FIELD MyPayload UPGRADE_INDEX idx1
```

{{% alert title="Notes" color="info" %}}

* If the RDB file does not contain a legacy index that's specified in the configuration, a warning message will be added to the log file and loading will continue.
* If the RDB file contains a legacy index that wasn't specified in the configuration loading will fail and the server won't start.

{{% /alert %}}

---

### OSS_GLOBAL_PASSWORD

Global oss cluster password that will be used to connect to other shards.

#### Default

Not set

#### Example

```
$ redis-server --loadmodule ./redisearch.so OSS_GLOBAL_PASSWORD password
```

{{% alert title="Notes" color="info" %}}

* Only relevant when Coordinator is used
* Added in v2.0.3

{{% /alert %}}

---

### DEFAULT_DIALECT

The default DIALECT to be used by `FT.CREATE`, `FT.AGGREGATE`, `FT.EXPLAIN`, `FT.EXPLAINCLI`, and `FT.SPELLCHECK`.

#### Default

"1"

#### Example

```
$ redis-server --loadmodule ./redisearch.so DEFAULT_DIALECT 2
```

{{% alert title="Notes" color="info" %}}

* Vector similarity search requires `DIALECT 2` or greater **(added in v2.4.3)**.
* Returning multiple values from `FT.SEARCH` and `FT.AGGREGATE` requires `DIALECT 3` or greater, when available **(added in v2.6.1)**.

{{% /alert %}}

---

### VSS_MAX_RESIZE

The maximum memory resize for Vector Similarity index in bytes. This value will override default memory limits if you need to allow a large [`BLOCK_SIZE`](/docs/interact/search-and-query/search/vectors/#creation-attributes-per-algorithm).

#### Default

"0"

#### Example

```
$ redis-server --loadmodule ./redisearch.so VSS_MAX_RESIZE 52428800  # 50MB
```

{{% alert title="Note" color="info" %}}

* Added in v2.4.8

{{% /alert %}}

---