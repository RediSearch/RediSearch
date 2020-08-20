# Upgrade to 2.0

On v2.0 we first introduce a new concept in the way data is indexed on RediSearch, instead of using `FT.ADD` commands RediSearch 2.0 is following hashes regardless of the way those were inserted or changed on Redis (hset, hincr, hdel). How RediSearch knows on which hashes to follow and which hashes to index? Mostly by the hash key prefix (but also with FILTER) that can be defined on index creation (look at [ft.create](Commands.md#ftcreate) command definition for more information). The problem is that indexes that were created on versions 1.x (call them legacy indexes) do not contain those following hash definition. The upgrade to 2.0 process allows you to add those definitions using module configuration on start time and allow RediSearch 2.0 to load those legacy indexes.

## UPGRADE_INDEX configuration

The upgrade index configuration allows you to specify the legacy index to upgrade. It needs to specify the index name and all the `follow hash` arguments that can be defined on [ft.create](Commands.md#ftcreate) command (notice that only the index name is mandatory, the other arguments have default values which are the same as the default values on [ft.create](Commands.md#ftcreate) command). So for example, if you have a legacy index called `idx`, in order for RediSearch 2.0 to load it, the following configuration needs to be added to the server on start time:
```
redis-server --loadmodule redisearch.so UPGRADE_INDEX idx
```

It is also possible to specify the prefixes to follow. For example, assuming all the documents indexed by `idx` starts with the prefix `idx:`, the following will upgrade the legacy index `idx`:
```
redis-server --loadmodule redisearch.so UPGRADE_INDEX idx PREFIX 1 idx:
```

## Upgrade Limitations

The way the upgrade process works behind the scene is that it redefined the index with the `follow hash` rule given on the configuration and reindex the data. This comes with some limitations:
* If NOSAVE was used then it's not possible to upgrade cause the data do not exist for the reindexing.
* If you have multiple indexes, you must find a way by which RediSearch will be able to identify which hash belongs to which index. You can do it either with prefix or with filter.
* If you have hashes that are not indexed, you will have to find a way by which RediSearch will be able to identify only the hashed that need to be index. Again this can be done using prefix or with filter.