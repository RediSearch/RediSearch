# Upgrade to 2.0 when running in Redis OSS

!!! note
    For enterprise upgrade please refer to the following [link](https://docs.redislabs.com/latest/modules/redisearch/).


v2 of RediSearch reachitects the way indices are kept in sync with the data. Instead of using `FT.ADD` command to index documents, RediSearch 2.0 follows hashes that match the index description regardless of how those were inserted or changed on Redis (`HSET`, `HINCR`, `HDEL`). The index description will filter hashes on a prefix of the key, and allows you to construct fine-grained filters with the `FILTER` option. This description can be defined during index creation ([ft.create](Commands.md#ftcreate)). 

v1.x indices (further referred to as legacy indices)  don't have such index description.  This is why you will need to supply this description on when upgrading to v2. During the upgrade to v2 you can add module configuration on start time and allow RediSearch 2.0 to load those legacy indexes.

## UPGRADE_INDEX configuration

The upgrade index configuration allows you to specify the legacy index to upgrade. It needs to specify the index name and all the `on hash` arguments that can be defined on [ft.create](Commands.md#ftcreate) command (notice that only the index name is mandatory, the other arguments have default values which are the same as the default values on [ft.create](Commands.md#ftcreate) command). So for example, if you have a legacy index called `idx`, in order for RediSearch 2.0 to load it, the following configuration needs to be added to the server on start time:
```
redis-server --loadmodule redisearch.so UPGRADE_INDEX idx
```

It is also possible to specify the prefixes to follow. For example, assuming all the documents indexed by `idx` starts with the prefix `idx:`, the following will upgrade the legacy index `idx`:
```
redis-server --loadmodule redisearch.so UPGRADE_INDEX idx PREFIX 1 idx:
```

## Upgrade Limitations

The way the upgrade process works behind the scene is that it redefines the index with the `on hash` index descripiton given in the configuration and reindexes the data. This comes with some limitations:
* If `NOSAVE` was used, than it's not possible to upgrade because the data do not exist for the reindexing.
* If you have multiple indices, you must find a way by for RediSearch to be able to identify which hashes belongs to which index. You can do it either with prefix or with filter.
* If you have hashes that are not indexed, you will need to find a way so that RediSearch will be able to identify only the hashes that need to be indexed. Again this can be done using prefix or with filter.
