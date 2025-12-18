# Design Document: Shared Strings in RDB

## Overview

This document proposes extending the use of the existing global cache for shared strings, implemented with a hash table, to saving/loading RDB files. The goal is to optimize memory usage and improve performance by leveraging the global cache for string deduplication during RDB operations. Additionally, it outlines enhancements for replication processes using a local cache.

## Current Approach

- **Global Cache for Keyspace**: 
  - A global hash table is already used to store shared strings across all keys in the keyspace.
  - This approach reduces memory usage and ensures consistency across the keyspace.
  - However, it is not currently utilized during RDB save/load or replication operations.

## Proposed Approach

### Extend Global Cache to RDB

- Use the existing global hash table for shared strings during RDB save/load operations.
- **RDB Save**:
  - First, store the cache, then the keys.
  - Requires access to the cache table and its properties outside of `ijson`.
- **RDB Load**:
  - First, load the cache, then the keys.
  - Requires exposing an API to load the cache externally.

### Extend Local Cache to Replication

- Introduce a local cache for each key during the full sync process.
- **Full Sync Process (Slave)**:
  - Read the local cache and remove it after loading the key.
- Support replication with and without the local cache using a feature flag.

### Requirements

- Should not affect `JSONHDT`, meaning the implementation must be confined to `ijson`.
- Must work with `flex`.
- Should include the ability to disable this feature.

## Implementation Details

1. **Reuse Existing Hash Table**:
   - Leverage the current global hash table for shared strings.
   - Ensure compatibility with RDB save/load workflows.
   - Allow saving/loading the cache directly from the module.

2. **Prepare Keys for Replication**:
   - Support replication with and without the local cache.

3. **Update RDB Version**:
   - Modify the RDB version to accommodate the new format.

4. **RDB Format**:
   - Include the following in the RDB format:
     - Cache size.
     - Cache key-value pairs.
     - JSON keys/key.

5. **Cache Usage During Load**:
   - Use the existing mechanism to store the global cache during load.
   - Drop the cache after use.

## Trade-offs

- **Concurrency**:
  - Requires synchronization mechanisms to handle concurrent access to the global cache during RDB operations. It is unclear if this is relevant to `flex`.
