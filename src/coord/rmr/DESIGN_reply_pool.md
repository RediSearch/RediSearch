# Memory Pool Design for Hiredis Reply Objects

## Problem

Coordinator receives shard responses parsed by hiredis. For 1000 results per cursor read:
- ~20,000 individual `rm_malloc()` calls
- ~20,000 individual `rm_free()` calls
- Each goes through Redis module memory accounting (atomic ops)

## Goal

Replace ~20,000 malloc/free with ~5 block allocations using bump-pointer allocation.

## Integration Point

Hiredis provides `redisReplyObjectFunctions` - callbacks for creating reply objects:

```c
typedef struct redisReplyObjectFunctions {
    void *(*createString)(const redisReadTask*, char*, size_t);
    void *(*createArray)(const redisReadTask*, size_t);
    void *(*createInteger)(const redisReadTask*, long long);
    void *(*createDouble)(const redisReadTask*, double, char*, size_t);
    void *(*createNil)(const redisReadTask*);
    void *(*createBool)(const redisReadTask*, int);
    void (*freeObject)(void*);
} redisReplyObjectFunctions;
```

Each callback receives `redisReadTask` with `privdata` (copied from `redisReader->privdata`).

**Key insight**: We implement these callbacks to allocate from our pool. Reader internals (task stack, buffers) continue using rm_* unchanged.

## Pool Design

### Structure

```c
#define REPLY_POOL_BLOCK_SIZE (64 * 1024)  // 64KB blocks

typedef struct ReplyPoolBlock {
    struct ReplyPoolBlock *next;
    size_t used;
    char data[];  // Flexible array member
} ReplyPoolBlock;

typedef struct ReplyPool {
    ReplyPoolBlock *head;      // First block (for freeing)
    ReplyPoolBlock *current;   // Current block for allocation
    size_t block_size;         // Size of each block's data area
} ReplyPool;
```

### Allocation (Bump Pointer)

```c
void *ReplyPool_Alloc(ReplyPool *pool, size_t size) {
    size = (size + 7) & ~7;  // 8-byte alignment
    
    if (size > pool->block_size) {
        // Oversized: allocate dedicated block
        ReplyPoolBlock *big = rm_malloc(sizeof(ReplyPoolBlock) + size);
        big->next = pool->current->next;
        pool->current->next = big;
        return big->data;
    }
    
    if (pool->current->used + size > pool->block_size) {
        // Current block full: allocate new block
        ReplyPoolBlock *new_block = rm_malloc(sizeof(ReplyPoolBlock) + pool->block_size);
        new_block->next = NULL;
        new_block->used = 0;
        pool->current->next = new_block;
        pool->current = new_block;
    }
    
    void *ptr = pool->current->data + pool->current->used;
    pool->current->used += size;
    return ptr;
}
```

### Deallocation (Bulk)

```c
void ReplyPool_Free(ReplyPool *pool) {
    ReplyPoolBlock *block = pool->head;
    while (block) {
        ReplyPoolBlock *next = block->next;
        rm_free(block);
        block = next;
    }
    rm_free(pool);
}
```

## Custom Reply Object Functions

### Thread-Local Storage for Pool Management

With hiredis async, we don't control when parsing happens - it's triggered internally when data arrives. We can't easily inject a new pool before each reply is parsed via `reader->privdata`.

**Solution**: Use thread-local storage (TLS) to track the current pool:

```c
static __thread ReplyPool *tls_current_pool = NULL;

static ReplyPool *getOrCreatePool(void) {
    if (!tls_current_pool) {
        tls_current_pool = ReplyPool_New();
    }
    return tls_current_pool;
}

ReplyPool *ReplyPool_TakeCurrentPool(void) {
    ReplyPool *pool = tls_current_pool;
    tls_current_pool = NULL;
    return pool;
}
```

This works because:
1. Parsing happens on a single thread (UV thread)
2. Each reply is parsed completely before the callback is invoked
3. The first `create*` callback lazily creates the pool
4. After parsing, the callback takes the pool from TLS

### Pooled Create Functions

```c
static void *pooledCreateString(const redisReadTask *task, char *str, size_t len) {
    ReplyPool *pool = getOrCreatePool();  // Get pool from TLS
    MRReply *r = ReplyPool_Alloc(pool, sizeof(MRReply));
    memset(r, 0, sizeof(*r));
    r->type = cycleType(task->type);  // Convert hiredis type to MRReply type
    r->str = ReplyPool_Alloc(pool, len + 1);
    memcpy(r->str, str, len);
    r->str[len] = '\0';
    r->len = len;

    if (task->parent) {
        MRReply *parent = task->parent->obj;
        parent->element[task->idx] = r;
    }
    return r;
}

static void *pooledCreateArray(const redisReadTask *task, size_t elements) {
    ReplyPool *pool = getOrCreatePool();  // Get pool from TLS
    MRReply *r = ReplyPool_Alloc(pool, sizeof(MRReply));
    memset(r, 0, sizeof(*r));
    r->type = cycleType(task->type);
    if (elements > 0) {
        r->element = ReplyPool_Alloc(pool, elements * sizeof(MRReply*));
        memset(r->element, 0, elements * sizeof(MRReply*));
    }
    r->elements = elements;

    if (task->parent) {
        MRReply *parent = task->parent->obj;
        parent->element[task->idx] = r;
    }
    return r;
}

// Similar for createInteger, createDouble, createNil, createBool...

static void pooledFreeObject(void *obj) {
    // No-op: pool frees everything at once
}

static redisReplyObjectFunctions pooledReplyFunctions = {
    .createString = pooledCreateString,
    .createArray = pooledCreateArray,
    .createInteger = pooledCreateInteger,
    .createDouble = pooledCreateDouble,
    .createNil = pooledCreateNil,
    .createBool = pooledCreateBool,
    .freeObject = pooledFreeObject,
};
```

## Usage

```c
// Set custom reply functions once at connection time
reader->fn = &pooledReplyFunctions;

// Parse - first allocation lazily creates pool in TLS
// All subsequent allocations use the same pool
// After parse completes, callback takes pool from TLS:
ReplyPool *pool = ReplyPool_TakeCurrentPool();
```

## Pool Lifetime

Pool is *logically* tied to a single parsed reply tree coming back from a shard. In code we track it alongside the root MRReply via a small wrapper:

```c
typedef struct {
    MRReply *reply;
    ReplyPool *pool;
} PooledReply;
```

The channel / iterator / reducer APIs should conceptually pass around `PooledReply*` rather than a bare `MRReply*` whenever the reply memory is pooled.

### Lifetime for coordinator query paths

There are two main coordinator-side consumption patterns for MRReply trees:

1. **Streaming iterator path (MRIterator + RPNet)**
   - Used for distributed FT.AGGREGATE, FT.SEARCH, and HYBRID.
   - For each shard batch, RPNet:
     - Receives one `PooledReply` (root array/map + cursor, meta, etc.).
     - Iterates its `rows` and converts every field value into an `RSValue` (via `MRReply_ToValue`), storing results in `SearchResult` / `RLookup`.
     - Optionally examines meta (warnings, `total_results`, result format) for that batch.
   - **Key property**: once the batch's rows have been fully consumed and its meta inspected, the original MRReply tree is never referenced again.
   - Therefore, the **pool for that reply can be freed immediately** at the point where RPNet currently calls `MRReply_Free(nc->current.root)` (after processing warnings and all rows for that reply).

2. **Reducer path (MRCtx + MRReduceFunc)**
   - Used for commands like FT.INFO (via `InfoReplyReducer`) and cluster spell-check reducers.
   - The MR layer collects *all* shard replies into an array `MRReply **replies` and invokes a user reducer once, which:
     - Reads from those MRReply trees (numbers, strings, nested arrays/maps).
     - Writes aggregated state into its own structures.
   - **Key property**: replies must remain valid for the *entire duration* of the reducer call, but are not needed afterwards.
   - With pooling, this means each `PooledReply`'s pool must live until the reducer returns, and then be freed (one pool per reply tree).

In both patterns, **pools do not need to live until the end of the high-level query or cursor** – only until the code that walks the MRReply tree has finished copying out the data it needs.

Channel / iterator / reducer ownership rules under pooling:

- Channel carries `PooledReply*` rather than `MRReply*`.
- The *consumer* (RPNet, reducers, iterator destructor) is responsible for freeing the pool exactly where it currently calls `MRReply_Free(root)` or loops over channel leftovers.
- Any MRReply subtree that needs to outlive that point **must not** remain backed by the pool (see next section).

### The "Take" Problem

`MRReply_Take*` functions are the only API today that conceptually transfer ownership of part of a reply tree (e.g., extracting profile data from a full reply and storing it for later printing).

With pooling, a naive implementation of `Take` is unsafe:

- If the child remains in the pool, then freeing the pool invalidates the taken pointer.
- Coordinator code *does* retain such children beyond the batch lifetime (e.g., `RPNet.shardsProfile` collects per-shard profile replies and prints them only after all results are read).

**Required contract change for pooling**:

- `MRReply_TakeArrayElement` / `MRReply_TakeMapElement` must **deep copy** the selected child subtree out of the pool, allocating it with the normal module allocators (`rm_malloc` / `rm_calloc`) instead of `ReplyPool_Alloc`.
- The returned MRReply (and all its descendants) are *non-pooled* objects and must be freed later with `MRReply_Free` as usual.
- The parent MRReply inside the pool should still have its pointer slot nulled to avoid double-free logic, but from a memory perspective the original pooled subtree may be left untouched (it will be reclaimed when the pool is freed).

Operationally this is acceptable because in current coordinator code `Take` is only used for **profile data** (~1KB per shard), not for the high-volume result rows.

## Thread Model

```
UV Thread                          Coord Thread
─────────                          ────────────
1. Data arrives, hiredis starts parsing
2. First create callback lazily creates pool in TLS
3. Parse RESP continues (allocs to TLS pool)
4. Parse complete, callback invoked
5. Callback takes pool from TLS
6. Push {reply, pool} ──────────►  7. Pop {reply, pool}
                                   8. Process reply
                                   9. Free pool
```

No synchronization needed - clean handoff. TLS ensures each reply gets its own pool.

## Files to Modify

- `src/coord/rmr/reply_pool.h/c` - New: Pool + callbacks
- `src/coord/rmr/conn.c` - Set custom reply functions on reader
- `src/coord/rpnet.c/h` - Handle PooledReply, track pool

## Design Decisions

1. **Block size**: 64KB constant
2. **Pool reuse**: No - malloc/free per reply
3. **Metrics**: DEBUG_LOG level logs (easy to find/remove)
4. **Fallback**: None - assume pool always works

---

## Required Audits Before Implementation

Before enabling pooled replies, the following code paths must be audited and updated:

### 1. All `MRReply_Take*` Usages

**Current usages found:**

| File | Usage | Lifetime Requirement | Action |
|------|-------|---------------------|--------|
| `src/coord/rpnet.c:366-376` | `MRReply_TakeMapElement(data, "profile")` and `MRReply_TakeArrayElement(root, 2)` | Stored in `nc->shardsProfile`, freed in `rpnetFree()` after query completes | **Must deep copy** |

**Verification command:**
```bash
grep -rn "MRReply_Take" src/coord --include="*.c" --include="*.h"
```

**Implementation requirement:**
- `MRReply_TakeArrayElement` and `MRReply_TakeMapElement` must be reimplemented to:
  1. Deep copy the subtree using `rm_malloc`/`rm_calloc` (not pool)
  2. Recursively copy all child nodes and strings
  3. Return a standalone MRReply tree that can be freed with `MRReply_Free`
  4. Null the parent's slot (as today) to prevent logical double-access

### 2. All `MRReply_Free` Call Sites

Each call to `MRReply_Free` on a pooled reply must be replaced with `ReplyPool_Free(pool)` (or equivalent via `PooledReply`).

**Call sites to update:**

| File | Line | Context | Action |
|------|------|---------|--------|
| `src/coord/rpnet.c:257` | `MRReply_Free(nc->current.root)` | After processing batch in `processWarningsAndCleanup` | Replace with pool free |
| `src/coord/rpnet.c:432` | `MRReply_Free(nc->current.root)` | In `rpnetFree` destructor | Replace with pool free |
| `src/coord/rpnet.c:169` | `array_foreach(nc->pendingReplies, reply, MRReply_Free(reply))` | Freeing pending replies on error | Replace with pool free for each |
| `src/coord/rpnet.c:428` | `array_foreach(nc->shardsProfile, reply, MRReply_Free(reply))` | Freeing profile data | **Keep as-is** (profile is deep-copied, not pooled) |
| `src/coord/rpnet.c:561` | `MRReply_Free(nc->current.root)` | Error path cleanup | Replace with pool free |
| `src/coord/dist_aggregate.c:231` | `MRReply_Free(rpnet->current.root)` | Cleanup on profile path | Replace with pool free |
| `src/coord/dist_utils.c:59,101` | `MRReply_Free(rep)` | In `netCursorCallback` | Replace with pool free |
| `src/coord/rmr/conn.c:418` | `MRReply_Free(rep)` | After processing connection reply | Replace with pool free |
| `src/coord/rmr/rmr.c:101,706` | `MRReply_Free(ctx->replies[i])` and iterator cleanup | MRCtx cleanup and iterator drain | Replace with pool free |

### 3. MRReply Pointer Storage Patterns

Code that stores `MRReply*` pointers for later use must be audited. The stored pointer must either:
- Be used and freed before the pool is freed, OR
- Be deep-copied out of the pool

**Patterns found:**

| File | Pattern | Safe? | Notes |
|------|---------|-------|-------|
| `src/coord/rpnet.h:48-50` | `nc->current.root/rows/meta` | ✅ Safe | Used within batch, freed at batch end |
| `src/coord/rpnet.h:60` | `nc->shardsProfile` | ✅ Safe | Uses `Take` which will deep copy |
| `src/coord/rpnet.h:66` | `nc->pendingReplies` | ✅ Safe | Each reply has own pool, freed after barrier |
| `src/coord/dist_profile.h:15` | `PrintShardProfile_ctx.replies` | ✅ Safe | Points to `shardsProfile` (deep-copied) |
| `src/coord/info_command.c:104-113` | `InfoFields.indexDef/indexSchema/indexOptions/stopWordList` | ⚠️ **Needs review** | Stores pointers into reply, used during reducer |
| `src/coord/rmr/rmr.c:52` | `MRCtx.replies` | ✅ Safe | Each reply has own pool, freed after reducer |

### 4. InfoReplyReducer Special Case

`InfoReplyReducer` in `src/coord/info_command.c` stores raw `MRReply*` pointers from the replies array:

```c
fields->indexSchema = value;   // line 248
fields->indexDef = value;      // line 252
fields->indexOptions = value;  // line 256
fields->stopWordList = value;  // line 260
fields->indexName = MRReply_String(value, &len);  // line 245 - stores char* into reply
```

These pointers are used later in `generateFieldsReply()` via `RedisModule_ReplyKV_MRReply()`.

**Analysis:**
- The reducer pattern collects all replies first, then processes them in a single call
- All stored pointers are used within the reducer function before it returns
- Pools are freed after the reducer returns (in `MRCtx_Free`)

**Conclusion:** ✅ Safe - pointers are used before pools are freed.

### 5. Spell Check Reducers

`spellCheckReducer_resp2` and `spellCheckReducer_resp3` in `src/coord/cluster_spell_check.c`:
- Iterate over `MRReply**` array
- Extract strings via `MRReply_String()` and copy them into `spellcheckReducerCtx`
- Do not store raw `MRReply*` pointers beyond the reducer call

**Conclusion:** ✅ Safe - all data is copied out during reducer execution.

### 6. MRReply_String Return Value Lifetime

`MRReply_String()` returns a `const char*` pointing directly into the reply's string buffer (which is in the pool).

**All usages must ensure:**
- The string is copied before the pool is freed, OR
- The string is only used transiently within the same batch/reducer scope

**Key safe patterns (already in codebase):**
- `MRReply_ToValue()` uses `RS_NewCopiedString()` - ✅ copies
- `QueryError_SetError()` copies the error string - ✅ copies
- `MRReply_StringEquals()` compares immediately - ✅ transient use

**Potentially unsafe patterns to verify:**
- Any code that stores `MRReply_String()` result in a struct field for later use

---

## API Changes Summary

### New Types

```c
typedef struct PooledReply {
    MRReply *reply;
    ReplyPool *pool;
} PooledReply;
```

### Modified Functions

| Function | Change |
|----------|--------|
| `MRIterator_Next` | Return `PooledReply*` instead of `MRReply*` (or add new variant) |
| `MRIterator_NextWithTimeout` | Same as above |
| `MRIteratorCallback_AddReply` | Accept `PooledReply*` or separate pool parameter |
| `MRReply_TakeArrayElement` | Deep copy subtree out of pool |
| `MRReply_TakeMapElement` | Deep copy subtree out of pool |

### New Functions

```c
// Deep copy an MRReply subtree using rm_malloc (not pool)
MRReply *MRReply_DeepCopy(const MRReply *src);

// Free a PooledReply (frees pool, not individual nodes)
void PooledReply_Free(PooledReply *pr);

// Take the current pool from TLS (called after parsing a reply)
ReplyPool *ReplyPool_TakeCurrentPool(void);
```

### Modified Structures

```c
// MRCtx now tracks pools alongside replies for the reducer path
typedef struct MRCtx {
    // ... existing fields ...
    MRReply **replies;
    ReplyPool **pools;  // Parallel array - one pool per reply
    // ...
} MRCtx;

// RPNet tracks pool for current reply
typedef struct RPNet {
    struct {
        MRReply *root;
        MRReply *rows;
        MRReply *meta;
        ReplyPool *pool;  // Pool for current reply
    } current;
    // pendingReplies changed from MRReply* to PooledReply*
    arrayof(PooledReply *) pendingReplies;
    // ...
} RPNet;
```

---

