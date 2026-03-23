# Inverted Index Locking and Reader Design

## IndexSpec Locking Scheme

`IndexSpec` is protected by a [`pthread_rwlock_t`][spec-rwlock]. All access goes through
wrappers in `redis_index.c`:

- `RedisSearchCtx_LockSpecRead` — acquires the read lock, pauses dict rehashing on
  `spec->keysDict`.
- `RedisSearchCtx_LockSpecWrite` — acquires the write lock.
- `RedisSearchCtx_UnlockSpec` — releases either lock, resumes dict rehashing if it was paused.

The rwlock allows **multiple concurrent readers OR one exclusive writer**, never both.

## The Lock-Release-Revalidate Lifecycle

A query does **not** hold the read lock for its entire execution. The lock is released and
re-acquired between batches of results. The lifecycle repeats:

1. **Lock acquired** — [`handleSpecLockAndRevalidate`][handleSpecLock] acquires the
   read lock and calls `it->Revalidate(it)` on the iterator tree.
2. **Read phase** — the iterator reads records from inverted indexes. The read lock is held
   throughout this phase.
3. **Lock released** — once a batch of doc IDs is collected, the spec lock is released
   (`RedisSearchCtx_UnlockSpec`). For example:
   - [`rpSafeLoaderNext_Accumulate`][safeloader-unlock] releases the spec lock before
     acquiring the Redis GIL to load document payloads.
   - Cursor-based queries release the spec lock [after sending each chunk][cursor-unlock],
     then pause the cursor.
4. **Writes may happen** — while the lock is released, writers can acquire the write lock and
   modify inverted indexes (append entries, run GC, increment `gc_marker`).
5. **Lock re-acquired** — on the next call to `rpQueryItNext`, `handleSpecLockAndRevalidate`
   detects `sctx->flags == RS_CTX_UNSET`, re-acquires the read lock, and calls `Revalidate`.

### What Revalidate Does

[`Revalidate`][revalidate-api] is a method on every `QueryIterator`. It propagates down the
iterator tree (union, intersection, not, optional, etc.) to the leaf inverted index iterators.

At the leaf level, [`InvIndIterator_Revalidate`][invind-revalidate] calls
[`IndexReader_Revalidate`][reader-revalidate-rs], which checks the `gc_marker`:

```rust
fn needs_revalidation(&self) -> bool {
    self.gc_marker != self.ii.gc_marker
}
```

The reader stores a snapshot of the index's `gc_marker` at creation time. If GC (or any write)
has incremented the index's marker, the reader knows the index was modified. In that case:

1. The reader's byte offsets into blocks may be stale.
2. The iterator **rewinds** and **re-seeks** to its `lastDocId`.
3. If that exact docId no longer exists (deleted by GC), it lands on the next valid one →
   `VALIDATE_MOVED`.

Even when `needs_revalidation` returns false, `refresh_buffer_pointers` is called — blocks may
have been reallocated by appending new entries, so the `Cursor`'s internal pointer needs
refreshing.

Other iterators which sit on top of the inverted index reader may also require additional
revalidation steps before resuming iteration.

### ValidateStatus

```c
typedef enum ValidateStatus {
    VALIDATE_OK,      // Iterator still valid, same position
    VALIDATE_MOVED,   // Iterator still valid, but moved forward
    VALIDATE_ABORTED, // Iterator invalid, must be freed
} ValidateStatus;
```

Composite iterators handle these per child:

- **Union** — aborted children are removed, remaining children continue.
- **Intersection** — any aborted child aborts the whole intersection.
- **Not** — aborted child is replaced with an empty iterator (NOT nothing = everything).
- **Optional** — aborted child is replaced with an empty iterator.

## Problem: Current Rust Modeling

[`IndexReaderCore`][reader-core] holds a `&'index InvertedIndex<E>`:

```rust
pub struct IndexReaderCore<'index, E> {
    ii: &'index InvertedIndex<E>,
    current_buffer: Cursor<&'index [u8]>,
    current_block_idx: usize,
    last_doc_id: t_docId,
    gc_marker: u32,
}
```

At the FFI boundary ([`NewIndexReader`][new-index-reader]), this reference is created from a
raw pointer and the lifetime is erased via `Box::into_raw`:

```rust
let ii = unsafe { &*ii };          // fabricate &InvertedIndex
let reader = Box::new(/* ... */);
Box::into_raw(reader)              // erase the lifetime
```

The C code holds the resulting `*mut IndexReader` **across lock release/reacquire cycles**.
During those windows, a writer can mutate the `InvertedIndex` (append to blocks, run GC,
increment `gc_marker`). This means a `&InvertedIndex` exists while the referent is being
mutated — **undefined behavior** under Rust's aliasing rules, regardless of whether the
accesses are serialized by the rwlock.

Concretely, after a lock release:
- `gc_marker` may have been incremented.
- `blocks: ThinVec<IndexBlock>` may have been modified (blocks removed by GC, block buffers
  grown/reallocated by appending).
- `n_unique_docs` may have been updated.

The `refresh_buffer_pointers` method acknowledges this — it re-derives the `Cursor` from
`self.ii.blocks[self.current_block_idx].buffer`, reading through a `&InvertedIndex` that may
have been mutated. This is the UB in action.

## Solutions Considered

### Option 1: Raw Pointer

Replace `ii: &'index InvertedIndex<E>` with `ii: *const InvertedIndex<E>`. Every access goes
through `unsafe { &*self.ii }` with a safety comment documenting that the caller holds the read
lock.

**Pros:** Honest about what's happening, no false aliasing guarantees.
**Cons:** Raw pointer ergonomics throughout the read path. Every field access needs unsafe.

### Option 2: UnsafeCell

Wrap the mutable parts of `InvertedIndex` (`blocks`, `gc_marker`, `n_unique_docs`) in
`UnsafeCell`. The reader can hold `&InvertedIndex` legitimately because `UnsafeCell` opts out
of the "shared references are immutable" rule.

**Pros:** Idiomatic Rust for "externally synchronized mutation." Reader keeps `&InvertedIndex`
with a real lifetime.
**Cons:** `UnsafeCell` leaks into the `InvertedIndex` type itself. Mutating methods take
`&self` instead of `&mut self`, losing the compiler's help in the write path. Every access to
the wrapped fields requires `unsafe`.

### Option 3: Two-State Reader (Non-Transmutable)

Split the reader into `ActiveReader<'a, E>` (holds `&'a InvertedIndex`, can read) and
`SuspendedReader` (holds only cursor state, no references). Transition between them on
lock acquire/release.

**Pros:** Clean separation. `ActiveReader` has full reference ergonomics. `SuspendedReader`
is safe to hold across lock releases. No `UnsafeCell`.
**Cons:** `suspend`/`resume` copy scalar fields between the two structs. Not transmutable, so
the FFI layer needs an enum to store both states, adding a branch.

### Option 4: Generic ReaderCore with Ref Trait (Chosen)

Parameterize `ReaderCore` over a `Ref` trait that switches between references and raw pointers.
The two instantiations are layout-compatible and transmutable.

**Pros:** Active version has real reference ergonomics. Suspended version correctly uses raw
pointers. Zero-cost state transitions via transmute. Single allocation for FFI.
**Cons:** Slightly more complex type machinery (trait + two marker types).

## Chosen Design: Generic ReaderCore

### The Ref Trait

```rust
/// Abstracts over reference-like pointer types.
///
/// Two implementations exist:
/// - `Active<'a>` — uses `&'a T`, for use while the index read lock is held.
/// - `Suspended` — uses `*const T`, for use while the lock is released.
///
/// Both produce layout-compatible pointer types, so `ReaderCore<Active<'a>, E>`
/// and `ReaderCore<Suspended, E>` are transmutable.
trait Ref {
    type Ptr<T: ?Sized>;
}

struct Active<'a>(PhantomData<&'a ()>);
struct Suspended;

impl<'a> Ref for Active<'a> {
    type Ptr<T: ?Sized> = &'a T;
}

impl Ref for Suspended {
    type Ptr<T: ?Sized> = *const T;
}
```

### ReaderCore

```rust
/// The core reader state, parameterized over the pointer kind.
///
/// When `R = Active<'a>`, the reader holds real references and can read records.
/// When `R = Suspended`, the reader holds raw pointers and is inert.
///
/// `repr(C)` ensures deterministic field layout. Since `&T` and `*const T` are
/// layout-compatible (and likewise `&[T]` and `*const [T]`), the two
/// instantiations have identical memory representation.
#[repr(C)]
struct ReaderCore<R: Ref, E> {
    ii: R::Ptr<InvertedIndex<E>>,
    buf: R::Ptr<[u8]>,
    buf_pos: u64,
    current_block_idx: usize,
    last_doc_id: t_docId,
    gc_marker: u32,
    _phantom: PhantomData<E>,
}

type ActiveReader<'a, E> = ReaderCore<Active<'a>, E>;
type SuspendedReader<E> = ReaderCore<Suspended, E>;
```

### State Transitions

```rust
impl<'a, E: DecodedBy> ActiveReader<'a, E> {
    /// Drop the references, keeping only cursor state.
    /// The raw pointers in the resulting `SuspendedReader` may go stale
    /// if the index is modified, but that's fine — raw pointers are allowed
    /// to dangle, and `resume` will refresh them.
    fn suspend(self) -> SuspendedReader<E> {
        // SAFETY: ActiveReader and SuspendedReader are #[repr(C)] with
        // pairwise layout-compatible fields (&T <-> *const T, &[T] <-> *const [T]).
        unsafe { transmute(self) }
    }
}

impl<E: DecodedBy> SuspendedReader<E> {
    /// Re-activate the reader after re-acquiring the read lock.
    /// Refreshes pointers and runs revalidation.
    ///
    /// Unlike `suspend` (which transmutes the whole struct), `resume` constructs
    /// `ActiveReader` field-by-field. This asymmetry is intentional:
    ///
    /// - **`suspend`**: whole-struct transmute (Active → Suspended) is sound
    ///   because turning references into raw pointers is always valid — raw
    ///   pointers are allowed to dangle.
    /// - **`resume`**: must NOT transmute the whole struct, because that would
    ///   create `active.buf: &'a [u8]` pointing into a block buffer that may
    ///   have been freed by GC or reallocated by appending — a dangling
    ///   reference is UB regardless of whether it is dereferenced. Instead,
    ///   `buf` is derived fresh from the validated index state.
    ///
    /// # Safety
    ///
    /// The caller must hold the read lock on the IndexSpec that owns `ii`.
    unsafe fn resume<'a>(self) -> (ActiveReader<'a, E>, ValidateStatus) {
        // SAFETY: caller holds the read lock, so the InvertedIndex is valid.
        let ii: &'a InvertedIndex<E> = unsafe { &*self.ii };

        if self.gc_marker != ii.gc_marker {
            // Index was modified (GC or writes). Rewind to block 0 and re-seek.
            let mut active = ActiveReader {
                ii,
                buf: &ii.blocks[0].buffer,
                buf_pos: 0,
                current_block_idx: 0,
                last_doc_id: 0,
                gc_marker: ii.gc_marker,
                _phantom: PhantomData,
            };
            let target = self.last_doc_id;
            if target > 0 && !active.skip_to(target) {
                return (active, VALIDATE_MOVED);
            }
            (active, VALIDATE_OK)
        } else {
            // No GC, but blocks may have reallocated. Derive buf from current block.
            let active = ActiveReader {
                ii,
                buf: &ii.blocks[self.current_block_idx].buffer,
                buf_pos: self.buf_pos,
                current_block_idx: self.current_block_idx,
                last_doc_id: self.last_doc_id,
                gc_marker: self.gc_marker,
                _phantom: PhantomData,
            };
            (active, VALIDATE_OK)
        }
    }
}
```

### Read Path (Active Only)

```rust
impl<'a, E: DecodedBy> ActiveReader<'a, E> {
    /// Access the unread portion of the current block.
    fn remaining_buffer(&self) -> &'a [u8] {
        &self.buf[self.buf_pos as usize..]
    }

    /// Read the next record. No unsafe needed — self.ii and self.buf are
    /// real references, valid for 'a.
    fn read(&mut self, result: &mut RSIndexResult<'a>) -> io::Result<bool> {
        // ...
    }
}
```

### FFI Layer

C holds iterators as opaque `*mut` pointers across lock boundaries. Since every
iterator struct is generic over `Ref` and `#[repr(C)]`, the `Active<'a>` and
`Suspended` instantiations have identical memory layout. The FFI layer exploits
this by casting the same allocation to one type or the other depending on
which methods it needs to call:

- **Revalidate** (lock just re-acquired): cast to `*mut Suspended…`, call
  `resume()`, write the resulting `Active…` back to the same allocation.
- **Read / SkipTo / etc.** (lock held): cast to `*mut Active…`, call read
  methods directly. The safety contract of these FFI functions requires
  the caller holds the read lock **and** has called Revalidate since the last
  lock acquisition — no per-call resume needed.
- **Lock release**: no Rust call needed. The bytes in memory are valid for
  both types. On the next lock acquisition, Revalidate will refresh stale
  pointers before any read method is called.

The `unsafe` is confined to:
1. The `transmute` in `suspend` (justified by `#[repr(C)]` + layout compatibility).
2. The field-by-field construction in `resume` (raw pointer dereference under lock).
3. The FFI boundary functions (already `unsafe extern "C"`), which cast between
   `Active` and `Suspended` instantiations via pointer casts.

The pure Rust read path has no `unsafe`.

## C Lock Sites Involving Iterators

These are the code paths where an `IndexReader` exists across a lock release/reacquire
cycle. These are the sites where `suspend` and `resume` calls are needed.

### Resume: `handleSpecLockAndRevalidate` (the single convergence point)

[`result_processor.c:207-226`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/result_processor.c#L207-L226)

This is the **only place** where the read lock is re-acquired for iterator reading. Called
at the start of every `rpQueryItNext` / `rpQueryItNext_AsyncDisk` invocation:

```c
static bool handleSpecLockAndRevalidate(RPQueryIterator *self) {
    if (sctx->flags != RS_CTX_UNSET) return false;  // already locked
    RedisSearchCtx_LockSpecRead(sctx);
    ValidateStatus rc = it->Revalidate(it);          // <-- RESUME point
    // ...
}
```

When `sctx->flags == RS_CTX_UNSET`, the lock was previously released. `Revalidate`
propagates down the iterator tree; at the leaf level it calls `IndexReader_Revalidate`
which checks `gc_marker` and refreshes buffer pointers.

**This is where `resume` naturally maps.** The existing `Revalidate` call becomes the
resume. No new C call site needed.

### Suspend point 1: `rpSafeLoaderNext_Accumulate`

[`result_processor.c:1119`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/result_processor.c#L1119)

```c
// Accumulated a batch of doc IDs from iterators (lock was held)
RedisSearchCtx_UnlockSpec(sctx);                     // <-- SUSPEND before this
RedisModule_ThreadSafeContextLock(sctx->redisCtx);   // acquire GIL for doc loading
```

The iterator tree ([`RPQueryIterator.iterator`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/result_processor.c#L60))
survives across this unlock. The lock is re-acquired on the next batch via
`handleSpecLockAndRevalidate`.

### Suspend point 2: `runCursor`

[`aggregate_exec.c:1225-1233`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/aggregate/aggregate_exec.c#L1225-L1233)

```c
sendChunk(req, reply, num);
RedisSearchCtx_UnlockSpec(AREQ_SearchCtx(req));      // <-- SUSPEND before this
if (req->stateflags & QEXEC_S_ITERDONE) {
    Cursor_Free(cursor);
} else {
    Cursor_Pause(cursor);                             // cursor goes idle
}
```

The cursor holds the entire `AREQ` which contains the iterator tree. The iterator persists
in the paused cursor until [`cursorRead`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/aggregate/aggregate_exec.c#L1257)
resumes it, which flows back through `runCursor` → `sendChunk` → `rpQueryItNext` →
`handleSpecLockAndRevalidate`.

If `QEXEC_S_ITERDONE` is set, the iterator is about to be freed — suspend is unnecessary.

### Non-issue: `RPSafeDepleter_DepleteFromUpstream`

[`result_processor.c:1660-1693`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/result_processor.c#L1660-L1693)

The depleter acquires the lock, exhausts the upstream iterator fully (until EOF or timeout),
then releases the lock. The iterator is NOT held across a re-lock — the depleter runs to
completion in one locked session. **No suspend/resume needed.**

### Summary

| Point | Location | Action |
|-------|----------|--------|
| **Resume** | [`result_processor.c:215-216`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/result_processor.c#L215-L216) | Existing `Revalidate` call becomes the resume |
| **Suspend** | [`result_processor.c:1119`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/result_processor.c#L1119) | Before `UnlockSpec` in safe loader |
| **Suspend** | [`aggregate_exec.c:1226`](https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/aggregate/aggregate_exec.c#L1226) | Before `UnlockSpec` in cursor flow |

Resume **already exists** — the `Revalidate` mechanism. The Rust-side `resume` subsumes
the current `revalidate` + `needs_revalidation` + `refresh_buffer_pointers` into a
single state transition from `Suspended` to `Active`. The `revalidate` method is removed
from the `RQEIterator` trait — there is no need to revalidate an already-active iterator.
No C-side suspend call is needed (see next section).

## Suspend/Resume on the Rust Trait

Suspend/resume is a Rust type-system constraint — C operates on raw pointers and cannot
see the Active/Suspended distinction. There is no C `QueryIterator::Suspend` vtable
method. Suspend exists on the Rust [`RQEIterator`][rqe-iterator] trait now, so that tests
can be written soundly without `#[cfg(not(miri))]` workarounds.

The calling code (result_processor, aggregate_exec) stays in C for now. When it is
ported to Rust, the suspend sites are:

- Before the unlock in [`rpSafeLoaderNext_Accumulate`][safeloader-unlock]
- Before the unlock in [`runCursor`][cursor-unlock]

### Trait design

Only Active iterators can read. Suspending produces a different type that cannot read.
Resuming restores the Active type. Two traits with associated types form a bidirectional
relationship:

```rust
pub trait RQEIterator<'index>: Sized {
    type Suspended: SuspendedRQEIterator<Active<'index> = Self>;

    /// Consume the active iterator, producing a suspended version.
    /// All `&'index` references become raw pointers.
    fn suspend(self) -> Self::Suspended;

    // --- existing methods (revalidate removed — subsumed by resume) ---
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError>;
    fn skip_to(&mut self, doc_id: t_docId) -> ...;
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>>;
    fn rewind(&mut self);
    fn num_estimated(&self) -> usize;
    fn last_doc_id(&self) -> t_docId;
    fn at_eof(&self) -> bool;
}

pub trait SuspendedRQEIterator: Sized {
    type Active<'a>: RQEIterator<'a, Suspended = Self>;

    /// Resume the iterator after re-acquiring the read lock.
    /// Re-derives `&'a` references from stored raw pointers and
    /// revalidates (checks gc_marker, refreshes buffer pointers).
    ///
    /// # Safety
    ///
    /// The caller must hold the read lock on the IndexSpec.
    unsafe fn resume<'a>(self) -> (Self::Active<'a>, ValidateStatus);
}
```

`suspend(self)` takes ownership and returns the suspended version. Since Active and
Suspended are layout-compatible (`#[repr(C)]`, identical layouts), `suspend` is a
transmute — O(1) for the entire tree.

`resume` is self-contained: each leaf `SuspendedReader` stores a `*const InvertedIndex`
from before suspend. On resume, it re-derives `&'a InvertedIndex` from that raw pointer
(valid because the caller holds the lock), checks gc_marker, and refreshes buffer
pointers. Composites forward resume to children.

### Per-iterator behavior

| Iterator | `suspend` | `resume` |
|----------|-----------|----------|
| `InvIndIterator` | Transmute (Active → Suspended) | Transmute + revalidate inner reader |
| `Numeric` / `Term` | Forward to inner `InvIndIterator` | Forward |
| `Intersection` / `Not` / `Optional` | Forward to each child | Forward to each child |
| `Profile` | Forward to child | Forward |
| `Empty` / `IdList` / `Metric` / `Wildcard` | No-op (identity) | No-op (identity) |

## `Ref` Propagation Through the Iterator Hierarchy

The `Ref` parameter cannot be confined to `ReaderCore`. Suspending the reader changes its
type from `ReaderCore<Active<'a>, E>` to `ReaderCore<Suspended, E>`. Since `InvIndIterator`
stores `reader: R`, the reader type change forces the iterator's type to change too.
This propagates up through every struct that directly or transitively contains a reader.

### Propagation chain

```
ReaderCore<R, E>
  └─ InvIndIterator<R, E>          reader: ReaderCore<R, E>
       └─ Numeric<R, E>            it: InvIndIterator<R, E>
       └─ Term<R, E>               it: InvIndIterator<R, E>
            └─ Intersection<R, I>  children: Vec<I>, result: RSIndexResult<R>
            └─ Not<R, I>           child: MaybeEmpty<I>
            └─ Optional<R, I>      child: Option<I>, result: RSIndexResult<R>
                 └─ RSIndexResult<R>       data: RSResultData<R>
                      └─ RSResultData<R>        Union/Intersection(RSAggregateResult<R>)
                           └─ RSAggregateResult<R>   records: SmallThinVec<R::Ptr<RSIndexResult<R>>>
```

The `'index` lifetime that currently parameterizes these types is replaced by `R: Ref`.
In `Active<'a>` mode, `R::Ptr<T>` = `&'a T` — the same ergonomics as today. In
`Suspended` mode, `R::Ptr<T>` = `*const T`.

### What needs `#[repr(C)]`

Every struct in the chain must be `#[repr(C)]` so the whole-tree transmute is sound.
The transmute happens once at the outermost level (the FFI boundary), converting the
entire iterator tree from Active to Suspended in one shot.

### Two categories of `R::Ptr` fields

Not all `R::Ptr` fields are equal. It's important to distinguish:

**References into the index** — point to data owned by `InvertedIndex`, protected by the
spec rwlock. These *must* become raw pointers on suspend because the referent can be
mutated by writers while the lock is released:

| Struct | Field | Points to |
|--------|-------|-----------|
| `ReaderCore` | `ii` | `InvertedIndex<E>` (the index itself) |
| `ReaderCore` | `buf` | `[u8]` (current block buffer) |
| [`RSOffsetVector`][offset-vector] | `data` | `[u8]` (offsets within block buffer, future) |

**References to other iterator results** — point to `RSIndexResult` fields inside child
iterators, which are heap-allocated and don't move. These remain valid regardless of lock
state — children survive across suspend and their result fields are not mutated by
external writers:

| Struct | Field | Points to |
|--------|-------|-----------|
| [`RSAggregateResult`][agg-result]`::Borrowed` | `records` | child `RSIndexResult`s |

The aggregate result references change type during the whole-tree transmute (because
`RSIndexResult<R>` is parameterized), but this is a type-level consequence of `Ref`
propagation, not a safety requirement.

### `RSIndexResult` is parameterized by `Ref`

`Ref` propagates into [`RSIndexResult`][rs-index-result] because of
[`RSOffsetVector`][offset-vector]: it currently stores `*mut c_char` +
`PhantomData<&'index ()>` due to C FFI constraints. Once fully ported to Rust, this
becomes `R::Ptr<[u8]>` — a real reference into the inverted index block buffer.
Parameterizing with `Ref` now avoids a second migration later.

`RSAggregateResult`'s child references also naturally use `R::Ptr<RSIndexResult<R>>`,
though as noted above, these don't strictly need to transition for safety.

All other fields in [`RSIndexResult`][rs-index-result] are invariant: raw pointers
(`*const RSDocumentMetadata`, `*mut RSYieldableMetric`), scalars (`t_docId`, `u32`,
`f64`).

The full type chain:

```rust
RSIndexResult<R: Ref>
  └─ data: RSResultData<R>
       ├─ Term(RSTermRecord<R>)
       │    └─ offsets: RSOffsetVector<R>   // R::Ptr<[u8]> — ref into block buffer
       ├─ Union(RSAggregateResult<R>)
       │    └─ Borrowed { records: SmallThinVec<R::Ptr<RSIndexResult<R>>> }
       ├─ Intersection(RSAggregateResult<R>)
       ├─ HybridMetric(RSAggregateResult<R>)
       ├─ Virtual                           // no Ref fields
       ├─ Numeric(f64)                      // no Ref fields
       └─ Metric(f64)                       // no Ref fields
```

### Self-contained iterators

`Empty`, `IdList`, `Metric`, `Wildcard` have no `Ref`-parameterized fields. Their
Suspend is a no-op. They don't need `#[repr(C)]` for this purpose.

<!-- Link definitions -->
[spec-rwlock]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/spec.h#L348
[handleSpecLock]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/result_processor.c#L207
[safeloader-unlock]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/result_processor.c#L1119
[cursor-unlock]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/aggregate/aggregate_exec.c#L1226
[revalidate-api]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/iterators/iterator_api.h#L101
[invind-revalidate]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/iterators/inverted_index_iterator.c#L125
[reader-revalidate-rs]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/redisearch_rs/c_entrypoint/inverted_index_ffi/src/lib.rs#L1257
[reader-core]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/redisearch_rs/inverted_index/src/lib.rs#L1168
[new-index-reader]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/redisearch_rs/c_entrypoint/inverted_index_ffi/src/lib.rs#L920
[offset-vector]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/redisearch_rs/inverted_index/src/index_result.rs#L62
[agg-result]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/redisearch_rs/inverted_index/src/index_result.rs#L361
[rs-index-result]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/redisearch_rs/inverted_index/src/index_result.rs#L691
[rqe-iterator]: https://github.com/RediSearch/RediSearch/blob/0c1a6b196ae97c19a69ac467113c690130cf6cb9/src/redisearch_rs/rqe_iterators/src/lib.rs#L71
