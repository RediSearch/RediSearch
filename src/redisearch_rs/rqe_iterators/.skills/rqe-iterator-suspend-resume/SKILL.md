---
name: rqe-iterator-suspend-resume
description: Implementation & review guide for suspend/resume (revalidation) on query-engine iterators in src/redisearch_rs/rqe_iterators/. Use this when adding the Box<Self> suspend/resume mechanism to an RQE iterator that currently only implements the legacy revalidate, when modifying an existing suspend/resume implementation, or when reviewing a PR that touches this machinery (boxed.rs, ref_mode markers, child-slot helpers, layout-invariant proofs).
---

# RQE iterator suspend/resume — implementation & review guide

A reference for **adding or reviewing** `suspend`/`resume` (a.k.a. revalidation)
implementations for query-engine iterators in `src/redisearch_rs/rqe_iterators/`.
This is **not** about porting an iterator from C to Rust. It is about **adding the
`Box<Self>` suspend/resume mechanism to an iterator that today only implements the
legacy `revalidate`**, or working safely on an **existing** `suspend`/`resume`
implementation. Use it as a checklist when doing either, or when reviewing a PR
that touches this machinery.

> Scope: the `Box<Self>`-based suspend/resume design in `boxed.rs`, which
> supersedes the legacy `RQEIterator::revalidate`. During the transition
> `RQEIteratorBoxed` is a **subtrait** of `RQEIterator` (adds `type Suspended` +
> `fn suspend`); a later phase folds the read/skip surface in and renames it
> back to `RQEIterator`. Legacy `revalidate` returns `RQEValidateStatus`; the new
> `resume` mirrors it with `ResumeOutcome`.

## Mental model

An iterator exists in two forms that share one heap allocation:

| Form | Trait | Typical type |
|------|-------|--------------|
| Active (holds live index refs) | `RQEIteratorBoxed<'index>` | `RawFoo<'index, Active<'index>, …>` |
| Suspended (index refs weakened to raw pointers) | `RQESuspendedIterator<'query>` | `RawFoo<'query, Suspended, …>` |

- **One `#[repr(C)]` struct, parametrized by a `ref_mode` marker** (`Active` /
  `Suspended`, both `impl ref_mode::Ref`). `PhantomData<Rf>` is the only
  *unconditionally* `Rf`-dependent field; every other field is either
  mode-agnostic or an `Rf`-parametrized borrow that is weakened to a raw pointer
  on suspend and re-validated on resume (e.g. an owned `RawIndexResult`'s `data`
  — see "Owned result fields").
- **`suspend`** consumes `Box<Self>` → `Box<Self::Suspended>`.
  **`resume`** consumes `Box<Self>` + `&IndexSpecReadGuard` →
  `Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>`.
- **Dyn siblings** `RQEDynIterator` / `RQEDynSuspendedIterator` are produced by
  blanket bridge impls — never implement them by hand. `TypeErasedRQEIterator`
  (a `#[repr(transparent)]` newtype over `Box<dyn RQEDynIterator>`) is the erased
  form composites store as their child; its suspended sibling is
  `TypeErasedRQESuspendedIterator` (a **different** `Box<dyn …>`, hence a
  **different vtable** — see the type-erased pitfall below).

### The non-negotiable rule: reuse the allocation

`suspend`/`resume` **must** reuse the same heap allocation via
`Box::into_raw` + `Box::from_raw(ptr.cast())`. The box's address (and every
interior slot's address) must survive the whole cycle, because:

- the FFI wrapper caches a raw pointer into the iterator (`header.current`), and
- delegating wrappers hand out pointers into their child's storage.

**Never rebuild the iterator's own storage via `Box::new(...)`** — a fresh
allocation dangles those cached pointers. (This bans re-allocating the box whose
interior addresses are cached, not every `Box::new`: the `boxed.rs` choke-point
helpers legitimately `Box::new` a *temporary* wrapper that they immediately move
back into the original slot via `ptr::write` — that transient box owns no cached
pointers and is fine.)

## Non-generic (leaf) iterators

Examples: `Empty`, `IdList`, `Metric`, top-level `Wildcard`, and the
`inverted_index` family (`Term`, `Tag`, `Numeric`, `Missing`, `Wildcard`) built
on `RawInvIndIterator`.

> Note: `MaybeEmpty`, despite the name, is **not** a leaf — it is a generic
> wrapper (see below). `Empty` is the leaf.

- `suspend`/`resume` is a **whole-box pointer cast** between the `Active` and
  `Suspended` instantiations, reusing the allocation.
- Provide a **standalone `const _: () = { assert!(size_of/align_of/offset_of …) }`
  proof** ("invariant 1") that the two modes are layout-identical. A `#[repr(C)]`
  newtype over an already-proven inner type may **cite** the inner's invariant 1
  instead of re-proving (see `Numeric`, which relies on `RawInvIndIterator`'s
  proof in `inverted_index/core.rs`).
- If the leaf holds an index reader that weakens `R → R::Suspended`, **freeze the
  dispatch pointers** by carrying the active reader type in a separate frozen
  slot (`RA`), so `fn`-pointer fields keep the same type across modes (see
  `RawInvIndIterator`'s `ReadImpl`/`SkipToImpl` + the `RA` param).
- `resume` steps: (1) cheap identity/abort check on the suspended form; (2)
  re-acquire index refs under `guard` and re-validate position (re-seek if GC
  moved us); (3) reinterpret the box. Return `Ok`/`Moved`/`Aborted`; `Err` only
  for real failures (`IoError`/`TimedOut`). On `Aborted`/`Err` the suspended
  iterator is dropped.
- Implement `last_doc_id()` and `num_estimated()` on the suspended form (read
  from cached fields) — callers need them **without** resuming.

## Generic (wrapper / composite) iterators

Examples: `Profile` (single child); `Optional` (single child + an owned virtual
sentinel result); `MaybeEmpty` (child-or-`Empty`). `Not`, `Intersection`, and the
`Union*` family are also wrappers over child collections. The child is a generic
`I: RQEIteratorBoxed` (active) / `S: RQESuspendedIterator` (suspended), and in
practice is usually a **`TypeErasedRQEIterator`**.

### Pitfall: do NOT whole-box cast a generic child

A whole-box cast reinterprets the child slot's bytes without running the child's
own transition. For a **type-erased** child that is unsound: the active and
suspended forms carry **different `dyn` vtables**, so a byte reinterpretation
leaves the *active* vtable in place while the type claims it is suspended — the
later `resume` then dispatches through the wrong vtable (UB). (Even the erased
type's own `suspend` can't be a byte cast; it unwraps and dispatches.)

### The rule: transition the child through the trait, then cast the wrapper

Route every child transition through the choke-point helpers in `boxed.rs`, then
whole-box cast the **outer wrapper** (reusing the allocation):

- `suspend_child_slot_in_place::<I>(slot)` — drives `I::suspend` in place
  (a vtable swap for erased children; an ordinary whole-box cast for concrete
  ones) and reinitializes the slot as a valid `I::Suspended`.
- `resume_child_slot_in_place::<S>(slot, guard) -> Result<ResumeSlotOutcome,
  RQEIteratorError>` (`Unchanged`/`Moved`/`Aborted`) — drives `S::resume`, writing
  `S::Resumed` back into the **same slot**; on `Aborted` (or `Err`) it consumes
  the child and leaves the slot **uninitialized**, so the caller must reinitialize
  or tear down that slot before the box can be dropped.

After the child slot is transitioned, `Box::from_raw(raw.cast())` the wrapper.
The wrapper must be `#[repr(C)]` with the child slot plus only `Rf`-free fields
and the ZST `PhantomData<Rf>`.

### Transition *every* child, exhausted ones included

A composite that holds several children (a `Vec<I>`, a fixed pair, …) walks all of
them. Not the active region, not the ones still in its heap — **every slot it
owns**, including the ones it parked as spent. This is not an optimisation
opportunity; skipping the inactive ones is wrong twice over:

1. **The cast retypes the whole buffer at once.** After
   `Box::from_raw(raw.cast())` every slot is typed `S::Resumed<'a>`, so one left
   holding an `S` is read at the wrong type by anything that touches it —
   `Drop` included. There is no per-slot opt-out: it is transition-all or
   transition-none.
2. **`rewind` re-admits all of them.** A composite's `rewind` resets
   `num_active` to `children.len()` (or rebuilds its heap) and rewinds each
   child, so exhaustion is not out-of-reach — it is one `rewind` away. A child
   skipped on the grounds that it was spent is a suspended iterator the very next
   rewind rewinds and reads.

The only child that leaves the collection is one whose `resume` reported
`Aborted` or `Err`: the helper consumed it, so the composite removes or compacts
over its slot, exactly as `revalidate` drops an aborted child. Whatever it does
there, the buffer must stay in a shape the teardown path can describe — for a
`Vec` of children, compacting survivors down keeps it a resumed prefix followed
by a suspended suffix, which is the split a teardown after a mid-walk `Err` needs.

Removing a child *does* invalidate any active/parked partition or index-keyed
side table (`num_active`, a heap of `child_idx` entries), so rebuild those after
the walk — and say that is why, not the reason ruled out in the next section.
Rebuild them **before** rebuilding the aggregate, not after: a side-table
rebuild that touches children mutably undoes the entries' provenance. See
"Call it last" below.

### Exhaustion is terminal: assume it, don't compensate for it

`RQEIterator::at_eof` owns the rule: once it is `true` it stays `true` until
`rewind`, across `revalidate` and `resume` alike. A composite relies on it:

- a child suspended at `at_eof` resumes at `at_eof`;
- a child dropped from the active set *because* it hit EOF cannot re-enter it
  behind the parent's position.

A leaf that restores its position with `rewind()` + `skip_to` has to take care
here: the `rewind` clears the past-the-end state, so an iterator that was *at*
EOF must have that position restored explicitly rather than re-sought.

So **do not carry recovery code for a resurrected child**. Where a "child landed
behind us" path is still structurally reachable for some *other* reason — a
`QUICK_EXIT` union's own early return strands a live sibling — gate the recovery
on that reason and `debug_assert!` the rest, so a broken child is surfaced rather
than quietly absorbed. Both unions do exactly this:

```rust
if min_doc_id < original_last_doc_id {
    debug_assert!(
        QUICK_EXIT,
        "a full union's child moved behind the union's position: …",
    );
```

Enforcement is already in the harness — lean on it rather than re-deriving it.
`rqe_iterators_test_utils::revalidate_via_resume` is the funnel every resume test
goes through, and it asserts across the cycle that an at-EOF iterator stays at
EOF, that the position never moves backwards, and that `Ok` means an unchanged
position; `ContractChecker` asserts the `RQEIterator` half. A test that reaches a
state the rules forbid asserts the **panic** (`#[should_panic]` +
`#[cfg(debug_assertions)]`) rather than the compensation.

### The child slot: use a `#[repr(C)]` enum, not `Option<I>`

When the child slot must express **"present or absent"** (or "either the child or
a fallback"), give it a dedicated `#[repr(C)]` enum rather than `Option<I>`.
`Option<I>`'s niche optimization is layout-dependent on `I`, so it is **not
transmute-stable** across the `I → I::Suspended` swap — the whole-box cast could
land on a different discriminant encoding. Both landed wrappers carry a bespoke
`#[repr(C)]` enum for exactly this reason:

- `Optional` — `enum OptionalChild<I> { Gone, Present(I) }`.
- `MaybeEmpty` — `enum MaybeEmptyOption<I> { None(Empty), Some(I) }` (the newtype
  `MaybeEmpty<I>(MaybeEmptyOption<I>)` is itself `#[repr(C)]`).

The `I`-free variant (`Gone` / `None`) doubles as the teardown target below: it
is what you `ptr::write` into a consumed slot so the reused box can be dropped
without touching the moved-from child.

### Teardown on Aborted / Err

When the child was consumed and its slot is uninitialized, free the reused
allocation **without dropping the moved-from child**. Two approved shapes:

- Drop the `Rf`-free fields in place, then `std::alloc::dealloc` (see `Profile`'s
  `dealloc_after_child_gone`).
- Overwrite the consumed slot with an `I`-free enum variant via `ptr::write`
  (`OptionalChild::Gone` / `MaybeEmptyOption::None(Empty)`) — which does **not**
  drop the moved-from payload — then `drop(Box::from_raw(raw))` to reclaim the
  reused box normally. This is what `Optional` and `MaybeEmpty` do.

### Align `resume` with the existing `revalidate`

An iterator you are adding `suspend`/`resume` to almost always **already
implements the legacy `RQEIterator::revalidate`** (returning `RQEValidateStatus`)
that `resume` supersedes. Keep the two **behaviourally identical**: the same
re-seek/re-read decisions, the same conditions for reporting moved vs. unchanged
vs. aborted, and the same handling of any owned result. The outcomes correspond
one-to-one:

| `revalidate` (`RQEValidateStatus`) | `resume` (`ResumeOutcome`)       |
|------------------------------------|----------------------------------|
| `Ok`                               | `ResumeOutcome::Ok`              |
| `Moved { .. }`                     | `ResumeOutcome::Moved`           |
| `Aborted`                          | `ResumeOutcome::Aborted`         |
| *(real failure)*                   | `Err(RQEIteratorError)`          |

Derive `resume`'s decisions from the existing `revalidate` rather than inventing
new ones; during the transition both paths can run, so any divergence is a bug.
If you change one, change the other (or delete `revalidate` once the iterator is
fully cut over). Reviewers: diff the two side by side and flag any behavioural
difference.

### Wrapper invariant proof

Give the wrapper its own standalone `const _: () = { … }` invariant-1 proof using
a **representative concrete child** (module consts can't be generic). Name the
suspended child via the trait associated type to keep it terse, e.g.:

```rust
type AChild = Wildcard<'static>;
type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
type A = RawWrapper<Active<'static>, AChild>;
type S = RawWrapper<Suspended, SChild>;
assert!(offset_of!(A, child) == offset_of!(S, child));
// … other fields …
assert!(size_of::<A>() == size_of::<S>() && align_of::<A>() == align_of::<S>());
```

## Owned result fields (`RSIndexResult`)

Some iterators own an `RSIndexResult` field (`RawIndexResult<'query, Rf>`) that
they hand out from `current()` / `read()` / `skip_to`. Only **one** of its fields
is `Rf`-parametrized, and so only one is touched by the `Active ↔ Suspended`
reinterpretation:

- **`data: RawResultData<'query, Rf>`** — the only `Rf`-parametrized field. For
  non-`Virtual` kinds it holds `'index`-scoped borrows (term/offset buffers,
  aggregate children) that are weakened to raw pointers on suspend and can be
  invalidated by concurrent index mutation while suspended. `Virtual` is the only
  kind that carries none.
- Everything else is **not** `Rf`-parametrized, so the reinterpretation never
  changes its validity contract: `metrics` (`RLookupKey`-backed) are
  `'query`-scoped and stay valid across the whole cycle (the `'query: 'a` bound);
  `dmd` (`*const RSDocumentMetadata`) is a plain raw pointer in *both* modes —
  `into_active` never narrows it — so it is the consumer's concern, orthogonal to
  suspend/resume.

The direction asymmetry is the crux:

- **suspend** (`Active → Suspended`) only *weakens* `data`'s `&'index` borrows →
  raw pointers (`into_suspended` / `into_suspended_in_place`). It "has no lifetime
  or pointee precondition" — always sound, whatever the result holds.
- **resume** (`Suspended → Active`) *asserts* `data`'s raw pointers are live `&'a`
  references again (`into_active` / `into_active_in_place`). This is the dangerous
  direction: a non-`Virtual` `data` must be **re-validated** (the owning iterator
  re-reads it from the freshly re-acquired index under the guard) *before*
  re-narrowing. Leaves do exactly this in their `resume_in_place` (refresh the
  reader, promote the result).

### Aggregates: rebuild the entries, never merely re-narrow them

An **aggregate** result holds one borrowed entry per contributing child, each
derived from a borrow of that child's result. Transitioning a child hands its
allocation through a by-value `Box<Self>`, and that retag kills the borrow the
entry came from, even though the child never moves. The address survives, the
provenance does not — so the whole-box cast alone leaves entries that are
well-addressed and UB to read, which is what a scorer walking `current()` after
an `Ok` resume does.

Discharge it by **rebuilding the entry list** while the result is still
suspended: reset the aggregate's records, then push one entry per contributor
through `index_result::RawAggregateResult::push_borrowed_ptr_from_ref`.

**Recompute the contributor set; do not recognise it.** An entry records only an
address, so matching entries back to children breaks as soon as a composite
compacts over a dropped slot, and cannot tell a survivor that moved onto that
address from the one always there. Every composite already knows the rule it
chose contributors by — a union takes every child on its document, an
intersection publishes only once all of them agree — so evaluate that rule again
over the survivors, in the composite's own module.

**Recompute `freq` and `field_mask`; carry `metrics` and `doc_id` across.** The
first two are copies of what each contributor holds. Metrics are *moved* out of
the children when the aggregate is first built, so there is nothing to
re-accumulate and resetting them drops `__vector_score` for good — which is why
`reset_aggregate` is the wrong clearing tool, since it takes all four. Reset the
records alone, and read `doc_id` before you do.

**Narrow the result, never widen the children.** Entries are stored at the
composite's `'query` while its resumed children sit at the shorter `'a`.
Admitting an `'a` child into a `'query` slot silently widens its query-pipeline
pointers, which `into_active` excludes from the caller's obligations precisely
because the `'query: 'a` bound covers them. `push_borrowed_ptr_from_ref` ties its
child to `'query` so that is rejected; `&mut` is invariant, so narrowing the
result takes an explicit re-cast of the pointer.

**Call it last** — after the child walk, after any side-table rebuild, with no
`&mut` taken to a child afterwards: each entry is a shared reborrow whose tag a
later `read`, `skip_to`, `iter_mut` or `swap_remove` pops. `&mut` to the
composite itself is fine.

**Act on the outcome**, behind a `#[must_use]` type phrased as the composite
means it — a union needs one child behind its document, an intersection needs
all of them. Coming up short is safe to re-narrow and unsafe to publish: return
`ResumeOutcome::Aborted` unless the path rebuilds anyway (a union's settle, an
intersection's `skip_to`) or is gated on `is_eof`.

## Layout-invariant enforcement (compile-time)

The whole-box / `ptr::write` casts are sound only if the active and suspended
types share **size and alignment**. This is enforced, not assumed:

- `assert_layout_compatible<A, B>()` (a `const fn` asserting equal `size_of` +
  `align_of`) is called via a `const { … }` block at the top of
  `suspend_child_slot_in_place` (`I` / `I::Suspended`) and
  `resume_child_slot_in_place` (`S` / `S::Resumed<'a>`). A mismatched implementer
  **fails to compile** at monomorphization instead of causing UB — this covers
  every present and future wrapper that routes through the helpers.
- Per-struct standalone `const _` proofs (above) cover each concrete type's own
  modes.

> Monomorphization note: a `const {}` guard inside a generic fn only fires for
> **instantiated** type arguments. That is sufficient — a cast that is never
> instantiated can never execute — but it means `cargo build --lib` alone may not
> trigger it; the test/downstream build (which instantiates the iterators) does.

## Panic safety in the in-place helpers

Between the `ptr::read` (moves the child out) and the matching `ptr::write`
(reinitializes the slot) the slot is logically uninitialized while its owner
still considers it live. Because the dispatched `suspend`/`resume` is a safe
trait method that may panic, unwinding across that window would let the owner
double-drop a moved-from value. The helpers arm an `AbortOnUnwind` guard
(`std::process::abort` on drop-during-unwind) across the window, disarmed with
`std::mem::forget` once the slot is reinitialized or handed to the caller for
teardown. Any bespoke in-place transition code must reproduce this.

## Documentation & `# Safety` conventions

- Document every `unsafe` block with a `// SAFETY:` comment. Cite the relevant
  invariant: *"layout-identical across modes by invariant 1 on `RawFoo` (const
  proof above) … the remaining fields carry no `Rf`"* (the `Numeric` style).
- **Do not list compiler-enforced facts as caller preconditions.** The
  size/alignment compatibility is guaranteed by `assert_layout_compatible`, so it
  belongs in a "this is enforced, not your obligation" note — not in the
  `# Safety` bullet list of caller obligations. `# Safety` lists only what the
  caller must actually uphold (slot validity/ownership, how to interpret the slot
  afterward, teardown duties).

## Review checklist

- [ ] Active/Suspended is **one `#[repr(C)]` struct** differing only by the
      `ref_mode` marker (+ pointer-weakened fields); `PhantomData<Rf>` is the only
      *unconditionally* `Rf`-dependent field. Any other `Rf`-parametrized field
      must be a borrow weakened on suspend and re-validated on resume (e.g. an
      owned `RawIndexResult`'s `data`), not an unaudited live borrow.
- [ ] Allocation is **reused** across suspend/resume (`Box::into_raw`/
      `from_raw`); never rebuilt with `Box::new`.
- [ ] Generic children are transitioned via `suspend_child_slot_in_place` /
      `resume_child_slot_in_place`, **never** whole-box cast directly.
- [ ] **Every** child slot is transitioned — the parked and exhausted ones too,
      not just the active region — and only an `Aborted`/`Err` child leaves the
      collection. Any active/parked partition or index-keyed side table is
      rebuilt afterwards *because a removal invalidated it*, not because a child
      can come back.
- [ ] No resurrection compensation: a child at `at_eof` before the cycle is
      assumed at `at_eof` after it. A "child landed behind us" recovery is gated
      on a reason other than resurrection (e.g. `QUICK_EXIT`) and
      `debug_assert!`ed otherwise.
- [ ] A child slot that can be absent uses a **`#[repr(C)]` enum** (e.g.
      `OptionalChild`/`MaybeEmptyOption`), **never** `Option<I>` (niche layout is
      not transmute-stable across `I → I::Suspended`).
- [ ] A standalone `const _` invariant-1 proof exists (or cites the inner type's).
- [ ] Teardown on `Aborted`/`Err` frees the allocation **without** dropping the
      moved-from child slot.
- [ ] `last_doc_id()` and `num_estimated()` are implemented on the suspended form.
- [ ] `resume`'s behaviour is **compared against the existing `revalidate`** (if
      the iterator has one) and matches it — same re-seek/re-read/abort decisions,
      with `RQEValidateStatus::{Ok, Moved, Aborted}` ↔ `ResumeOutcome::{Ok, Moved,
      Aborted}` and real failures (`IoError`/`TimedOut`) → `Err`. No divergence
      between the two paths.
- [ ] If the iterator owns an `RSIndexResult`, resume re-validates its
      `Rf`-parametrized `data` payload before re-narrowing (`dmd`/`metrics` are
      not `Rf`-parametrized and need no handling); a virtual sentinel is
      **checked** (`kind() == Virtual`) on `&self` **before** `Box::into_raw`, and
      the resume **refuses to reinterpret** on violation — returning
      `ResumeOutcome::Aborted` (never `Err`: the state is recoverable, and
      `RQEIteratorError` is only for `TimedOut`/`IoError`).
- [ ] If the iterator owns an **aggregate**, `resume` rebuilds its entries from
      the transitioned children — recomputing the contributor set rather than
      matching by address, recomputing `freq`/`field_mask` with it, carrying
      `metrics`/`doc_id` across, narrowing the result to the children's lifetime
      rather than widening theirs, and acting on the `#[must_use]` outcome.
- [ ] Panic window is covered (the shared helpers already do; bespoke in-place
      code must arm its own abort guard).
- [ ] `// SAFETY:` comments cite the invariant; no compiler-enforced fact is
      listed as a caller precondition.
- [ ] Tests: a suspend→resume round-trip, **including a type-erased child** for
      wrappers, plus a box-address-stability assertion. (A concrete-child test
      alone will NOT catch the wrong-vtable bug.) An always-`Ok` mock cannot
      exercise the `Moved`/`Aborted`/`Err` branches — use a **steerable mock** for
      those. A mock's suspended type must carry the real **`'query` lifetime** (not
      `'static`), so borrowed query data in a handed-out result stays
      borrow-tracked (see `InfiniteSuspended<'query>`).

## Key files

- `boxed.rs` — the four traits, blanket bridges, `TypeErased*` wrappers,
  `assert_layout_compatible`, `suspend_child_slot_in_place` /
  `resume_child_slot_in_place`, `ResumeSlotOutcome`.
- `inverted_index/core.rs` — `RawInvIndIterator`, the canonical leaf invariant-1
  const proof and the frozen-dispatch (`RA`) technique.
- `profile.rs` — worked example of a generic single-child wrapper (child-slot
  helpers, wrapper invariant proof, teardown, outcome handling).
- `optional.rs` — wrapper that **owns a virtual sentinel**: `#[repr(C)]`
  `OptionalChild` slot, `&self` `kind() == Virtual` check before `into_raw`,
  child-abort absorption with conditional re-read.
- `maybe_empty.rs` — wrapper with **no result of its own**: `#[repr(C)]`
  `MaybeEmptyOption` newtype, child-abort propagation, delegating suspended
  accessors.
- `numeric.rs`, `term.rs` — newtype-over-inner leaves that cite the inner's
  invariant.
