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

**Virtual sentinels are the trap.** An iterator whose result is a virtual
sentinel (`RSIndexResult::build_virt()`, `kind() == Virtual`) has no `Rf`-borrows
in `data`, so resume needs to re-validate nothing. But note two things:

1. Because `current()` hands the result out **mutably**, "still virtual" is an
   *unenforced* runtime invariant — a consumer *could* replace `data` with a real
   payload. So it must be **checked at the resume boundary, not assumed**:
   `kind() == Virtual` is the whole condition (`dmd`/`metrics` are not
   `Rf`-parametrized, so they don't affect the reinterpretation).
2. An owner of a *sentinel* has no index backing to re-validate a foreign payload
   against — it cannot recover a non-virtual result. So on violation it must
   **refuse to reinterpret** stale pointers as live — never silently continue.
   The approved pattern makes this check on `&self` **before** `Box::into_raw`
   opens the raw-pointer critical section, and returns `Ok(ResumeOutcome::Aborted)`
   so the box simply drops. It must **not** be surfaced as `Err`: a sentinel
   violation is a recoverable unsafe-to-resume state that callers already handle
   via `ResumeOutcome::Aborted`, and `RQEIteratorError` is reserved for genuine
   `TimedOut`/`IoError` failures — turning this into an `Err` would raise a
   user-visible error where none is warranted.

Worked examples:

- `optional.rs` — `Optional`'s `resume` checks `self.result.kind() != Virtual`
  on `&self`, **before** `Box::into_raw`, and returns `Ok(ResumeOutcome::Aborted)`
  on violation (the box just drops). It cannot re-validate an index-backed payload
  it did not create, so it refuses to reinterpret rather than erroring.
- `maybe_empty.rs` — `MaybeEmpty` owns **no** result, so it has no virtual
  fallback: a child that aborts on resume aborts the whole wrapper (the consumed
  `Some(I)` slot is rewritten to `None(Empty)` before the box is dropped).

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
