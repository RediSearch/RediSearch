/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sparse-set wildcard automaton.
//!
//! Used for patterns past 255 atoms — beyond what the stack-resident
//! bitsets in [`super::atoms`] cover. Instead of representing each active
//! state as a wide bitset that would have to live on the heap, we keep the
//! per-frame state as a short list of active NFA positions and run all
//! transitions through two shared sparse-set scratches that live inside
//! the automaton and are recycled across every `step_all` call via the
//! O(1)-clear trick from [regex-automata]'s `SparseSet`.
//!
//! This kills the per-byte malloc that would otherwise dominate, and
//! turns ε-closure unions from "OR a 64+ byte bitset" into "iterate a
//! short list of positions and `insert` each".
//!
//! [regex-automata]: https://docs.rs/regex-automata
//!
//! ## Layout
//!
//! - [`SparseStateSet`] — the per-frame, per-yield state. A `SmallVec` of
//!   up to 4 inline `u32` positions plus a cached [`StateClass`]. Frames
//!   on the trie's traversal stack carry one of these; for sparse states
//!   (the common case) it stays inline at ~16 bytes with no heap.
//! - [`WorkSet`] — the recycled internal scratch. A paired `dense` /
//!   `sparse` buffer, each sized to `n_atoms + 1`. The `sparse` buffer is
//!   never zeroed between clears: membership testing filters out stale
//!   entries via the `sparse[id] < len && dense[sparse[id]] == id`
//!   invariant.
//! - [`EpsilonTable`] — the precomputed ε-closure table. Laid out so that
//!   the dominant "trivial singleton" case (the closure of a non-`Any`
//!   position is just `{position}`) skips the table lookup entirely; only
//!   the closures of `Any` atoms get a stored entry, typically a small
//!   minority of positions for real patterns.

use super::super::{Automaton, StateClass};
use super::atoms::{Atom, flatten};
use smallvec::SmallVec;
use wildcard::WildcardPattern;

/// Per-frame active state — the dense list of NFA positions currently
/// active, plus a precomputed [`StateClass`] so `classify` is a field read
/// instead of two linear scans over `dense`.
///
/// `class` is computed once at snapshot time using the automaton's `work`
/// scratch (which has an O(1) `contains` via its sparse buffer); the trie
/// driver then reads it back for free per pop.
#[derive(Clone, Debug)]
pub struct SparseStateSet {
    pub(super) dense: SmallVec<[u32; 4]>,
    pub(super) class: StateClass,
}

/// Internal recycled scratch implementing regex-automata's sparse-set trick:
///
/// ```text
/// dense:  [a, b, c, …, _, _, _]   <- positions in insertion order
/// sparse: [_, idx_a, idx_b, …]    <- sparse[pos] = index into dense
/// len:    3                        <- only first 3 entries of `dense` are live
/// ```
///
/// `clear` is `len = 0`; `sparse` keeps its stale data. `contains` reads
/// both and confirms the round-trip, which makes stale `sparse` entries
/// harmless. This lets us reuse the full `n_atoms + 1` sparse buffer
/// across every step without a memset.
struct WorkSet {
    dense: Box<[u32]>,
    sparse: Box<[u32]>,
    len: usize,
}

impl WorkSet {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            dense: vec![0u32; capacity].into_boxed_slice(),
            sparse: vec![0u32; capacity].into_boxed_slice(),
            len: 0,
        }
    }

    #[inline]
    const fn clear(&mut self) {
        self.len = 0;
    }

    #[inline]
    fn contains(&self, pos: usize) -> bool {
        // Bounds check protects us if `pos == capacity` (the accept
        // position lives at index `n_atoms`, so capacity is `n_atoms + 1`
        // and this is in range — but for safety we keep the check).
        let Some(&idx) = self.sparse.get(pos) else {
            return false;
        };
        let idx = idx as usize;
        idx < self.len && self.dense[idx] as usize == pos
    }

    #[inline]
    fn insert(&mut self, pos: usize) {
        debug_assert!(pos < self.dense.len());
        if self.contains(pos) {
            return;
        }
        let idx = self.len;
        self.dense[idx] = pos as u32;
        self.sparse[pos] = idx as u32;
        self.len += 1;
    }

    #[inline]
    const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn dense_slice(&self) -> &[u32] {
        &self.dense[..self.len]
    }
}

/// ε-closure table laid out for the common case where most rows are
/// trivially `{target}`.
///
/// `closure_index[target]`:
/// - `NON_ANY` → `target` is a non-`Any` atom (or the accept position);
///   the ε-closure is just `{target}`. Skip the table lookup and
///   `next.insert(target)` directly.
/// - else → index into `any_closures`; iterate that closure and insert
///   each position.
///
/// Memory cost per pattern: one `u32` per atom + `accept` (the index),
/// plus one `Box<[u32]>` per `Any` atom. For a 1102-atom pattern with 2
/// `Any` atoms this is ~4.4 KB total — vs ~35 KB for a per-row enum
/// representation.
#[derive(Debug)]
pub(super) struct EpsilonTable {
    closure_index: Box<[u32]>,
    any_closures: Vec<Box<[u32]>>,
}

const NON_ANY: u32 = u32::MAX;

/// Sparse-set wildcard automaton.
///
/// Compiled once per query (via [`WildcardSparseNfa::compile`]); the trie
/// iterator drives it via the [`Automaton`] trait. Internally holds two
/// recycled [`WorkSet`] scratches whose `sparse` buffers cost
/// `2 × (n_atoms + 1) × 4` bytes up front in exchange for **zero** per-byte
/// allocation in the hot loop.
pub struct WildcardSparseNfa {
    atoms: Vec<Atom>,
    start_state: SparseStateSet,
    /// If the pattern ends with `*`, the position of that trailing `Any`.
    /// Containment of this position triggers [`StateClass::Permanent`].
    trailing_star: Option<u32>,
    /// `accept_only` as a single position — `classify` reports
    /// [`StateClass::Terminal`] when the state is exactly `[accept_pos]`.
    accept_pos: u32,
    /// `Some(table)` if the pattern has `Any` atoms; `None` otherwise
    /// (ε-closure would be a no-op in that case and we skip it).
    epsilon_table: Option<EpsilonTable>,
    /// Working scratch holding the source positions for the current step.
    /// In `step_all`, the very first byte reads directly from
    /// `state.dense` (saving an ingest pass); subsequent bytes flow through
    /// `work` ↔ `next` swaps so we never re-populate from scratch.
    work: WorkSet,
    /// Working scratch receiving the destination positions of the current
    /// step. Must already be cleared on entry to `transition_into_next`.
    next: WorkSet,
}

impl WildcardSparseNfa {
    /// Compile a pattern to a sparse-set NFA.
    ///
    /// `pattern.atom_count()` must fit in `u32`. Pattern lengths beyond ~4
    /// billion atoms are not supported — far past anything realistic.
    pub fn compile(pattern: &WildcardPattern<'_>) -> Self {
        let atoms = flatten(pattern);
        let accept = atoms.len();
        debug_assert!(
            accept <= u32::MAX as usize,
            "WildcardSparseNfa caps at {} atoms",
            u32::MAX,
        );
        let capacity = accept + 1;

        let epsilon_table = atoms
            .iter()
            .any(|a| matches!(a, Atom::Any))
            .then(|| build_sparse_epsilon_table(&atoms));

        let trailing_star =
            matches!(atoms.last(), Some(Atom::Any)).then(|| (atoms.len() - 1) as u32);

        let start_state = initial_state(accept as u32, trailing_star, epsilon_table.as_ref());

        Self {
            atoms,
            start_state,
            trailing_star,
            accept_pos: accept as u32,
            epsilon_table,
            work: WorkSet::with_capacity(capacity),
            next: WorkSet::with_capacity(capacity),
        }
    }

    /// Compute one byte transition: read positions from `source`, write the
    /// resulting positions into `next`. `next` must already be cleared.
    ///
    /// Free function (not a method) so callers can split-borrow `self.work`
    /// and `self.next` — the source is `self.work.dense_slice()` between
    /// the bytes of a multi-byte label.
    #[inline]
    fn transition_into_next(
        atoms: &[Atom],
        epsilon_table: Option<&EpsilonTable>,
        source: &[u32],
        byte: u8,
        next: &mut WorkSet,
    ) {
        match epsilon_table {
            Some(table) => {
                for &pos_u32 in source {
                    let pos = pos_u32 as usize;
                    // Accept position is a sink with no outgoing transition.
                    if pos >= atoms.len() {
                        continue;
                    }
                    let target = match atoms[pos] {
                        Atom::Byte(b) if b == byte => pos + 1,
                        Atom::Byte(_) => continue,
                        Atom::One => pos + 1,
                        Atom::Any => pos,
                    };
                    // The dominant case is the trivial-singleton row:
                    // `closure_index[target] == NON_ANY`, meaning the
                    // ε-closure of `{target}` is just `{target}`. Skip the
                    // closure lookup entirely and `insert(target)` directly.
                    let idx = table.closure_index[target];
                    if idx == NON_ANY {
                        next.insert(target);
                    } else {
                        for &p in &table.any_closures[idx as usize] {
                            next.insert(p as usize);
                        }
                    }
                }
            }
            None => {
                for &pos_u32 in source {
                    let pos = pos_u32 as usize;
                    if pos >= atoms.len() {
                        continue;
                    }
                    let target = match atoms[pos] {
                        Atom::Byte(b) if b == byte => pos + 1,
                        Atom::Byte(_) => continue,
                        Atom::One => pos + 1,
                        // Unreachable: `epsilon_table` is `None` iff no `Any`.
                        Atom::Any => continue,
                    };
                    next.insert(target);
                }
            }
        }
    }

    /// Classify `self.work` using the sparse buffer's O(1) `contains` — much
    /// cheaper than `state.dense.contains(...)`'s linear scan, which is what
    /// a naive [`Automaton::classify`] would do on the snapshot. Cached on
    /// the resulting [`SparseStateSet`].
    #[inline]
    fn classify_work(&self) -> StateClass {
        // Trailing-`*`: once that position is active, every subsequent byte
        // self-loops and accept stays in via ε-closure.
        if let Some(pos) = self.trailing_star
            && self.work.contains(pos as usize)
        {
            return StateClass::Permanent;
        }
        // `{accept}` and only `{accept}` has no outgoing transitions — the
        // unique terminal state for fixed-length patterns.
        let accept = self.accept_pos as usize;
        if self.work.len == 1 && self.work.dense[0] == self.accept_pos {
            return StateClass::Terminal;
        }
        if self.work.contains(accept) {
            StateClass::LiveAccepting
        } else {
            StateClass::Live
        }
    }

    /// Snapshot `self.work` into an owned [`SparseStateSet`] for storage in
    /// a trie frame. Uses [`SmallVec::from_slice`] so the inline case is a
    /// single memcpy, and precomputes the [`StateClass`] using the sparse
    /// buffer.
    #[inline]
    fn snapshot_work(&self) -> SparseStateSet {
        SparseStateSet {
            dense: SmallVec::from_slice(self.work.dense_slice()),
            class: self.classify_work(),
        }
    }
}

impl Automaton for WildcardSparseNfa {
    type State = SparseStateSet;

    fn start(&self) -> Self::State {
        self.start_state.clone()
    }

    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State> {
        self.next.clear();
        Self::transition_into_next(
            &self.atoms,
            self.epsilon_table.as_ref(),
            &state.dense,
            byte,
            &mut self.next,
        );
        if self.next.is_empty() {
            return None;
        }
        std::mem::swap(&mut self.work, &mut self.next);
        Some(self.snapshot_work())
    }

    fn classify(&self, state: &Self::State) -> StateClass {
        // Cached at snapshot time via `classify_work`, which uses
        // `WorkSet`'s O(1) sparse contains. Reading the field here is
        // strictly faster than re-scanning `state.dense`.
        state.class
    }

    fn step_all(&mut self, state: &Self::State, bytes: &[u8]) -> Option<Self::State> {
        // Empty label — the state passes through unchanged.
        let Some((&first, rest)) = bytes.split_first() else {
            return Some(state.clone());
        };

        // First byte: source is `state.dense`. We avoid ingesting it into
        // `self.work` because the source side never needs `O(1)` contains —
        // only the destination does.
        self.next.clear();
        Self::transition_into_next(
            &self.atoms,
            self.epsilon_table.as_ref(),
            &state.dense,
            first,
            &mut self.next,
        );
        if self.next.is_empty() {
            return None;
        }
        // Move the result into `self.work` so subsequent bytes can swap
        // through the work/next pair.
        std::mem::swap(&mut self.work, &mut self.next);

        for &byte in rest {
            self.next.clear();
            Self::transition_into_next(
                &self.atoms,
                self.epsilon_table.as_ref(),
                self.work.dense_slice(),
                byte,
                &mut self.next,
            );
            if self.next.is_empty() {
                return None;
            }
            std::mem::swap(&mut self.work, &mut self.next);
        }

        Some(self.snapshot_work())
    }
}

/// Build the ε-closure table.
///
/// Computed right-to-left so each `Any` row can union in whatever the next
/// row contains. Non-`Any` rows leave `closure_index` at [`NON_ANY`] —
/// their closure is implicitly `{i}` and the hot loop skips the lookup.
fn build_sparse_epsilon_table(atoms: &[Atom]) -> EpsilonTable {
    let n = atoms.len();
    let mut closure_index = vec![NON_ANY; n + 1].into_boxed_slice();
    let mut any_closures: Vec<Box<[u32]>> = Vec::new();

    // Walking right-to-left, we need the closure of `i+1` when we build
    // the closure of `i` for an `Any` atom. That closure is either:
    //   - `{i+1}` (when `i+1` is non-`Any`) — implicit, just track the
    //     position in `prev_singleton`.
    //   - the full closure (when `i+1` is `Any`) — track its index in
    //     `prev_any_idx` so we can copy its contents.
    //
    // The accept position `i = n` is always a non-`Any` singleton `{n}`.
    let mut prev_any_idx: Option<usize> = None;
    let mut prev_singleton: u32 = n as u32;

    for i in (0..n).rev() {
        if matches!(atoms[i], Atom::Any) {
            let mut positions: Vec<u32> = Vec::with_capacity(2);
            positions.push(i as u32);
            match prev_any_idx {
                Some(idx) => positions.extend_from_slice(&any_closures[idx]),
                None => positions.push(prev_singleton),
            }
            let new_idx = any_closures.len();
            any_closures.push(positions.into_boxed_slice());
            closure_index[i] = new_idx as u32;
            prev_any_idx = Some(new_idx);
        } else {
            // Non-`Any`: closure is implicitly `{i}`. Leave
            // `closure_index[i] == NON_ANY`.
            prev_any_idx = None;
            prev_singleton = i as u32;
        }
    }

    EpsilonTable {
        closure_index,
        any_closures,
    }
}

/// The NFA's start state — `{0}`, optionally extended via ε-closure when
/// the pattern has `Any` atoms. Computes [`StateClass`] inline for the
/// initial set so the cached field is always populated.
fn initial_state(
    accept_pos: u32,
    trailing_star: Option<u32>,
    epsilon_table: Option<&EpsilonTable>,
) -> SparseStateSet {
    let dense: SmallVec<[u32; 4]> = match epsilon_table {
        Some(table) => {
            let idx = table.closure_index[0];
            if idx == NON_ANY {
                SmallVec::from_slice(&[0u32])
            } else {
                SmallVec::from_slice(&table.any_closures[idx as usize])
            }
        }
        None => SmallVec::from_slice(&[0u32]),
    };
    let class = classify_dense(&dense, accept_pos, trailing_star);
    SparseStateSet { dense, class }
}

/// Linear classify over a dense list. Used only for the start state (small,
/// classified once) — the hot path uses [`WildcardSparseNfa::classify_work`]
/// which exploits the sparse buffer for O(1) contains.
fn classify_dense(dense: &[u32], accept_pos: u32, trailing_star: Option<u32>) -> StateClass {
    if let Some(pos) = trailing_star
        && dense.contains(&pos)
    {
        return StateClass::Permanent;
    }
    if dense.len() == 1 && dense[0] == accept_pos {
        return StateClass::Terminal;
    }
    if dense.contains(&accept_pos) {
        StateClass::LiveAccepting
    } else {
        StateClass::Live
    }
}
