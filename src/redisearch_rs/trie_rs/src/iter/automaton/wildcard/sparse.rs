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
//! For patterns large enough that a bitset-backed automaton would heap-allocate
//! per state ([`super::WildcardNfa`] with `Box<[u64; N]>`-style bitsets), we
//! switch to a sparse representation: each frame on the trie iterator's stack
//! holds **only** the dense list of currently-active NFA positions, while two
//! shared "scratch" sparse-sets — one for current, one for next — live in the
//! automaton itself and are recycled across every `step_all` call via the
//! O(1)-clear trick from [regex-automata]'s `SparseSet`.
//!
//! This kills the per-byte malloc that dominated heap-bitset profiles and
//! turns ε-closure unions from "OR a 64–256-byte buffer" into "iterate a
//! short list of positions and `insert` each".
//!
//! [regex-automata]: https://docs.rs/regex-automata
//!
//! ## Layout
//!
//! - [`SparseStateSet`] is the per-frame, per-yield state — a
//!   [`SmallVec<[u32; 4]>`] of active positions in insertion order. Frames
//!   on the trie's traversal stack carry one of these; for sparse states
//!   (the common case) it stays inline at 16 bytes, no heap.
//! - [`WorkSet`] is the recycled internal scratch — a paired `dense` /
//!   `sparse` buffer, sized once at compile time to `n_atoms + 1`. The
//!   `sparse` buffer is *never* zeroed between clears: membership testing
//!   filters out stale entries via the `sparse[id] < len && dense[…] == id`
//!   invariant.
//! - [`SparseEpsilonRow`] is the same `Singleton(usize) | Set(small list)`
//!   row representation [`super::nfa::EpsilonRow`] uses, just with
//!   position-list rows instead of bitset rows. Most rows are singletons
//!   for typical patterns.

use super::super::{Automaton, StateClass};
use super::atoms::{Atom, flatten};
use smallvec::SmallVec;
use wildcard::WildcardPattern;

/// Per-frame active state — just the dense list of NFA positions currently
/// active. `Clone` is a `SmallVec` clone (one shallow copy when inline).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SparseStateSet {
    pub(super) dense: SmallVec<[u32; 4]>,
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

/// One row of the ε-closure table — a position list, mirroring
/// [`super::nfa::EpsilonRow`] but storing positions rather than bitset
/// words. Most rows for typical patterns are `Singleton`.
#[derive(Debug)]
pub(super) enum SparseEpsilonRow {
    Singleton(u32),
    Set(SmallVec<[u32; 4]>),
}

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
    epsilon_table: Option<Vec<SparseEpsilonRow>>,
    /// Working scratch holding the input state of the current step.
    work: WorkSet,
    /// Working scratch receiving the output of the current step.
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

        let start_state = initial_state(accept, epsilon_table.as_deref());

        let trailing_star =
            matches!(atoms.last(), Some(Atom::Any)).then(|| (atoms.len() - 1) as u32);

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

    /// Populate `work` from a stored frame state.
    #[inline]
    fn load_state(&mut self, state: &SparseStateSet) {
        self.work.clear();
        for &p in &state.dense {
            self.work.insert(p as usize);
        }
    }

    /// Compute one byte transition: read positions from `self.work`, write
    /// the resulting positions into `self.next`. `self.next` must already
    /// be cleared.
    #[inline]
    fn transition(&mut self, byte: u8) {
        match self.epsilon_table.as_deref() {
            Some(table) => {
                let dense = &self.work.dense[..self.work.len];
                for &pos_u32 in dense {
                    let pos = pos_u32 as usize;
                    // Accept position is a sink with no outgoing transition.
                    if pos >= self.atoms.len() {
                        continue;
                    }
                    let target = match self.atoms[pos] {
                        Atom::Byte(b) if b == byte => pos + 1,
                        Atom::Byte(_) => continue,
                        Atom::One => pos + 1,
                        Atom::Any => pos,
                    };
                    // SAFETY-by-construction: `target` is in `0..=atoms.len()`,
                    // matching the table's length `n + 1`.
                    match &table[target] {
                        SparseEpsilonRow::Singleton(p) => {
                            self.next.insert(*p as usize);
                        }
                        SparseEpsilonRow::Set(positions) => {
                            for &p in positions {
                                self.next.insert(p as usize);
                            }
                        }
                    }
                }
            }
            None => {
                let dense = &self.work.dense[..self.work.len];
                for &pos_u32 in dense {
                    let pos = pos_u32 as usize;
                    if pos >= self.atoms.len() {
                        continue;
                    }
                    let target = match self.atoms[pos] {
                        Atom::Byte(b) if b == byte => pos + 1,
                        Atom::Byte(_) => continue,
                        Atom::One => pos + 1,
                        // Unreachable: `epsilon_table` is `None` iff no `Any`.
                        Atom::Any => continue,
                    };
                    self.next.insert(target);
                }
            }
        }
    }

    /// Snapshot `self.work` into an owned [`SparseStateSet`] for storage in
    /// a trie frame.
    #[inline]
    fn snapshot_work(&self) -> SparseStateSet {
        let dense: SmallVec<[u32; 4]> = self.work.dense_slice().iter().copied().collect();
        SparseStateSet { dense }
    }
}

impl Automaton for WildcardSparseNfa {
    type State = SparseStateSet;

    fn start(&self) -> Self::State {
        self.start_state.clone()
    }

    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State> {
        self.load_state(state);
        self.next.clear();
        self.transition(byte);
        if self.next.is_empty() {
            return None;
        }
        std::mem::swap(&mut self.work, &mut self.next);
        Some(self.snapshot_work())
    }

    fn classify(&self, state: &Self::State) -> StateClass {
        // Trailing-`*`: once that position is active, every subsequent byte
        // self-loops and accept stays in via ε-closure.
        if let Some(pos) = self.trailing_star
            && state.dense.contains(&pos)
        {
            return StateClass::Permanent;
        }
        // `{accept}` and only `{accept}` has no outgoing transitions — the
        // unique terminal state for fixed-length patterns.
        if state.dense.len() == 1 && state.dense[0] == self.accept_pos {
            return StateClass::Terminal;
        }
        if state.dense.contains(&self.accept_pos) {
            StateClass::LiveAccepting
        } else {
            StateClass::Live
        }
    }

    fn step_all(&mut self, state: &Self::State, bytes: &[u8]) -> Option<Self::State> {
        // Empty label — the state passes through unchanged. Just hand back a
        // clone (cheap: short `SmallVec`).
        let Some((&first, rest)) = bytes.split_first() else {
            return Some(state.clone());
        };

        // Ingest the input state into `work`, then alternate between
        // `work` (current) and `next` (output) for every byte.
        self.load_state(state);
        self.next.clear();
        self.transition(first);
        if self.next.is_empty() {
            return None;
        }
        std::mem::swap(&mut self.work, &mut self.next);

        for &byte in rest {
            self.next.clear();
            self.transition(byte);
            if self.next.is_empty() {
                return None;
            }
            std::mem::swap(&mut self.work, &mut self.next);
        }

        Some(self.snapshot_work())
    }
}

/// Build the per-position ε-closure table as a list of position-list rows.
///
/// Mirrors the logic of [`super::nfa::build_epsilon_table`] but stores rows
/// as `Singleton(usize)` (no allocation) for `Byte`/`One` atoms and the
/// accept row, and `Set(SmallVec<…>)` (small list of positions) for `Any`
/// atoms.
fn build_sparse_epsilon_table(atoms: &[Atom]) -> Vec<SparseEpsilonRow> {
    let n = atoms.len();
    let mut table: Vec<SparseEpsilonRow> = Vec::with_capacity(n + 1);
    table.push(SparseEpsilonRow::Singleton(n as u32));
    for i in (0..n).rev() {
        let entry = if matches!(atoms[i], Atom::Any) {
            // Closure of `{i}` spans more than one position: build a list.
            let mut positions: SmallVec<[u32; 4]> = SmallVec::new();
            positions.push(i as u32);
            // SAFETY: `table` has at least the accept row pushed above
            // plus whatever we've pushed in earlier loop iterations.
            match table.last().unwrap() {
                SparseEpsilonRow::Singleton(p) => positions.push(*p),
                SparseEpsilonRow::Set(others) => {
                    for &p in others {
                        positions.push(p);
                    }
                }
            }
            SparseEpsilonRow::Set(positions)
        } else {
            SparseEpsilonRow::Singleton(i as u32)
        };
        table.push(entry);
    }
    table.reverse();
    table
}

/// The NFA's start state — `{0}`, optionally extended via ε-closure when
/// the pattern has `Any` atoms.
fn initial_state(n_atoms: usize, epsilon_table: Option<&[SparseEpsilonRow]>) -> SparseStateSet {
    let dense: SmallVec<[u32; 4]> = match epsilon_table {
        Some(table) => match &table[0] {
            SparseEpsilonRow::Singleton(p) => SmallVec::from_slice(&[*p]),
            SparseEpsilonRow::Set(positions) => positions.clone(),
        },
        None => {
            let _ = n_atoms; // silence unused-warning when `Any`-free.
            SmallVec::from_slice(&[0u32])
        }
    };
    SparseStateSet { dense }
}
