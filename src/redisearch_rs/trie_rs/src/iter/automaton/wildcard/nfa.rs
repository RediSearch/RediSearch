/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! NFA-simulation wildcard automaton, generic over the [`NfaBitSet`]
//! representation.

use super::super::{Automaton, StateClass};
use super::atoms::{Atom, NfaBitSet, flatten};
use wildcard::WildcardPattern;

/// Build the per-position ε-closure table.
///
/// `epsilon_table[i]` is the set of positions reachable from `{i}` by zero
/// or more `*`-skip edges. Computed right-to-left so each entry just unions
/// in the next one when the current atom is `Any`.
pub(super) fn build_epsilon_table<S: NfaBitSet>(atoms: &[Atom]) -> Vec<S> {
    let n = atoms.len();
    let mut table: Vec<S> = (0..=n).map(|_| S::empty(n)).collect();
    table[n].insert(n);
    for i in (0..n).rev() {
        let mut s = S::singleton(n, i);
        if matches!(atoms[i], Atom::Any) {
            s.union_in_place(&table[i + 1]);
        }
        table[i] = s;
    }
    table
}

/// The NFA's start state — `{0}`, optionally extended via ε-closure when
/// the pattern has `Any` atoms.
pub(super) fn initial_state<S: NfaBitSet>(n_atoms: usize, epsilon_table: Option<&[S]>) -> S {
    // `epsilon_table[0]` is the ε-closure of `{0}` by construction, so we
    // can return it directly when the table is available.
    match epsilon_table {
        Some(table) => table[0].clone(),
        None => S::singleton(n_atoms, 0),
    }
}

/// Step an NFA state by one input byte, applying ε-closure inline. Writes
/// the result into the caller-provided `next`, which **must already be
/// empty** — this is the recycled-scratch entrypoint, used by
/// [`WildcardNfa::step_all`] when iterating over a multi-byte label so the
/// per-byte allocation budget doesn't include a fresh `S::empty(...)`.
///
/// `epsilon_table` may be `None` for patterns with no `Any` atoms — in that
/// case the byte transition target is inserted directly because ε-closure
/// would be a no-op.
///
/// When the table is present, each per-position transition's target row is
/// already the full transitive closure, so we OR it into `next` directly
/// without a separate post-pass. This avoids the snapshot allocation that
/// a "build, then epsilon-close" structure would require.
pub(super) fn nfa_step_into<S: NfaBitSet>(
    atoms: &[Atom],
    state: &S,
    byte: u8,
    epsilon_table: Option<&[S]>,
    next: &mut S,
) {
    debug_assert!(next.is_empty());
    match epsilon_table {
        Some(table) => {
            for pos in state.iter() {
                // The accept position (`pos == atoms.len()`) has no atom and
                // no outgoing transition — it's a sink. Any survival from
                // here happens through other active positions.
                if pos >= atoms.len() {
                    continue;
                }
                let target = match atoms[pos] {
                    Atom::Byte(b) if b == byte => pos + 1,
                    Atom::Byte(_) => continue,
                    Atom::One => pos + 1,
                    Atom::Any => pos,
                };
                // SAFETY: `target` is a valid atom-or-accept index in
                // `0..=atoms.len()`, matching the table's length `n + 1`.
                let row = unsafe { table.get_unchecked(target) };
                next.union_in_place(row);
            }
        }
        None => {
            for pos in state.iter() {
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

/// Allocating wrapper around [`nfa_step_into`] for one-off byte transitions
/// (e.g., the `Automaton::step` impl and the first byte of `step_all`),
/// where there's no scratch buffer to recycle.
pub(super) fn nfa_step<S: NfaBitSet>(
    atoms: &[Atom],
    state: &S,
    byte: u8,
    epsilon_table: Option<&[S]>,
) -> S {
    let mut next = S::empty(atoms.len());
    nfa_step_into(atoms, state, byte, epsilon_table, &mut next);
    next
}

/// NFA-simulation wildcard automaton, parameterized over the bitset.
pub struct WildcardNfa<S: NfaBitSet> {
    atoms: Vec<Atom>,
    accept: usize,
    start_state: S,
    /// If the pattern ends with `*`, this is the position of that trailing
    /// `Any` atom. Once a state contains this position it self-loops on
    /// every byte and `classify` reports [`StateClass::Permanent`].
    trailing_star: Option<usize>,
    /// Bit pattern that represents `{accept}` — the unique terminal state
    /// for fixed-length patterns.
    accept_only: S,
    /// `Some(table)` if the pattern has `Any` atoms; `None` otherwise
    /// (ε-closure would be a no-op in that case and we skip it).
    epsilon_table: Option<Vec<S>>,
}

impl<S: NfaBitSet> WildcardNfa<S> {
    /// Compile a pattern to an NFA backed by `S`.
    ///
    /// The caller is responsible for picking an `S` whose capacity is large
    /// enough for the pattern's atom count (≤ 63 for `u64`, ≤ 127 for
    /// `u128`, ≤ 255 for [`InlineStateSet`](super::atoms::InlineStateSet),
    /// ≤ `N * 64 - 1` for [`HeapStateSet<N>`](super::atoms::HeapStateSet),
    /// unlimited for [`LargeHeapStateSet`](super::atoms::LargeHeapStateSet)).
    pub fn compile(pattern: &WildcardPattern<'_>) -> Self {
        let atoms = flatten(pattern);
        let accept = atoms.len();
        let epsilon_table = atoms
            .iter()
            .any(|a| matches!(a, Atom::Any))
            .then(|| build_epsilon_table::<S>(&atoms));
        let start_state = initial_state(accept, epsilon_table.as_deref());
        let trailing_star = matches!(atoms.last(), Some(Atom::Any)).then(|| atoms.len() - 1);
        let accept_only = S::singleton(accept, accept);
        Self {
            atoms,
            accept,
            start_state,
            trailing_star,
            accept_only,
            epsilon_table,
        }
    }
}

impl<S: NfaBitSet> Automaton for WildcardNfa<S> {
    type State = S;

    #[inline]
    fn start(&self) -> Self::State {
        self.start_state.clone()
    }

    #[inline]
    fn step(&self, state: &Self::State, byte: u8) -> Option<Self::State> {
        match self.epsilon_table.as_deref() {
            Some(table) => {
                // General path: the state set may contain multiple bits and
                // ε-closure can spread it further.
                let next = nfa_step(&self.atoms, state, byte, Some(table));
                (!next.is_empty()).then_some(next)
            }
            None => {
                // Fixed-length pattern: no `Any` atoms ⇒ the active state is
                // always a singleton, so we can skip the bit-iteration loop
                // and dispatch on the single atom directly. Bounds check on
                // `atoms` is one cmp; not worth the unsafe footgun.
                let pos = state.singleton_pos();
                match self.atoms.get(pos)? {
                    Atom::Byte(b) if *b == byte => Some(S::singleton(self.accept, pos + 1)),
                    Atom::Byte(_) => None,
                    Atom::One => Some(S::singleton(self.accept, pos + 1)),
                    // Unreachable: `epsilon_table` is None iff no `Any` atoms.
                    Atom::Any => None,
                }
            }
        }
    }

    #[inline]
    fn classify(&self, state: &Self::State) -> StateClass {
        // Trailing-`*`: once that position is active the state self-loops on
        // every byte and accept stays in via ε-closure.
        if let Some(pos) = self.trailing_star
            && state.contains(pos)
        {
            return StateClass::Permanent;
        }
        // `{accept}` and only `{accept}` has no outgoing transitions —
        // the unique terminal state for fixed-length patterns.
        if *state == self.accept_only {
            return StateClass::Terminal;
        }
        if state.contains(self.accept) {
            StateClass::LiveAccepting
        } else {
            StateClass::Live
        }
    }

    #[inline]
    fn step_all(&self, state: &Self::State, bytes: &[u8]) -> Option<Self::State> {
        // Fast path for fixed-length patterns: state is always a singleton,
        // so we can pre-extract its position and advance an integer counter
        // per byte instead of going through `step`.
        if self.epsilon_table.is_none() {
            let mut pos = state.singleton_pos();
            for &byte in bytes {
                let advances = match self.atoms.get(pos)? {
                    Atom::Byte(b) => *b == byte,
                    Atom::One => true,
                    // Unreachable: `epsilon_table` is None iff no `Any` atoms.
                    Atom::Any => return None,
                };
                if !advances {
                    return None;
                }
                pos += 1;
            }
            // Returning `Some(singleton(accept))` when `pos == self.accept`
            // is intentional: the driver's `Terminal` branch (see `classify`)
            // yields the value and skips child traversal, so this state is
            // a sink — not a dead state.
            return Some(S::singleton(self.accept, pos));
        }

        // General path: advance through the full ε-closure-aware step.
        //
        // Process the first byte directly off the borrowed `state` so the
        // common short-label case doesn't pay for an upfront `state.clone()`
        // that the very next iteration would overwrite.
        //
        // For multi-byte labels (common with edge-compressed trie edges),
        // allocate one scratch `next` upfront and swap with `s` between
        // bytes. This recycles the bitset's storage instead of allocating
        // a fresh `S::empty(...)` per byte — important for the heap-backed
        // bitsets where every empty is a `Box::new` malloc.
        let Some((&first, rest)) = bytes.split_first() else {
            return Some(state.clone());
        };
        let table = self.epsilon_table.as_deref();
        let mut s = nfa_step(&self.atoms, state, first, table);
        if s.is_empty() {
            return None;
        }
        if rest.is_empty() {
            return Some(s);
        }
        let mut next = S::empty(self.accept);
        for &byte in rest {
            // Invariant: `next` is empty here. On the first iteration the
            // fresh `S::empty` above made it so; on later iterations the
            // `swap` left the just-consumed `s` in `next`, which we clear.
            nfa_step_into(&self.atoms, &s, byte, table, &mut next);
            if next.is_empty() {
                return None;
            }
            std::mem::swap(&mut s, &mut next);
            next.clear();
        }
        Some(s)
    }
}
