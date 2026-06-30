/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`WildcardNfa<S>`]: the wildcard NFA, generic over the [`NfaBitSet`]
//! width. The [parent module doc](super) walks through positions,
//! ε-closure, and the worked `*ab*` example this code implements.
//!
//! Two preprocessing steps run once per pattern at
//! [`WildcardNfa::compile`]:
//!
//! 1. [`flatten`] turns the parsed pattern into a flat
//!    [`Atom`] sequence.
//! 2. [`build_epsilon_table`] precomputes, for each position, the full
//!    ε-closure of that position. After a per-byte transition lands the
//!    NFA on a position `target`, the hot loop reads `table[target]`
//!    and unions it into the destination set — no recursive ε-walk at
//!    run time.

use super::super::{Automaton, StateClass};
use super::atoms::{Atom, flatten};
use super::nfa_bit_set::NfaBitSet;
use rqe_wildcard::WildcardPattern;

/// One row of the ε-closure table.
///
/// In a typical pattern most rows are one-bit sets: the ε-closure of a
/// non-`*` position is just `{i}` itself (no ε-moves emanate from a
/// `Byte` or `One` atom), and the accept row is just `{n_atoms}`. Only
/// `*` positions ε-close into a multi-element set. Encoding the cheap
/// case inline keeps the table compact (one `usize` per row, not one
/// `S`) and lets [`nfa_step_into`] pick the cheap `Singleton` arm in
/// the dominant case.
pub(super) enum EpsilonRow<S> {
    /// One-bit row at the named position — the ε-closure is `{p}`.
    Singleton(usize),
    /// Multi-bit row, constructed only for `*` positions.
    Set(S),
}

/// Precompute the ε-closure of every position.
///
/// Walked right-to-left so the closure of position `i` (when atom `i`
/// is a `*`) is just `{i}` unioned with whatever the closure of `i + 1`
/// already says. Non-`*` positions get [`EpsilonRow::Singleton`]
/// with no `S` allocated; `*` positions get [`EpsilonRow::Set`] with a
/// freshly built `S`. The trailing accept row (index `n`) is the
/// singleton `{n}`.
pub(super) fn build_epsilon_table<S: NfaBitSet>(atoms: &[Atom]) -> Vec<EpsilonRow<S>> {
    let n = atoms.len();
    // Build right-to-left into the table, then reverse in place. Each
    // iteration only ever reads the most recently pushed entry
    // (`table[i+1]` in the final index space).
    let mut table: Vec<EpsilonRow<S>> = Vec::with_capacity(n + 1);
    table.push(EpsilonRow::Singleton(n));
    for i in (0..n).rev() {
        let entry = if matches!(atoms[i], Atom::Any) {
            // ε-closure of `{i}` spans more than one position; allocate.
            let mut s = S::singleton(n, i);
            // `table` has at least one element (the accept row
            // pushed above, plus whatever we've pushed in earlier loop
            // iterations).
            match table.last().unwrap() {
                EpsilonRow::Singleton(p) => s.insert(*p),
                EpsilonRow::Set(other) => s.union_in_place(other),
            }
            EpsilonRow::Set(s)
        } else {
            EpsilonRow::Singleton(i)
        };
        table.push(entry);
    }
    table.reverse();
    table
}

/// The start state: the ε-closure of `{0}`. Built once at NFA-construction time
/// and cloned at the top of every traversal.
pub(super) fn initial_state<S: NfaBitSet>(
    n_atoms: usize,
    epsilon_table: Option<&[EpsilonRow<S>]>,
) -> S {
    match epsilon_table {
        Some(table) => match &table[0] {
            // First atom isn't `*` — ε-closure is just `{0}`. Build it
            // on demand; this runs once per iterator.
            EpsilonRow::Singleton(p) => S::singleton(n_atoms, *p),
            EpsilonRow::Set(s) => s.clone(),
        },
        // No `*` anywhere → no ε-closure to apply.
        None => S::singleton(n_atoms, 0),
    }
}

/// Advance the NFA by one input byte, writing the resulting active set
/// into `next`. `next` must already be empty on entry — the caller owns
/// it and recycles it across the bytes of a multi-byte label, which is
/// where the no-per-byte-allocation property comes from.
///
/// For every active position in `state`, the body matches on
/// `atoms[pos]` to compute the next-position `target` (or kills the
/// branch on a literal mismatch), then unions in `target`'s ε-closure
/// from `epsilon_table`. The two arms differ only in whether the
/// ε-closure step is needed:
///
/// - `Some(table)` — pattern contains at least one `*`. The hot loop
///   dispatches on each closure row: singletons are one `insert`, the
///   rarer multi-bit rows pay one `union_in_place`. Most patterns hit
///   the cheap arm overwhelmingly often.
/// - `None` — no `*` in the pattern, so ε-closure is the identity and
///   we just `next.insert(target)` directly.
pub(super) fn nfa_step_into<S: NfaBitSet>(
    atoms: &[Atom],
    state: &S,
    byte: u8,
    epsilon_table: &[EpsilonRow<S>],
    next: &mut S,
) {
    debug_assert!(next.is_empty());
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
        let row = unsafe { epsilon_table.get_unchecked(target) };
        match row {
            EpsilonRow::Singleton(p) => next.insert(*p),
            EpsilonRow::Set(s) => next.union_in_place(s),
        }
    }
}

/// Allocating wrapper around [`nfa_step_into`] for one-off transitions
/// where there's no scratch buffer to recycle.
pub(super) fn nfa_step<S: NfaBitSet>(
    atoms: &[Atom],
    state: &S,
    byte: u8,
    epsilon_table: &[EpsilonRow<S>],
) -> S {
    let mut next = S::empty(atoms.len());
    nfa_step_into(atoms, state, byte, epsilon_table, &mut next);
    next
}

/// Wildcard NFA, parameterised over the [`NfaBitSet`] width.
///
/// Holds the flattened atom sequence, the precomputed ε-closure table
/// (absent if the pattern has no `*`), and a couple of cached
/// bit-patterns that let [`Self::classify`] short-circuit the two
/// special accept shapes (permanent, terminal).
pub struct WildcardNfa<S: NfaBitSet> {
    atoms: Vec<Atom>,
    /// The accept position — atoms.len(). A state set containing this
    /// position means the whole pattern has matched.
    accept: usize,
    /// Cached ε-closure of `{0}`. Cloned at the top of every traversal.
    start_state: S,
    /// Position of the trailing `*` if the pattern ends in one. Once a
    /// state set contains this position the NFA self-loops forever and
    /// every descendant in the trie is a match —
    /// [`Self::classify`] reports [`StateClass::Permanent`] in that case.
    trailing_star: Option<usize>,
    /// Pre-built `{accept}` singleton — the unique terminal state for
    /// patterns with no `*` (fixed-length matches). Compared against by
    /// equality in [`Self::classify`].
    accept_only: S,
    /// Precomputed ε-closure rows, one per atom plus the accept row.
    /// `None` for patterns with no `*` — ε-closure would be the
    /// identity and the per-byte loop takes a singleton fast path.
    epsilon_table: Option<Vec<EpsilonRow<S>>>,
}

impl<S: NfaBitSet> WildcardNfa<S> {
    /// Compile a pattern into an NFA backed by `S`.
    ///
    /// Callers should match the width to the atom count: `S = u64` for
    /// ≤ 63 atoms, `S = u128` for ≤ 127.
    ///
    /// # Panics
    ///
    /// Panics if `pattern.atom_count() >= S::CAPACITY`. The accept
    /// position lives at index `n_atoms`, so the bitset must hold
    /// `n_atoms + 1` distinct positions; otherwise the per-byte `1 << pos`
    /// in [`NfaBitSet::singleton`] / [`NfaBitSet::insert`] would shift
    /// past the backend's width and silently corrupt the active set in
    /// release builds (the underlying shift wraps modulo the type width
    /// in Rust). Going through [`super::WildcardSpecializedIter`]
    /// guarantees this never fires.
    pub(crate) fn compile(pattern: &WildcardPattern<'_>) -> Self {
        assert!(
            pattern.atom_count() < S::CAPACITY,
            "WildcardNfa backend has capacity {} but pattern needs {} atoms; route through WildcardSpecializedIter for automatic backend selection",
            S::CAPACITY,
            pattern.atom_count(),
        );
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
    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State> {
        match self.epsilon_table.as_deref() {
            Some(table) => {
                // General path: the state set may contain multiple bits and
                // ε-closure can spread it further.
                let next = nfa_step(&self.atoms, state, byte, table);
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
    fn step_all(&mut self, state: &Self::State, bytes: &[u8]) -> Option<Self::State> {
        // Fast path for fixed-length patterns: state is always a singleton,
        // so we can pre-extract its position and advance an integer counter
        // per byte instead of going through `step`.
        let Some(epsilon_table) = &self.epsilon_table else {
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
        };

        // General path: advance through the full ε-closure-aware step.
        //
        // Process the first byte directly off the borrowed `state` so the
        // common short-label case doesn't pay for an upfront `state.clone()`
        // that the very next iteration would overwrite.
        //
        // For multi-byte labels,
        // allocate one scratch `next` upfront and swap with `s` between
        // bytes. This recycles the bitset's storage instead of producing
        // a fresh `S::empty(...)` per byte.
        let Some((&first, rest)) = bytes.split_first() else {
            return Some(state.clone());
        };
        let mut s = nfa_step(&self.atoms, state, first, epsilon_table);
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
            nfa_step_into(&self.atoms, &s, byte, epsilon_table, &mut next);
            if next.is_empty() {
                return None;
            }
            std::mem::swap(&mut s, &mut next);
            next.clear();
        }
        Some(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `u64` backend can hold positions `0..=63`, so 63 atoms (with
    /// the accept position at 63) is the upper bound. Patterns with more
    /// must be rejected to avoid silently-corrupted bit shifts in
    /// release.
    #[test]
    #[should_panic(expected = "WildcardNfa backend has capacity 64")]
    fn compile_u64_rejects_64_atom_pattern() {
        // 64 literal bytes → 64 atoms → one too many for u64.
        let pattern = WildcardPattern::parse(&[b'a'; 64]);
        let _ = WildcardNfa::<u64>::compile(&pattern);
    }

    #[test]
    #[should_panic(expected = "WildcardNfa backend has capacity 128")]
    fn compile_u128_rejects_128_atom_pattern() {
        let pattern = WildcardPattern::parse(&[b'a'; 128]);
        let _ = WildcardNfa::<u128>::compile(&pattern);
    }

    /// Boundary check: the largest pattern that *does* fit in each
    /// backend should compile without panicking.
    #[test]
    fn compile_accepts_max_fitting_pattern() {
        let p64 = WildcardPattern::parse(&[b'a'; 63]);
        let _ = WildcardNfa::<u64>::compile(&p64);
        let p128 = WildcardPattern::parse(&[b'a'; 127]);
        let _ = WildcardNfa::<u128>::compile(&p128);
    }
}
