/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! NFA-simulation wildcard automaton.

use super::atoms::{Atom, StateSet, flatten};
use super::super::{Automaton, StateClass};
use wildcard::WildcardPattern;

/// Build the per-position ε-closure table.
///
/// `epsilon_table[i]` is the set of positions reachable from `{i}` by zero or
/// more `*`-skip edges. This includes `i` itself.
///
/// Computed right-to-left so each entry just unions in the next one when the
/// current atom is `Any`.
pub(super) fn build_epsilon_table(atoms: &[Atom]) -> Vec<StateSet> {
    let n = atoms.len();
    let mut table = vec![StateSet::empty(); n + 1];
    table[n] = StateSet::singleton(n);
    for i in (0..n).rev() {
        let mut s = StateSet::singleton(i);
        if matches!(atoms[i], Atom::Any) {
            s = s.union(table[i + 1]);
        }
        table[i] = s;
    }
    table
}

/// Compute the ε-closure of `state` via a precomputed table.
fn epsilon_closure(epsilon_table: &[StateSet], state: StateSet) -> StateSet {
    let mut result = state;
    for pos in state.iter() {
        // SAFETY: positions in a state are always valid atom indices `0..=n`,
        // matching the table's length `n + 1`.
        result = result.union(unsafe { *epsilon_table.get_unchecked(pos) });
    }
    result
}

/// The NFA's start state — `{0}`, optionally extended via ε-closure when the
/// pattern has `Any` atoms.
pub(super) fn initial_state(epsilon_table: Option<&[StateSet]>) -> StateSet {
    let s = StateSet::singleton(0);
    match epsilon_table {
        Some(table) => epsilon_closure(table, s),
        None => s,
    }
}

/// Step an NFA state by one input byte.
///
/// `epsilon_table` may be `None` for patterns with no `Any` atoms — in that
/// case ε-closure is a no-op and we skip it entirely.
pub(super) fn nfa_step(
    atoms: &[Atom],
    state: StateSet,
    byte: u8,
    epsilon_table: Option<&[StateSet]>,
) -> StateSet {
    let mut next = StateSet::empty();
    for pos in state.iter() {
        // The accept position (`pos == atoms.len()`) has no atom and no
        // outgoing transition — it's a sink. Any survival from here happens
        // through other active positions in the same state set.
        if pos >= atoms.len() {
            continue;
        }
        match atoms[pos] {
            Atom::Byte(b) if b == byte => next.insert(pos + 1),
            Atom::Byte(_) => {}
            Atom::One => next.insert(pos + 1),
            Atom::Any => next.insert(pos),
        }
    }
    match epsilon_table {
        Some(table) => epsilon_closure(table, next),
        None => next,
    }
}

/// NFA-simulation wildcard automaton.
pub struct WildcardNfa {
    atoms: Vec<Atom>,
    accept: usize,
    start_state: StateSet,
    /// If the pattern ends with `*`, this is the position of that trailing
    /// `Any` atom. Once a state contains this position it self-loops on every
    /// byte and `classify` reports [`StateClass::Permanent`].
    trailing_star: Option<usize>,
    /// Bit pattern that represents `{accept}` — the unique terminal state for
    /// fixed-length patterns.
    accept_only: StateSet,
    /// `Some(table)` if the pattern has `Any` atoms; `None` otherwise (ε-closure
    /// would be a no-op in that case and we skip it).
    epsilon_table: Option<Vec<StateSet>>,
}

impl WildcardNfa {
    /// Compile a pattern to an NFA. Returns `None` if the pattern exceeds
    /// the per-bitset atom cap — see [`flatten`].
    pub fn compile(pattern: &WildcardPattern<'_>) -> Option<Self> {
        let atoms = flatten(pattern)?;
        let accept = atoms.len();
        let epsilon_table = atoms
            .iter()
            .any(|a| matches!(a, Atom::Any))
            .then(|| build_epsilon_table(&atoms));
        let start_state = initial_state(epsilon_table.as_deref());
        let trailing_star = matches!(atoms.last(), Some(Atom::Any)).then(|| atoms.len() - 1);
        let accept_only = StateSet::singleton(accept);
        Some(Self {
            atoms,
            accept,
            start_state,
            trailing_star,
            accept_only,
            epsilon_table,
        })
    }
}

impl Automaton for WildcardNfa {
    type State = StateSet;

    #[inline]
    fn start(&self) -> Self::State {
        self.start_state
    }

    #[inline]
    fn step(&self, state: &Self::State, byte: u8) -> Option<Self::State> {
        match self.epsilon_table.as_deref() {
            Some(table) => {
                // General path: the state set may contain multiple bits and
                // ε-closure can spread it further.
                let next = nfa_step(&self.atoms, *state, byte, Some(table));
                (!next.is_empty()).then_some(next)
            }
            None => {
                // Fixed-length pattern: no `Any` atoms ⇒ the active state is
                // always a singleton, so we can skip the bit-iteration loop
                // and dispatch on the single atom directly.
                let pos = state.0.trailing_zeros() as usize;
                if pos >= self.atoms.len() {
                    return None;
                }
                // SAFETY: we just bounds-checked `pos` against `atoms.len()`.
                let atom = unsafe { *self.atoms.get_unchecked(pos) };
                match atom {
                    Atom::Byte(b) if b == byte => Some(StateSet::singleton(pos + 1)),
                    Atom::Byte(_) => None,
                    Atom::One => Some(StateSet::singleton(pos + 1)),
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
        // per byte instead of going through `step` (which re-extracts on
        // every call) and the `Option<StateSet>` packaging.
        if self.epsilon_table.is_none() {
            let mut pos = state.0.trailing_zeros() as usize;
            for &byte in bytes {
                if pos >= self.atoms.len() {
                    return None;
                }
                // SAFETY: bounds-checked just above.
                let atom = unsafe { *self.atoms.get_unchecked(pos) };
                let advances = match atom {
                    Atom::Byte(b) => b == byte,
                    Atom::One => true,
                    // Unreachable: `epsilon_table` is None iff no `Any` atoms.
                    Atom::Any => return None,
                };
                if !advances {
                    return None;
                }
                pos += 1;
            }
            return Some(StateSet::singleton(pos));
        }

        // General path: advance through the full ε-closure-aware step.
        let mut s = *state;
        for &byte in bytes {
            let next = nfa_step(&self.atoms, s, byte, self.epsilon_table.as_deref());
            if next.is_empty() {
                return None;
            }
            s = next;
        }
        Some(s)
    }
}
