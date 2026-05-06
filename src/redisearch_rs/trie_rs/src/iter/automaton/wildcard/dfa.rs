/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Pre-built DFA wildcard automaton, constructed via subset construction over
//! the NFA primitives in [`super::nfa`].

use super::atoms::{Atom, StateSet, flatten};
use super::nfa::{build_epsilon_table, initial_state, nfa_step};
use super::super::{Automaton, StateClass};
use wildcard::WildcardPattern;

/// Sentinel for "no transition" in the transition table.
const DEAD: u32 = u32::MAX;

/// Cap on the number of DFA states. Compilation aborts above this and the
/// caller is expected to fall back to NFA simulation.
const DFA_STATE_CAP: usize = 8192;

/// DFA wildcard automaton built via subset construction over the NFA.
pub struct WildcardDfa {
    /// `transitions[s * 256 + b]` is the next DFA state, or `DEAD` if dead.
    transitions: Vec<u32>,
    /// Pre-computed [`StateClass`] for each DFA state — `classify` reduces
    /// to a single indexed lookup.
    class: Vec<StateClass>,
    start: u32,
}

impl WildcardDfa {
    /// Compile to a DFA.
    ///
    /// Returns `None` if either the pattern exceeds the per-bitset atom cap
    /// (see [`flatten`]) or subset construction exceeds the DFA state cap.
    pub fn compile(pattern: &WildcardPattern<'_>) -> Option<Self> {
        let atoms = flatten(pattern)?;
        let accept = atoms.len();
        let epsilon_table = atoms
            .iter()
            .any(|a| matches!(a, Atom::Any))
            .then(|| build_epsilon_table(&atoms));
        let start_set = initial_state(epsilon_table.as_deref());

        let mut state_id: std::collections::HashMap<StateSet, u32> =
            std::collections::HashMap::new();
        let mut states: Vec<StateSet> = Vec::new();
        let mut transitions: Vec<u32> = Vec::new();

        // Insert start state.
        state_id.insert(start_set, 0);
        states.push(start_set);
        transitions.extend(std::iter::repeat_n(DEAD, 256));

        // Subset construction: for each pending DFA state, compute its 256
        // byte-transitions, allocating new DFA states for previously-unseen
        // NFA state-sets as we go.
        let mut idx = 0;
        while idx < states.len() {
            if states.len() > DFA_STATE_CAP {
                return None;
            }
            let current = states[idx];
            for b in 0u8..=255 {
                let next = nfa_step(&atoms, current, b, epsilon_table.as_deref());
                let transition = if next.is_empty() {
                    DEAD
                } else if let Some(&id) = state_id.get(&next) {
                    id
                } else {
                    let id = states.len() as u32;
                    state_id.insert(next, id);
                    states.push(next);
                    transitions.extend(std::iter::repeat_n(DEAD, 256));
                    id
                };
                transitions[idx * 256 + b as usize] = transition;
            }
            idx += 1;
        }

        // Pre-compute the StateClass for each DFA state.
        let class = states
            .iter()
            .enumerate()
            .map(|(s, set)| {
                let accepting = set.contains(accept);
                if !accepting {
                    return StateClass::Live;
                }
                let row = &transitions[s * 256..(s + 1) * 256];
                if row.iter().all(|&t| t as usize == s) {
                    StateClass::Permanent
                } else if row.iter().all(|&t| t == DEAD) {
                    StateClass::Terminal
                } else {
                    StateClass::LiveAccepting
                }
            })
            .collect();

        Some(Self {
            transitions,
            class,
            start: 0,
        })
    }

    /// Number of DFA states — useful for diagnostics.
    pub const fn num_states(&self) -> usize {
        self.class.len()
    }
}

impl Automaton for WildcardDfa {
    type State = u32;

    #[inline]
    fn start(&self) -> Self::State {
        self.start
    }

    #[inline]
    fn step(&self, state: &Self::State, byte: u8) -> Option<Self::State> {
        // SAFETY: every `Self::State` value is produced by this DFA — either as
        // `self.start` (= 0) or by a previous `step` returning a valid state ID.
        // State IDs are bounded by `self.class.len()`, and the transitions
        // table is sized `class.len() * 256`, so `state * 256 + byte` is
        // always a valid index. `byte` is `u8`, so it's always < 256.
        let next = unsafe {
            *self
                .transitions
                .get_unchecked((*state as usize) * 256 + byte as usize)
        };
        if next == DEAD { None } else { Some(next) }
    }

    #[inline]
    fn classify(&self, state: &Self::State) -> StateClass {
        // SAFETY: see `step`. State IDs are always valid indices into `class`.
        unsafe { *self.class.get_unchecked(*state as usize) }
    }
}
