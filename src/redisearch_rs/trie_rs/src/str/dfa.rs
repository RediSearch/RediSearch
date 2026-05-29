/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! DFA-filtered iteration over a `StrTrieMap`.
//!
//! Port of the C `Trie_Iterate` path: builds a Levenshtein DFA from
//! `(query, max_dist)`, then walks the byte-keyed trie while feeding each
//! consumed character through the DFA in lockstep. Terminals reached with the
//! DFA in an accepting state are yielded along with the minimum edit distance
//! recorded along the matched path.
//!
//! See `src/trie/levenshtein.c` and `src/trie/trie_node.c:619` (`__ti_step`)
//! for the C reference. The semantics pinned by the shared snapshots under
//! `rune_trie_snapshots/tests/integration/snapshots/lex_iterate_*` are:
//!
//! - **Running-min distance.** The reported `distance` is `MIN(state.distance,
//!   minDist)` along the matched path, NOT a fresh Levenshtein recompute of
//!   the yielded term against the query.
//! - **Prefix-mode freeze.** Once a prefix accepts, subsequent chars push
//!   `None` state-stack entries (a "prefix sink"); the dist is preserved at
//!   the value captured at the accept boundary.
//! - **Internal terminals.** A node may be both a terminal AND a parent of
//!   longer terms (e.g. `appl` between `apple`/`apply`); the iterator must
//!   yield it before descending into children.
//!
//! ## Char model
//!
//! The DFA alphabet is `char`: each node's label bytes are decoded as UTF-8
//! once when the trie frame is pushed, and chars are fed through the DFA
//! exactly as found. Case-insensitivity is the caller's responsibility —
//! [`TermDictionary`](crate::str::term_dict::TermDictionary) pre-folds
//! inserted terms and lookup queries via ICU case-folding before they
//! reach the underlying [`StrTrieMap`].

use crate::node::Node;
use crate::str::StrTrieMap;

/// A sparse Levenshtein DP row. Entries are `(idx, val)` sorted by `idx`,
/// holding only positions where `val <= max_edits`. Mirrors the C
/// `sparseVector` from `src/trie/sparse_vector.c`.
#[derive(Clone, PartialEq, Eq)]
struct SparseVector(Vec<(u32, u32)>);

/// Sparse Levenshtein NFA over the query string. The DFA build
/// determinizes this NFA via subset construction (subsets are the sparse
/// vectors themselves; equal vectors collapse to the same DFA node).
struct SparseAutomaton {
    /// Query string as a sequence of `char`s.
    query: Vec<char>,
    /// Maximum allowed edit distance.
    max: u32,
}

impl SparseAutomaton {
    /// Initial DP row: `[(0, 0), (1, 1), ..., (max, max)]`. Mirrors
    /// `SparseAutomaton_Start` (`src/trie/levenshtein.c:36`).
    fn start(&self) -> SparseVector {
        SparseVector((0..=self.max).map(|i| (i, i)).collect())
    }

    /// One DP step. `c = None` represents a "non-query" character — the
    /// fallback edge in the C build that's seeded with `rune 1`
    /// (`src/trie/levenshtein.c:187`).
    fn step(&self, state: &SparseVector, c: Option<char>) -> SparseVector {
        let mut new_vec: Vec<(u32, u32)> = Vec::with_capacity(state.0.len());

        if let Some(&(idx, val)) = state.0.first()
            && idx == 0
            && val < self.max
        {
            new_vec.push((0, val + 1));
        }

        for j in 0..state.0.len() {
            let (idx, val) = state.0[j];

            if idx as usize == self.query.len() {
                break;
            }

            let mut new_val = val;
            let q_char = self.query[idx as usize];
            let same = matches!(c, Some(c) if c == q_char);
            if !same {
                new_val += 1;
            }

            if let Some(&(last_idx, last_val)) = new_vec.last()
                && last_idx == idx
            {
                new_val = new_val.min(last_val + 1);
            }

            if j + 1 < state.0.len() {
                let (next_idx, next_val) = state.0[j + 1];
                if next_idx == idx + 1 {
                    new_val = new_val.min(next_val + 1);
                }
            }

            if new_val <= self.max {
                new_vec.push((idx + 1, new_val));
            }
        }

        SparseVector(new_vec)
    }

    /// True iff the DP row's last entry sits at the query end — i.e. some
    /// alignment consumed the whole query within `max` edits.
    fn is_match(&self, v: &SparseVector) -> bool {
        v.0.last()
            .is_some_and(|&(idx, _)| idx as usize == self.query.len())
    }
}

/// One DFA node. `state` is kept only to drive the build-time cache lookup
/// (linear `__sv_equals` scan — mirrors the C implementation; for tiny
/// queries it's faster than a hash).
struct DfaNode {
    distance: u32,
    is_match: bool,
    edges: Vec<(char, usize)>,
    fallback: Option<usize>,
    state: SparseVector,
}

/// Built Levenshtein DFA. Node 0 is the start state.
struct Dfa {
    nodes: Vec<DfaNode>,
}

impl Dfa {
    fn build(query: &str, max_dist: u32) -> Self {
        let auto = SparseAutomaton {
            query: query.chars().collect(),
            max: max_dist,
        };

        let start_state = auto.start();
        let start_match = auto.is_match(&start_state);
        let mut nodes = vec![DfaNode {
            distance: 0,
            is_match: start_match,
            edges: Vec::new(),
            fallback: None,
            state: start_state,
        }];

        Self::build_rec(0, &auto, &mut nodes);

        Self { nodes }
    }

    /// Direct port of `dfa_build` (`src/trie/levenshtein.c:158`): for each
    /// query position currently active in `parent.state`, follow the edge
    /// for that query char (creating/caching the resulting DFA node); then
    /// always add the wildcard fallback edge.
    fn build_rec(parent_idx: usize, auto: &SparseAutomaton, nodes: &mut Vec<DfaNode>) {
        let parent_state = nodes[parent_idx].state.clone();
        nodes[parent_idx].is_match = auto.is_match(&parent_state);

        for &(idx, _) in &parent_state.0 {
            if (idx as usize) >= auto.query.len() {
                continue;
            }
            let c = auto.query[idx as usize];
            if nodes[parent_idx].edges.iter().any(|&(ec, _)| ec == c) {
                continue;
            }

            let nv = auto.step(&parent_state, Some(c));
            if nv.0.is_empty() {
                continue;
            }

            if let Some(cached) = nodes.iter().position(|n| n.state == nv) {
                nodes[parent_idx].edges.push((c, cached));
            } else {
                let dist = nv.0.last().unwrap().1;
                let new_idx = nodes.len();
                let is_match = auto.is_match(&nv);
                nodes.push(DfaNode {
                    distance: dist,
                    is_match,
                    edges: Vec::new(),
                    fallback: None,
                    state: nv,
                });
                nodes[parent_idx].edges.push((c, new_idx));
                Self::build_rec(new_idx, auto, nodes);
            }
        }

        let nv = auto.step(&parent_state, None);
        if !nv.0.is_empty() {
            if let Some(cached) = nodes.iter().position(|n| n.state == nv) {
                nodes[parent_idx].fallback = Some(cached);
            } else {
                let dist = nv.0.last().unwrap().1;
                let new_idx = nodes.len();
                let is_match = auto.is_match(&nv);
                nodes.push(DfaNode {
                    distance: dist,
                    is_match,
                    edges: Vec::new(),
                    fallback: None,
                    state: nv,
                });
                nodes[parent_idx].fallback = Some(new_idx);
                Self::build_rec(new_idx, auto, nodes);
            }
        }
    }

    fn step_edge(&self, from: usize, c: char) -> Option<usize> {
        let node = &self.nodes[from];
        for &(ec, next) in &node.edges {
            if ec == c {
                return Some(next);
            }
        }
        node.fallback
    }
}

/// One frame in the trie traversal stack, mirroring the C `stackNode` in
/// `src/trie/trie_node.h` (the per-node iterator state inside
/// `__ti_Push`/`__ti_step`).
struct TrieFrame<'tm, Data> {
    node: &'tm Node<Data>,
    /// Label decoded as UTF-8 once at frame push.
    label: Vec<char>,
    /// Current char position within `label`. Mirrors `stringOffset` in C
    /// but in chars, not bytes.
    label_offset: usize,
    /// Next child to descend into when `phase == Children`.
    child_offset: usize,
    phase: FramePhase,
}

#[derive(Clone, Copy)]
enum FramePhase {
    /// Consuming chars from the label, feeding them through the DFA.
    Label,
    /// A match just fired and is being pinned; next step pops the frame.
    /// Mirrors `ITERSTATE_MATCH` in C.
    Match,
    /// Label exhausted; descending into children.
    Children,
}

#[derive(Clone, Copy)]
enum StepOutcome {
    Continue,
    Match,
    Stop,
}

enum FilterCode {
    Continue,
    Stop,
}

/// DFA-filtered trie iterator. Yields `(key, &data, dist)` for each terminal
/// whose key lies within Levenshtein distance `max_dist` of the query — or,
/// in `prefix_mode`, any suffix beneath such an accepted prefix.
///
/// `dist` is the running-min `MIN(dfaNode.distance, minDist)` along the
/// matched path, NOT a fresh Levenshtein recompute. See the module doc.
pub struct IterateDfaIter<'tm, Data> {
    dfa: Dfa,
    prefix_mode: bool,
    trie_stack: Vec<TrieFrame<'tm, Data>>,
    /// Parallel to `dist_stack`. `None` means "in the prefix sink" — the
    /// prefix has accepted and we're just letting subsequent chars through.
    state_stack: Vec<Option<usize>>,
    /// Running min of accept-state distances along the matched path.
    /// Frozen across prefix-sink frames.
    dist_stack: Vec<u32>,
    key: Vec<char>,
    /// Last distance the filter wrote on a match. Seeded to `max_dist + 1`
    /// as a sentinel (`src/trie/trie.c:256`).
    last_dist: u32,
}

impl<'tm, Data> IterateDfaIter<'tm, Data> {
    fn new(root: Option<&'tm Node<Data>>, dfa: Dfa, prefix_mode: bool, max_dist: u32) -> Self {
        let mut iter = Self {
            dfa,
            prefix_mode,
            trie_stack: Vec::new(),
            state_stack: vec![Some(0)],
            dist_stack: vec![max_dist + 1],
            key: Vec::new(),
            last_dist: max_dist + 1,
        };
        if let Some(root) = root {
            iter.push_frame(root);
        }
        iter
    }

    fn push_frame(&mut self, node: &'tm Node<Data>) {
        // SAFETY-of-correctness: the label is required to be UTF-8 by
        // `StrTrieMap`'s insertion path (`insert(key: &str)` byte-encodes
        // the key). Non-UTF-8 bytes are not reachable here.
        let label_str = std::str::from_utf8(node.label()).expect("trie label is UTF-8");
        let label: Vec<char> = label_str.chars().collect();
        self.trie_stack.push(TrieFrame {
            node,
            label,
            label_offset: 0,
            child_offset: 0,
            phase: FramePhase::Label,
        });
    }

    fn pop_frame(&mut self) {
        let Some(frame) = self.trie_stack.pop() else {
            return;
        };
        // One state/dist push per consumed label char; one key char per
        // consumed label char. Frame's `label_offset` is the count of both.
        for _ in 0..frame.label_offset {
            self.state_stack.pop();
            self.dist_stack.pop();
            self.key.pop();
        }
    }

    /// One DFA filter step. Mirrors `FilterFunc` in
    /// `src/trie/levenshtein.c:249` — push the next DFA state onto the
    /// stacks, with the running-min distance, and report whether the new
    /// position is accepting.
    fn filter(
        dfa: &Dfa,
        prefix_mode: bool,
        state_stack: &mut Vec<Option<usize>>,
        dist_stack: &mut Vec<u32>,
        last_dist: &mut u32,
        c: char,
    ) -> (FilterCode, bool) {
        let dn = *state_stack.last().unwrap();
        let min_dist = *dist_stack.last().unwrap();

        if dn.is_none() {
            state_stack.push(None);
            dist_stack.push(min_dist);
            return (FilterCode::Continue, true);
        }

        let dn_idx = dn.unwrap();
        let mut matched = dfa.nodes[dn_idx].is_match;
        if matched {
            *last_dist = dfa.nodes[dn_idx].distance.min(min_dist);
        }

        let next = dfa.step_edge(dn_idx, c);

        if let Some(next_idx) = next {
            let next_node = &dfa.nodes[next_idx];
            if next_node.is_match {
                matched = true;
                *last_dist = next_node.distance.min(min_dist);
            }
            state_stack.push(Some(next_idx));
            dist_stack.push(next_node.distance.min(min_dist));
            return (FilterCode::Continue, matched);
        }

        if prefix_mode && matched {
            state_stack.push(None);
            dist_stack.push(min_dist);
            return (FilterCode::Continue, matched);
        }

        (FilterCode::Stop, matched)
    }

    /// One step of the state machine. Mirrors `__ti_step` in
    /// `src/trie/trie_node.c:619`.
    fn step(&mut self) -> StepOutcome {
        let Some(frame) = self.trie_stack.last_mut() else {
            return StepOutcome::Stop;
        };

        match frame.phase {
            FramePhase::Match => {
                self.pop_frame();
                StepOutcome::Continue
            }
            FramePhase::Label => {
                if frame.label_offset < frame.label.len() {
                    let c = frame.label[frame.label_offset];
                    let (rc, matched) = Self::filter(
                        &self.dfa,
                        self.prefix_mode,
                        &mut self.state_stack,
                        &mut self.dist_stack,
                        &mut self.last_dist,
                        c,
                    );
                    match rc {
                        FilterCode::Stop => {
                            let frame = self.trie_stack.last_mut().unwrap();
                            if matched {
                                frame.phase = FramePhase::Match;
                                StepOutcome::Match
                            } else {
                                self.pop_frame();
                                StepOutcome::Continue
                            }
                        }
                        FilterCode::Continue => {
                            let frame = self.trie_stack.last_mut().unwrap();
                            self.key.push(c);
                            frame.label_offset += 1;
                            if matched {
                                StepOutcome::Match
                            } else {
                                StepOutcome::Continue
                            }
                        }
                    }
                } else {
                    frame.phase = FramePhase::Children;
                    StepOutcome::Continue
                }
            }
            FramePhase::Children => {
                let n_children = frame.node.n_children() as usize;
                if frame.child_offset < n_children {
                    let child_idx = frame.child_offset;
                    frame.child_offset += 1;
                    // Capture `parent_node` at the original `'tm` lifetime
                    // — `frame.node: &'tm Node<Data>` — so calling
                    // `children()` returns a `&'tm [Node<Data>]` and the
                    // child reference outlives the stack mutation below.
                    let parent_node: &'tm Node<Data> = self.trie_stack.last().unwrap().node;
                    let child: &'tm Node<Data> = &parent_node.children()[child_idx];
                    self.push_frame(child);
                } else {
                    self.pop_frame();
                }
                StepOutcome::Continue
            }
        }
    }
}

impl<'tm, Data> Iterator for IterateDfaIter<'tm, Data> {
    type Item = (String, &'tm Data, u32);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.step() {
                StepOutcome::Stop => return None,
                StepOutcome::Continue => continue,
                StepOutcome::Match => {
                    let frame = self.trie_stack.last().expect("Match implies a frame");
                    // Same lifetime-recovery trick as `step`'s children
                    // branch: capture `node` at the original `'tm` so
                    // `node.data()` returns `Option<&'tm Data>`.
                    let node: &'tm Node<Data> = frame.node;
                    if frame.label_offset == frame.label.len()
                        && let Some(data) = node.data()
                    {
                        let key: String = self.key.iter().collect();
                        return Some((key, data, self.last_dist));
                    }
                }
            }
        }
    }
}

impl<Data> StrTrieMap<Data> {
    /// Yield every terminal whose key lies within Levenshtein edit distance
    /// `max_dist` of `query`. With `prefix_mode = true`, any suffix beneath
    /// a matched-prefix node is also yielded.
    ///
    /// The reported `dist` is the running-min `MIN(state.distance, minDist)`
    /// along the matched path — see [`IterateDfaIter`] for details.
    pub fn iterate_dfa<'tm>(
        &'tm self,
        query: &str,
        max_dist: u32,
        prefix_mode: bool,
    ) -> IterateDfaIter<'tm, Data> {
        let dfa = Dfa::build(query, max_dist);
        IterateDfaIter::new(self.inner.root(), dfa, prefix_mode, max_dist)
    }
}
