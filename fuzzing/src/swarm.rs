/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Swarm testing (Groce et al.): each server instance runs under a different,
//! deterministically-derived set of `search-*` config values, applied via
//! `CONFIG SET` right after startup. The same grammar then exercises wholly
//! different code — worker-pool concurrency, GC policy, numeric/vector index
//! encodings, background-indexer timing — instead of always the one default
//! configuration. Applied via `CONFIG SET` (not load args) so an
//! invalid/immutable value simply errors and is ignored rather than aborting
//! module load. The chosen values are recorded per finding so a crash replays
//! under the same regime.

/// A tiny deterministic PRNG (splitmix64) so swarm selection is reproducible
/// from a seed without pulling in `rand`.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn pick(&mut self, xs: &[&str]) -> String {
        xs[(self.next() % xs.len() as u64) as usize].to_string()
    }
    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.next() % den < num
    }
}

/// Derive a set of `(config, value)` pairs to `CONFIG SET` after startup. Each
/// knob flips a subsystem; a fraction of servers keep the default so the
/// baseline configuration is still exercised. Values that a build rejects (wrong
/// type, immutable) are harmless — the caller ignores `CONFIG SET` errors.
pub fn swarm_config(seed: u64) -> Vec<(String, String)> {
    let mut r = SplitMix(seed ^ 0x5EED_5EED_5EED_5EED);
    let mut cfg: Vec<(String, String)> = Vec::new();
    let mut set = |k: &str, v: String| cfg.push((k.to_string(), v));

    // Worker pool: 0 keeps things single-threaded/deterministic; >0 turns on the
    // concurrent query/index thread pool where C data races live.
    set("search-workers", r.pick(&["0", "0", "2", "4"]));
    if r.chance(1, 2) {
        set("search-min-operation-workers", r.pick(&["1", "2", "4"]));
    }

    // GC aggressiveness: short intervals/thresholds run GC constantly, racing it
    // against queries and indexing.
    if r.chance(2, 3) {
        set(
            "search-fork-gc-run-interval",
            r.pick(&["1", "1", "10", "100"]),
        );
    }
    if r.chance(1, 2) {
        set("search-fork-gc-clean-threshold", r.pick(&["0", "1", "100"]));
    }

    // Numeric index encoding.
    if r.chance(1, 2) {
        set("search-_numeric-compress", r.pick(&["true", "false"]));
    }
    if r.chance(1, 2) {
        set("search-_numeric-ranges-parents", r.pick(&["0", "1", "2"]));
    }

    // Query planner ordering.
    if r.chance(1, 2) {
        set(
            "search-_prioritize-intersect-union-children",
            r.pick(&["true", "false"]),
        );
    }

    // Default query dialect for the whole server.
    if r.chance(1, 2) {
        set("search-default-dialect", r.pick(&["1", "2", "3", "4"]));
    }

    cfg
}
