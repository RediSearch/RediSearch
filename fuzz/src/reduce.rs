/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::edit::smaller;
use crate::judge::{Finding, judge};
use crate::program::Program;
use crate::run::run;
use crate::target::Env;

/// Shrinks `program` while a fresh run of it still yields `finding`. After a
/// smaller candidate is kept, the search resumes at the same position in the
/// new candidate list, and it ends after a full pass keeps nothing. Each
/// candidate runs once: a racy finding can shrink into a program that hits it
/// less often, which the caller measures afterwards.
pub fn reduce(
    program: &Program,
    finding: &Finding,
    env: &Env,
    mut kept: impl FnMut(&Program),
) -> Program {
    let mut best = program.clone();
    let mut start = 0;
    loop {
        let candidates = smaller(&best);
        let start_at = start.min(candidates.len());
        let found = (start_at..candidates.len())
            .chain(0..start_at)
            .find(|&i| reproduces(&candidates[i], finding, env));
        match found {
            Some(i) => {
                best = candidates[i].clone();
                kept(&best);
                start = i;
            }
            None => return best,
        }
    }
}

fn reproduces(program: &Program, finding: &Finding, env: &Env) -> bool {
    run(program, env).is_ok_and(|trace| judge(program, &trace).contains(finding))
}
