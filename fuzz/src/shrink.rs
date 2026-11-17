/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::judge::{Finding, execute};
use crate::program::{Literal, OpKind, Program, Query};
use crate::target::Env;

/// Shrinks `program` while a fresh run still yields `finding`. After keeping
/// a candidate, the search resumes at the same position in the new candidate
/// list; it ends when a full pass keeps nothing. Each candidate runs once, so
/// a racy finding may shrink into a program that hits it less often.
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
        let reproduces = |candidate: &Program| {
            execute(candidate, env).is_ok_and(|(_, findings)| findings.contains(finding))
        };
        match (start_at..candidates.len())
            .chain(0..start_at)
            .find(|&i| reproduces(&candidates[i]))
        {
            Some(i) => {
                best = candidates[i].clone();
                kept(&best);
                start = i;
            }
            None => return best,
        }
    }
}

/// Every program one shrinking step away, largest steps first. Each is
/// strictly smaller by [`size`], so [`reduce`] terminates.
pub fn smaller(program: &Program) -> Vec<Program> {
    let edited = |edit: &dyn Fn(&mut Program)| {
        let mut candidate = program.clone();
        edit(&mut candidate);
        candidate
    };
    let mut out = Vec::new();
    if !program.module_args.is_empty() {
        out.push(edited(&|p| p.module_args.clear()));
    }
    let n = program.ops.len();
    let mut chunk = n;
    while chunk > 0 {
        for start in (0..n).step_by(chunk) {
            out.push(edited(&|p| {
                p.ops.drain(start..(start + chunk).min(n));
            }));
        }
        chunk /= 2;
    }
    for (at, op) in program.ops.iter().enumerate() {
        for kind in one_step_smaller(&op.kind) {
            out.push(edited(&|p| p.ops[at].kind = kind.clone()));
        }
    }
    out
}

/// The op with a field, query node, or token fewer, half the documents, or
/// one literal shortened.
fn one_step_smaller(kind: &OpKind) -> Vec<OpKind> {
    let edited = |edit: &dyn Fn(&mut OpKind)| {
        let mut candidate = kind.clone();
        edit(&mut candidate);
        candidate
    };
    let mut out = Vec::new();
    match kind {
        OpKind::Create { fields, .. } | OpKind::Populate { fields, .. } => {
            for i in 0..fields.len() {
                out.push(edited(&|k| {
                    if let OpKind::Create { fields, .. } | OpKind::Populate { fields, .. } = k {
                        fields.remove(i);
                    }
                }));
            }
            if let OpKind::Populate { n, .. } = kind
                && *n > 1
            {
                out.push(edited(&|k| {
                    if let OpKind::Populate { n, .. } = k {
                        *n /= 2;
                    }
                }));
            }
        }
        OpKind::Query { query, .. } => {
            for simpler in simpler_queries(query) {
                out.push(edited(&|k| {
                    if let OpKind::Query { query, .. } = k {
                        *query = simpler.clone();
                    }
                }));
            }
        }
        OpKind::Command { argv } if argv.len() > 1 => {
            for i in 0..argv.len() {
                out.push(edited(&|k| {
                    if let OpKind::Command { argv } = k {
                        argv.remove(i);
                    }
                }));
            }
        }
        _ => {}
    }
    for i in 0..kind.clone().literals().len() {
        for to_empty in [true, false] {
            let mut candidate = kind.clone();
            let changed = match candidate.literals().swap_remove(i) {
                Literal::Word(word) => {
                    let len = if to_empty { 0 } else { word.0.len() / 2 };
                    let changed = len < word.0.len() && (to_empty || len > 0);
                    word.0.truncate(len);
                    changed
                }
                Literal::Number(n) => {
                    let shorter = if to_empty { 0 } else { *n / 2 };
                    let changed = shorter != *n && (to_empty || shorter != 0);
                    *n = shorter;
                    changed
                }
            };
            if changed {
                out.push(candidate);
            }
        }
    }
    out
}

/// The query with a node replaced by one of its children, or a child of a
/// group dropped, anywhere in the tree.
fn simpler_queries(query: &Query) -> Vec<Query> {
    match query {
        Query::And(children) | Query::Or(children) => {
            let rebuild = |children| match query {
                Query::And(_) => Query::And(children),
                _ => Query::Or(children),
            };
            let mut out = children.clone();
            for (i, child) in children.iter().enumerate() {
                let mut fewer = children.clone();
                fewer.remove(i);
                out.push(rebuild(fewer));
                for simpler in simpler_queries(child) {
                    let mut replaced = children.clone();
                    replaced[i] = simpler;
                    out.push(rebuild(replaced));
                }
            }
            out
        }
        Query::Not(child) | Query::Knn { filter: child, .. } => {
            let mut out = vec![(**child).clone()];
            for simpler in simpler_queries(child) {
                let mut query = query.clone();
                if let Query::Not(child) | Query::Knn { filter: child, .. } = &mut query {
                    **child = simpler;
                }
                out.push(query);
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Counts ops, module arguments, schema fields, query nodes, populated
/// documents, and the length of every literal.
pub fn size(program: &Program) -> usize {
    let mut total = program.ops.len() + program.module_args.len();
    for op in &program.ops {
        total += match &op.kind {
            OpKind::Create { fields, .. } => fields.len(),
            OpKind::Populate { fields, n, .. } => fields.len() + usize::try_from(*n).expect("fits"),
            OpKind::Query { query, .. } => nodes(query),
            _ => 0,
        };
        for literal in op.kind.clone().literals() {
            total += 1 + match literal {
                Literal::Word(word) => word.0.len(),
                Literal::Number(n) => {
                    usize::try_from(64 - n.unsigned_abs().leading_zeros()).expect("at most 64")
                }
            };
        }
    }
    total
}

fn nodes(query: &Query) -> usize {
    1 + match query {
        Query::And(children) | Query::Or(children) => children.iter().map(nodes).sum(),
        Query::Not(child) | Query::Knn { filter: child, .. } => nodes(child),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    use super::*;
    use crate::generate::generate;
    use crate::mutate::mutate;

    const PROGRAMS: u64 = 30;

    #[test]
    fn mutants_round_trip_and_every_shrinking_step_is_strictly_smaller() {
        let mut rng = SmallRng::seed_from_u64(0);
        for _ in 0..PROGRAMS {
            let donor = generate(&mut rng);
            let program = mutate(&generate(&mut rng), &donor, &mut rng);
            assert_eq!(Program::from_json(&program.to_json()).unwrap(), program);
            for candidate in smaller(&program) {
                assert!(size(&candidate) < size(&program), "{candidate:?}");
            }
        }
    }
}
