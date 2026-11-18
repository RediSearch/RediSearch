/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A plain search loop: keep every program that shows a new feature, and
//! mutate one picked uniformly, with a donor picked the same way.

use std::fmt::Display;
use std::hash::{DefaultHasher, Hash, Hasher};

use super::{Campaign, Halt, Op, Outcome, Runner, load_programs};
use crate::draw::Draw;
use crate::error::Error;
use crate::findings::to_json;
use crate::model::Program;
use crate::system::Feature;

/// Longer mutants are cut to this many ops.
const MOST_OPS: usize = 40;

pub(super) fn search<O: Op, E: Display, F>(
    campaign: &Campaign<'_>,
    mut runner: Runner<'_, F, O>,
    mut mutate: impl FnMut(&Program<O>, &Program<O>, &mut Draw) -> Program<O>,
) -> Result<Outcome, Error>
where
    F: FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
{
    std::fs::create_dir_all(campaign.corpus)?;
    let mut dirs = campaign.seeds.to_vec();
    dirs.push(campaign.corpus.to_owned());
    let mut kept: Vec<Program<O>> = load_programs(&dirs);
    if kept.is_empty() {
        kept.push(Program::default());
    }
    let mut rng = Draw::new(rand_seed());
    let halt = 'search: {
        for program in &kept {
            if let Err(halt) = runner.observe(program) {
                break 'search halt;
            }
        }
        loop {
            kept.extend(runner.imports().into_iter().map(|(program, _)| program));
            let parent = &kept[rng.index(kept.len())];
            let donor = &kept[rng.index(kept.len())];
            let mut child = mutate(parent, donor, &mut rng);
            child.ops.truncate(MOST_OPS);
            match runner.observe(&child) {
                Ok(Some(observed)) if observed.new => {
                    std::fs::write(campaign.corpus.join(name(&child)), to_json(&child)?)?;
                    kept.push(child);
                }
                Ok(_) => {}
                Err(halt) => break 'search halt,
            }
        }
    };
    match halt {
        Halt::Done => Ok(Outcome {
            runs: runner.runs,
            kept: kept.len(),
        }),
        Halt::Failed(e) => Err(Error::Failing(e)),
    }
}

fn name<O: Op>(program: &Program<O>) -> String {
    let mut hasher = DefaultHasher::new();
    program.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn rand_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos() & u128::from(u64::MAX)).unwrap_or(0)
        })
}
