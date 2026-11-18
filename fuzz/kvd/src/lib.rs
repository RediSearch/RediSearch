/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The model of the `kvd` test server, and `kvd` as a service.

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use procfuzz::draw::Draw;
use procfuzz::model::Model;
use procfuzz::service::{Input, Service, Timing};
use procfuzz::system::{Notes, Step, in_completion_order};
use procfuzz_redis::resp::{Argv, Resp2, Value, argv};
use serde::{Deserialize, Serialize};

const KEYS: u64 = 6;
const LISTS: u64 = 2;
const SLOTS: u64 = 2;
const VALUES: u64 = 4;
/// Connections other than the default one, for ops that block.
const OTHER_CONNS: u64 = 2;

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    Set {
        key: u8,
        val: u8,
    },
    Get {
        key: u8,
    },
    Del {
        key: u8,
    },
    Push {
        list: u8,
        val: u8,
    },
    Pop {
        list: u8,
    },
    DelList {
        list: u8,
    },
    Open {
        slot: u8,
    },
    Next {
        slot: u8,
    },
    Close {
        slot: u8,
    },
    /// Blocks on another connection; its reply is read later.
    BPop {
        conn: u8,
        list: u8,
    },
    Settle,
    /// A launch flag; it counts only before every other op.
    Compact,
}

#[derive(Clone, Default)]
pub struct State {
    started: bool,
    compact: bool,
    vals: BTreeMap<u8, u8>,
    /// The id the server chose for each open scan, once its reply is read.
    scans: BTreeMap<u8, Option<i64>>,
}

pub struct Kvd;

impl Model for Kvd {
    type Op = Op;
    type State = State;
    type Input = Input<Argv>;
    type Reply = Value;

    fn next(&self, state: &State, draw: &mut Draw) -> Op {
        let known: Vec<u8> = state.vals.keys().copied().collect();
        let scans: Vec<u8> = state.scans.keys().copied().collect();
        let key = |draw: &mut Draw| match draw.below(4) {
            0 => small(draw.below(KEYS)),
            _ => draw.pick(&known).copied().unwrap_or(0),
        };
        let list = |draw: &mut Draw| small(draw.below(LISTS));
        let conn = |draw: &mut Draw| 1 + small(draw.below(OTHER_CONNS));
        let kinds = if state.started { 11 } else { 12 };
        match draw.below(kinds) {
            0 => Op::Set {
                key: key(draw),
                val: small(draw.below(VALUES)),
            },
            1 => Op::Get { key: key(draw) },
            2 => Op::Del { key: key(draw) },
            3 => Op::Push {
                list: list(draw),
                val: small(draw.below(VALUES)),
            },
            4 => Op::Pop { list: list(draw) },
            5 => Op::DelList { list: list(draw) },
            6 => Op::Open {
                slot: small(draw.below(SLOTS)),
            },
            7 => draw
                .pick(&scans)
                .map_or(Op::Open { slot: 0 }, |&slot| Op::Next { slot }),
            8 => draw
                .pick(&scans)
                .map_or(Op::Open { slot: 0 }, |&slot| Op::Close { slot }),
            9 => Op::BPop {
                conn: conn(draw),
                list: list(draw),
            },
            10 => Op::Settle,
            _ => Op::Compact,
        }
    }

    fn apply(&self, state: &mut State, op: &Op, reply: Option<&Value>) {
        let ok = !matches!(reply, Some(Value::Error(_)));
        match *op {
            Op::Compact => {
                state.compact |= !state.started;
                return;
            }
            Op::Set { key, val } if ok => {
                state.vals.insert(key, val);
            }
            Op::Del { key } if ok => {
                state.vals.remove(&key);
            }
            Op::Open { slot } if ok => {
                let id = match reply {
                    Some(Value::Int(id)) => Some(*id),
                    _ => None,
                };
                state.scans.insert(slot, id);
            }
            Op::Close { slot } if ok => {
                state.scans.remove(&slot);
            }
            _ => {}
        }
        state.started = true;
    }

    fn lower(&self, state: &State, op: &Op) -> Input<Argv> {
        let send = |words: &[&str]| Input::Send {
            conn: 0,
            wait: true,
            requests: vec![argv(words)],
        };
        let key = |key: &u8| format!("k{key}");
        let list = |list: &u8| format!("l{list}");
        let id = |slot: &u8| {
            let id = state.scans.get(slot).copied().flatten();
            id.unwrap_or(0).to_string()
        };
        match op {
            Op::Compact => Input::Boot(vec!["--compact".to_owned()]),
            Op::Set { key: k, val } => send(&["SET", &key(k), &val.to_string()]),
            Op::Get { key: k } => send(&["GET", &key(k)]),
            Op::Del { key: k } => send(&["DEL", &key(k)]),
            Op::Push { list: l, val } => send(&["PUSH", &list(l), &val.to_string()]),
            Op::Pop { list: l } => send(&["POP", &list(l)]),
            Op::DelList { list: l } => send(&["DEL", &list(l)]),
            Op::Open { .. } => send(&["SCAN.OPEN", "k"]),
            Op::Next { slot } => send(&["SCAN.NEXT", &id(slot)]),
            Op::Close { slot } => send(&["SCAN.CLOSE", &id(slot)]),
            Op::BPop { conn, list: l } => Input::Send {
                conn: *conn,
                wait: false,
                requests: vec![argv(&["BPOP", &list(l)])],
            },
            Op::Settle => Input::Settle,
        }
    }

    /// A `GET` returns the last value set, or nothing after a delete. Also
    /// records which kinds of reply and which shapes of state the run
    /// reached.
    fn check(&self, ops: &[Op], steps: &[Step<Value>], notes: &mut Notes) {
        in_completion_order(self, ops, steps, |at, state| {
            let reply = &steps[at].reply;
            if let (Op::Get { key }, Some(got)) = (&ops[at], reply) {
                let want = state
                    .vals
                    .get(key)
                    .map_or(Value::Nil, |val| Value::Bulk(val.to_string().into()));
                if *got != want {
                    notes.fail("logic", "get_last_set");
                }
            }
            let class = match reply {
                None => 0,
                Some(Value::Nil) => 1,
                Some(Value::Error(_)) => 2,
                Some(Value::Array(items)) => 3 + items.len().min(2),
                Some(_) => 6,
            };
            notes.saw("reply", hash(&(std::mem::discriminant(&ops[at]), class)));
            notes.saw(
                "state",
                hash(&(state.vals.len(), state.scans.len(), state.compact)),
            );
        });
    }
}

fn small(value: u64) -> u8 {
    u8::try_from(value).expect("small")
}

fn hash(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// `kvd` at `binary`. It starts in milliseconds, so it gets short deadlines.
pub fn service(binary: PathBuf) -> Service<Resp2> {
    let launch = move |_: &std::path::Path, socket: &std::path::Path, boot: &[String]| {
        let mut command = Command::new(&binary);
        command.arg("--socket").arg(socket).args(boot);
        command
    };
    let mut service = Service::new(Resp2, launch, argv(&["PING"]), argv(&["SHUTDOWN"]));
    service.heal = Box::new(|admin| {
        admin.call(&argv(&["UNBLOCK"]));
    });
    service.timing = Timing {
        startup: Duration::from_secs(5),
        patience: Duration::from_millis(8),
        deadline: Duration::from_millis(200),
        exit: Duration::from_secs(2),
    };
    service
}
