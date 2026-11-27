/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A finding as a standalone Python script that runs its program against a
//! server with redis-py, the client the Python tests use.

use std::collections::{BTreeMap, BTreeSet};

use procfuzz::service::Input;
use procfuzz::system::Step;
use procfuzz_redis::resp::{Argv, Bytes, Value};

use crate::RediSearch;
use crate::logic::{Compared, broken};
use crate::op::{Action, Op};
use crate::state::State;

const SETTLE: &str = r#"def settle():
    """Waits up to 2 seconds for indexing and background jobs to finish."""
    for _ in range(200):
        indexes = r.execute_command('FT._LIST')
        indexing = any(int(fields(r.execute_command('FT.INFO', i))[b'indexing']) for i in indexes)
        jobs = fields(r.execute_command('_FT.DEBUG', 'WORKERS', 'STATS'))
        if not indexing and int(jobs[b'totalPendingJobs']) + int(jobs[b'numJobsInProgress']) == 0:
            r.execute_command('_FT.DEBUG', 'GC_WAIT_FOR_JOBS')
            return
        time.sleep(0.01)


def fields(reply):
    return dict(zip(reply[::2], reply[1::2]))
"#;

const DOCUMENTS: &str = r#"def documents(reply):
    """The number of matches and the keys an FT.SEARCH reply lists."""
    return reply[0], {item for item in reply[1:] if isinstance(item, bytes)}
"#;

/// The script for `finding`, from the run whose `steps` showed it.
pub fn script(model: &RediSearch, ops: &[Op], steps: &[Step<Value>], finding: &str) -> String {
    let kind = finding.split(':').next().unwrap_or_default();
    let compared = (kind == "logic")
        .then(|| {
            broken(model, ops, steps)
                .into_iter()
                .find(|broken| format!("logic:{}", broken.key) == finding)
        })
        .flatten()
        .map(|broken| broken.steps);
    let mut script = Script::new(ops, steps, compared);
    for (at, step) in steps.iter().enumerate() {
        script.op(at, &ops[at], step.reply.as_ref());
    }
    let check = match compared {
        Some(Compared::Same(..)) => Some(
            "# Both searches must return the same documents.\n\
             assert documents(first) == documents(second), (first, second)\n",
        ),
        Some(Compared::Partition { .. }) => Some(
            "# Each match of the first search matches exactly one of the other two.\n\
             assert whole[0] == with_part[0] + without_part[0], (whole, with_part, without_part)\n",
        ),
        None => None,
    };
    if let Some(check) = check {
        script.drain_all();
        script.body.push('\n');
        script.body += check;
    }
    if kind == "leak" {
        script.body += "r.shutdown(nosave=True)\n";
    }
    script.finish(finding, kind, compared)
}

struct Script {
    /// Each op's input, with names resolved against the state the recorded
    /// replies of the ops before it left.
    inputs: Vec<Input<Argv>>,
    states: Vec<State>,
    /// Connections that leave replies to be read later, which redis-py
    /// does only on a bare `Connection`.
    bare: BTreeSet<u8>,
    /// The steps whose replies the assertion compares, by variable.
    named: BTreeMap<usize, &'static str>,
    pending: BTreeMap<u8, Vec<Pending>>,
    body: String,
    settles: bool,
    suppresses: bool,
}

/// A reply to read on a bare connection.
struct Pending {
    target: Option<(String, &'static str)>,
    error: Option<String>,
}

impl Script {
    fn new(ops: &[Op], steps: &[Step<Value>], compared: Option<Compared>) -> Script {
        let mut state = State::default();
        let mut inputs = Vec::new();
        let mut states = Vec::new();
        for (op, step) in ops.iter().zip(steps) {
            inputs.push(op.lower(&state));
            states.push(state.clone());
            state.apply(op, step.reply.as_ref());
        }
        let bare = inputs
            .iter()
            .filter_map(|input| match input {
                Input::Send {
                    conn, wait: false, ..
                } => Some(*conn),
                _ => None,
            })
            .collect();
        let named = match compared {
            Some(Compared::Same(a, b)) => BTreeMap::from([(a, "first"), (b, "second")]),
            Some(Compared::Partition { whole, part, rest }) => BTreeMap::from([
                (whole, "whole"),
                (part, "with_part"),
                (rest, "without_part"),
            ]),
            None => BTreeMap::new(),
        };
        Script {
            inputs,
            states,
            bare,
            named,
            pending: BTreeMap::new(),
            body: String::new(),
            settles: false,
            suppresses: false,
        }
    }

    fn client(&self, conn: u8) -> String {
        match (self.bare.contains(&conn), conn) {
            (true, n) => format!("conn{n}"),
            (false, 0) => "r".to_owned(),
            (false, n) => format!("r{n}"),
        }
    }

    fn op(&mut self, at: usize, op: &Op, reply: Option<&Value>) {
        let error = match reply {
            Some(Value::Error(message)) => Some(text(message)),
            _ => None,
        };
        match self.inputs[at].clone() {
            Input::Settle => {
                self.drain_all();
                self.settles = true;
                self.body += "settle()\n";
            }
            Input::Send { requests, .. } if requests.is_empty() => {}
            Input::Send {
                conn,
                wait,
                requests,
            } => {
                let target = self.target(at, op, reply);
                let id = cursor_id(op)
                    .filter(|slot| {
                        self.states[at]
                            .cursors
                            .get(slot)
                            .is_some_and(|cursor| cursor.id.is_some())
                    })
                    .map(|slot| format!("cursor{slot}"));
                let commands: Vec<String> = requests
                    .iter()
                    .map(|argv| args(argv, id.as_deref()))
                    .collect();
                let client = self.client(conn);
                if !self.bare.contains(&conn) {
                    let call = |command: &str| format!("{client}.execute_command({command})");
                    match commands.as_slice() {
                        [command] => self.call(&call(command), target, error),
                        commands => self.each(commands, &call("*command")),
                    }
                    return;
                }
                self.drain(conn);
                match commands.as_slice() {
                    [command] => self.body += &format!("{client}.send_command({command})\n"),
                    commands => self.each(commands, &format!("{client}.send_command(*command)")),
                }
                let pending = self.pending.entry(conn).or_default();
                for _ in 1..commands.len() {
                    pending.push(Pending {
                        target: None,
                        error: None,
                    });
                }
                pending.push(Pending { target, error });
                if wait {
                    self.drain(conn);
                }
            }
        }
    }

    /// What the reply is kept as: a compared search's variable, or the
    /// cursor id a cursor's slot keeps, with the index into the reply.
    fn target(&self, at: usize, op: &Op, reply: Option<&Value>) -> Option<(String, &'static str)> {
        if let Some(name) = self.named.get(&at) {
            return Some(((*name).to_owned(), ""));
        }
        let slot = cursor_slot(op)?;
        matches!(reply, Some(Value::Array(items)) if matches!(items.as_slice(), [_, Value::Int(_)]))
            .then(|| (format!("cursor{slot}"), "[1]"))
    }

    fn call(&mut self, call: &str, target: Option<(String, &str)>, error: Option<String>) {
        let line = match target {
            Some((variable, index)) => format!("{variable} = {call}{index}"),
            None => call.to_owned(),
        };
        self.line(&line, error);
    }

    fn line(&mut self, line: &str, error: Option<String>) {
        match error {
            Some(message) => {
                self.suppresses = true;
                self.body +=
                    &format!("with suppress(redis.ResponseError):  # {message}\n    {line}\n");
            }
            None => {
                self.body += line;
                self.body.push('\n');
            }
        }
    }

    fn each(&mut self, commands: &[String], call: &str) {
        self.body += "for command in [\n";
        for command in commands {
            self.body += &format!("    ({command}),\n");
        }
        self.body += &format!("]:\n    {call}\n");
    }

    /// Reads the replies outstanding on `conn`.
    fn drain(&mut self, conn: u8) {
        let client = self.client(conn);
        let read = format!("{client}.read_response()");
        let mut plain = 0;
        for pending in self.pending.remove(&conn).unwrap_or_default() {
            if pending.target.is_none() && pending.error.is_none() {
                plain += 1;
                continue;
            }
            self.reads(&read, plain);
            plain = 0;
            let line = match pending.target {
                Some((variable, index)) => format!("{variable} = {read}{index}"),
                None => read.clone(),
            };
            self.line(&line, pending.error);
        }
        self.reads(&read, plain);
    }

    fn reads(&mut self, read: &str, count: usize) {
        match count {
            0 => {}
            1 => self.body += &format!("{read}\n"),
            n => self.body += &format!("for _ in range({n}):\n    {read}\n"),
        }
    }

    fn drain_all(&mut self) {
        let conns: Vec<u8> = self.pending.keys().copied().collect();
        for conn in conns {
            self.drain(conn);
        }
    }

    fn finish(self, finding: &str, kind: &str, compared: Option<Compared>) -> String {
        let (env, built) = match kind {
            "leak" => (
                "RS_GLOBAL_DTORS=1 LSAN_OPTIONS=suppressions=tests/memcheck/asan.supp ",
                ", built with AddressSanitizer (`./build.sh SAN=address`)",
            ),
            "asan" => (
                "",
                ", built with AddressSanitizer (`./build.sh SAN=address`)",
            ),
            _ => ("", ""),
        };
        let outcome = match kind {
            "asan" => " AddressSanitizer reports the error in the server's log.",
            "leak" => " When the script shuts the server down, its log reports the leak.",
            "hang" => " The server stops answering.",
            "logic" if compared.is_some() => " The assertion at the end fails.",
            "logic" | "protocol" => "",
            _ => " The server crashes.",
        };
        let mut out = format!(
            "\"\"\"Reproduces {finding}, found by the RediSearch fuzzer.\n\n\
             Start the server{built}:\n\n    \
             {env}redis-server --enable-debug-command yes \
             --loadmodule redisearch.so --loadmodule rejson.so\n\n\
             then run `python repro.py [port]`.{outcome}\n\"\"\"\n\
             import sys\n",
        );
        if self.settles {
            out += "import time\n";
        }
        if self.suppresses {
            out += "from contextlib import suppress\n";
        }
        out += "\nimport redis\n\nport = int(sys.argv[1]) if len(sys.argv) > 1 else 6379\n";
        out += "r = redis.Redis(port=port)\n";
        let conns: BTreeSet<u8> = self
            .inputs
            .iter()
            .filter_map(|input| match input {
                Input::Send { conn, requests, .. } if !requests.is_empty() => Some(*conn),
                _ => None,
            })
            .collect();
        for conn in conns.into_iter().filter(|&conn| self.client(conn) != "r") {
            let class = if self.bare.contains(&conn) {
                "Connection"
            } else {
                "Redis"
            };
            out += &format!("{} = redis.{class}(port=port)\n", self.client(conn));
        }
        let same = matches!(compared, Some(Compared::Same(..)));
        let helpers: Vec<&str> = [(self.settles, SETTLE), (same, DOCUMENTS)]
            .into_iter()
            .filter_map(|(used, helper)| used.then_some(helper))
            .collect();
        for helper in &helpers {
            out += "\n\n";
            out += helper;
        }
        out += if helpers.is_empty() { "\n" } else { "\n\n" };
        out += &self.body;
        out
    }
}

fn cursor_slot(op: &Op) -> Option<u8> {
    match &op.action {
        Action::Aggregate {
            cursor: Some(cursor),
            ..
        } => Some(cursor.slot),
        Action::CursorRead { slot, .. } => Some(*slot),
        _ => None,
    }
}

/// The slot whose cursor id is the op's last word.
fn cursor_id(op: &Op) -> Option<u8> {
    match &op.action {
        Action::CursorRead { slot, .. } | Action::CursorDel { slot } => Some(*slot),
        _ => None,
    }
}

/// The words as Python arguments, with the last one replaced by `last`.
fn args(argv: &Argv, last: Option<&str>) -> String {
    let mut words: Vec<String> = argv.iter().map(literal).collect();
    if let (Some(last), Some(word)) = (last, words.last_mut()) {
        *word = last.to_owned();
    }
    words.join(", ")
}

fn text(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes.as_slice()).into_owned()
}

/// A Python `str` literal for UTF-8 without control characters, else a
/// `bytes` literal.
fn literal(word: &Bytes) -> String {
    let escape = |c: char| match c {
        '\\' => "\\\\".to_owned(),
        '\'' => "\\'".to_owned(),
        c => c.to_string(),
    };
    match std::str::from_utf8(word.as_slice()) {
        Ok(text) if !text.chars().any(char::is_control) => {
            format!("'{}'", text.chars().map(escape).collect::<String>())
        }
        _ => {
            let bytes: String = word
                .as_slice()
                .iter()
                .map(|&b| match b {
                    b' '..=b'~' => escape(char::from(b)),
                    b => format!("\\x{b:02x}"),
                })
                .collect();
            format!("b'{bytes}'")
        }
    }
}

#[cfg(test)]
mod tests {
    use procfuzz_redis::resp::argv;

    use super::*;
    use crate::effect::DEBUG;
    use crate::op::{CursorSlot, DataType, Field, FieldKind, Query, QueryOptions};

    fn step(at: usize, reply: Option<Value>) -> Step<Value> {
        let tick = u32::try_from(2 * at + 1).unwrap();
        Step {
            sent: tick,
            done: Some(tick + 1),
            reply,
        }
    }

    fn ok() -> Option<Value> {
        Some(Value::Simple("OK".into()))
    }

    fn create() -> Op {
        Op::new(Action::Create {
            index: "i0".into(),
            on: DataType::Hash,
            prefix: "d0:".into(),
            fields: vec![Field {
                name: "f0".into(),
                kind: FieldKind::Tag,
                sortable: false,
            }],
        })
    }

    fn search(index: &str) -> Op {
        Op::new(Action::Query {
            index: index.into(),
            query: Query::All,
            options: QueryOptions::default(),
        })
    }

    fn run(ops: Vec<(Op, Option<Value>)>) -> (Vec<Op>, Vec<Step<Value>>) {
        ops.into_iter()
            .enumerate()
            .map(|(at, (op, reply))| (op, step(at, reply)))
            .unzip()
    }

    #[test]
    fn a_crash_reads_as_its_commands_with_failing_ones_marked() {
        let (ops, steps) = run(vec![
            (create(), ok()),
            (
                Op::new(Action::command(&["HSET", "d0:1", "f0", "it's\x01"])),
                Some(Value::Int(1)),
            ),
            (
                search("nope"),
                Some(Value::Error("Unknown index name".into())),
            ),
            (
                Op::new(Action::command(&[DEBUG, "GC_FORCEINVOKE", "i0"])),
                None,
            ),
        ]);
        let script = script(&RediSearch, &ops, &steps, "crash:gc.c:12");
        assert_eq!(
            script,
            r#""""Reproduces crash:gc.c:12, found by the RediSearch fuzzer.

Start the server:

    redis-server --enable-debug-command yes --loadmodule redisearch.so --loadmodule rejson.so

then run `python repro.py [port]`. The server crashes.
"""
import sys
from contextlib import suppress

import redis

port = int(sys.argv[1]) if len(sys.argv) > 1 else 6379
r = redis.Redis(port=port)

r.execute_command('FT.CREATE', 'i0', 'ON', 'HASH', 'PREFIX', '1', 'd0:', 'SCHEMA', 'f0', 'TAG')
r.execute_command('HSET', 'd0:1', 'f0', b'it\'s\x01')
with suppress(redis.ResponseError):  # Unknown index name
    r.execute_command('FT.SEARCH', 'nope', '*')
r.execute_command('_FT.DEBUG', 'GC_FORCEINVOKE', 'i0')
"#
        );
    }

    #[test]
    fn a_broken_rule_ends_in_an_assertion_over_the_searches_it_compared() {
        let mut deferred = search("i0");
        deferred.conn = 1;
        deferred.defer = true;
        let mut read = Op::new(Action::CursorRead {
            slot: 0,
            count: None,
        });
        read.conn = 1;
        let aggregate = Op::new(Action::Aggregate {
            index: "i0".into(),
            query: Query::All,
            pipeline: argv(&[]),
            cursor: Some(CursorSlot { slot: 0, count: 1 }),
        });
        let total = |n| Some(Value::Array(vec![Value::Int(n)]));
        let page = |id| Some(Value::Array(vec![Value::Array(Vec::new()), Value::Int(id)]));
        let (ops, steps) = run(vec![
            (
                Op::new(Action::command(&[
                    "CONFIG",
                    "SET",
                    "search-on-timeout",
                    "fail",
                ])),
                ok(),
            ),
            (create(), ok()),
            (Op::new(Action::Settle), None),
            (search("i0"), total(1)),
            (aggregate, page(77)),
            (deferred, total(0)),
            (read, page(0)),
        ]);
        let script = script(&RediSearch, &ops, &steps, "logic:equivalence:all");
        let body = script.split("\n\n\n").last().unwrap();
        assert_eq!(
            body,
            "r.execute_command('CONFIG', 'SET', 'search-on-timeout', 'fail')
r.execute_command('FT.CREATE', 'i0', 'ON', 'HASH', 'PREFIX', '1', 'd0:', 'SCHEMA', 'f0', 'TAG')
settle()
first = r.execute_command('FT.SEARCH', 'i0', '*')
cursor0 = r.execute_command('FT.AGGREGATE', 'i0', '*', 'WITHCURSOR', 'COUNT', '1')[1]
conn1.send_command('FT.SEARCH', 'i0', '*')
second = conn1.read_response()
conn1.send_command('FT.CURSOR', 'READ', 'i0', cursor0)
cursor0 = conn1.read_response()[1]

# Both searches must return the same documents.
assert documents(first) == documents(second), (first, second)
"
        );
        for line in [
            "conn1 = redis.Connection(port=port)",
            "def settle():",
            "def documents(reply):",
        ] {
            assert!(script.contains(line), "{line}\n{script}");
        }
    }
}
