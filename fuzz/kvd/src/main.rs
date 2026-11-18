/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A small multi-threaded RESP server with three planted bugs, for testing
//! the fuzzer: `kvd --socket PATH [--compact]`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hint::black_box;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PAGE: usize = 2;
const BLOCK_POLL: Duration = Duration::from_millis(5);

#[derive(Default)]
struct Db {
    compact: bool,
    vals: BTreeMap<Vec<u8>, Vec<u8>>,
    lists: BTreeMap<Vec<u8>, VecDeque<Vec<u8>>>,
    pushes: BTreeMap<Vec<u8>, u32>,
    cursors: BTreeMap<i64, (Vec<Vec<u8>>, usize)>,
    stuck: BTreeSet<Vec<u8>>,
    /// Bumped by `UNBLOCK`, which releases every blocked `BPOP`.
    epoch: u64,
}

type Shared = Arc<(Mutex<Db>, Condvar)>;

enum Out {
    Simple(&'static str),
    Err(&'static str),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Vec<u8>>),
}

fn random_id() -> i64 {
    let mut bytes = [0u8; 4];
    let _ = std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut bytes));
    i64::from(u32::from_le_bytes(bytes) >> 1) + 1
}

/// Edges that depend on timing, not on the input: one command in fifty
/// reaches one of 64.
#[inline(never)]
fn flaky() {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    if !t.is_multiple_of(50) {
        return;
    }
    macro_rules! arms {
        ($($n:literal)*) => { match (t / 50) % 64 { $($n => { black_box($n); })* _ => {} } }
    }
    arms!(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
          32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63);
}

fn command(shared: &Shared, argv: &[Vec<u8>]) -> Out {
    flaky();
    let (lock, wake) = &**shared;
    let mut db = lock.lock().expect("not poisoned");
    let name = argv
        .first()
        .map(|name| name.to_ascii_uppercase())
        .unwrap_or_default();
    let arg = |at: usize| argv[at].clone();
    match (name.as_slice(), argv.len()) {
        (b"PING", 1) => Out::Simple("PONG"),
        (b"SET", 3) => {
            let (key, val) = (arg(1), arg(2));
            // Planted wrong answer: in compact mode, once there are four
            // keys, none can be overwritten.
            if db.compact && db.vals.len() >= 4 && db.vals.contains_key(&key) {
                return Out::Simple("OK");
            }
            db.vals.insert(key, val);
            Out::Simple("OK")
        }
        (b"GET", 2) => Out::Bulk(db.vals.get(&argv[1]).cloned()),
        (b"DEL", 2) => {
            let key = &argv[1];
            let removed = i64::from(db.vals.remove(key).is_some())
                + i64::from(db.lists.remove(key).is_some());
            // Planted hang: a list deleted after three pushes loses whoever
            // waits on it next.
            if db.pushes.get(key).copied().unwrap_or(0) >= 3 {
                db.stuck.insert(key.clone());
            }
            Out::Int(removed)
        }
        (b"PUSH", 3) => {
            let (key, val) = (arg(1), arg(2));
            *db.pushes.entry(key.clone()).or_default() += 1;
            db.stuck.remove(&key);
            let list = db.lists.entry(key).or_default();
            list.push_back(val);
            let len = list.len();
            wake.notify_all();
            Out::Int(i64::try_from(len).unwrap_or(i64::MAX))
        }
        (b"POP", 2) => Out::Bulk(db.lists.get_mut(&argv[1]).and_then(VecDeque::pop_front)),
        (b"BPOP", 2) => {
            let key = arg(1);
            let epoch = db.epoch;
            loop {
                if !db.stuck.contains(&key) {
                    if let Some(val) = db.lists.get_mut(&key).and_then(VecDeque::pop_front) {
                        return Out::Bulk(Some(val));
                    }
                    if db.epoch != epoch {
                        return Out::Bulk(None);
                    }
                }
                db = wake.wait_timeout(db, BLOCK_POLL).expect("not poisoned").0;
            }
        }
        (b"UNBLOCK", 1) => {
            db.epoch += 1;
            wake.notify_all();
            Out::Simple("OK")
        }
        (b"SCAN.OPEN", 2) => {
            let keys: Vec<Vec<u8>> = db
                .vals
                .keys()
                .filter(|key| key.starts_with(&argv[1]))
                .cloned()
                .collect();
            let id = random_id();
            db.cursors.insert(id, (keys, 0));
            Out::Int(id)
        }
        (b"SCAN.NEXT", 2) => {
            let Some(id) = cursor_id(&argv[1]) else {
                return Out::Err("ERR bad cursor");
            };
            let Some((keys, at)) = db.cursors.get(&id).cloned() else {
                return Out::Err("ERR no such cursor");
            };
            let page: Vec<Vec<u8>> = keys.iter().skip(at).take(PAGE).cloned().collect();
            for key in &page {
                // Planted crash: a page after the first that names a key
                // deleted since the scan opened.
                if at > 0 && !db.vals.contains_key(key) {
                    std::process::abort();
                }
            }
            db.cursors.get_mut(&id).expect("open").1 = at + page.len();
            Out::Array(
                page.into_iter()
                    .filter(|key| db.vals.contains_key(key))
                    .collect(),
            )
        }
        (b"SCAN.CLOSE", 2) => match cursor_id(&argv[1]).and_then(|id| db.cursors.remove(&id)) {
            Some(_) => Out::Simple("OK"),
            None => Out::Err("ERR no such cursor"),
        },
        (b"SHUTDOWN", 1) => std::process::exit(0),
        _ => Out::Err("ERR unknown command or wrong number of arguments"),
    }
}

fn cursor_id(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok()?.parse().ok()
}

fn parse(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    let line = |at: usize| {
        buf[at..]
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|end| (&buf[at..at + end], at + end + 2))
    };
    let (head, mut at) = line(0)?;
    let count: usize = std::str::from_utf8(head.strip_prefix(b"*")?)
        .ok()?
        .parse()
        .ok()?;
    let mut argv = Vec::with_capacity(count);
    for _ in 0..count {
        let (head, next) = line(at)?;
        let len: usize = std::str::from_utf8(head.strip_prefix(b"$")?)
            .ok()?
            .parse()
            .ok()?;
        argv.push(buf.get(next..next + len)?.to_vec());
        buf.get(next + len..next + len + 2)?;
        at = next + len + 2;
    }
    Some((argv, at))
}

fn bulk(out: &mut Vec<u8>, val: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
    out.extend_from_slice(val);
    out.extend_from_slice(b"\r\n");
}

fn serve(shared: &Shared, mut stream: UnixStream) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        while let Some((argv, used)) = parse(&buf) {
            buf.drain(..used);
            let mut out = Vec::new();
            match command(shared, &argv) {
                Out::Simple(s) => out.extend_from_slice(format!("+{s}\r\n").as_bytes()),
                Out::Err(s) => out.extend_from_slice(format!("-{s}\r\n").as_bytes()),
                Out::Int(n) => out.extend_from_slice(format!(":{n}\r\n").as_bytes()),
                Out::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
                Out::Bulk(Some(val)) => bulk(&mut out, &val),
                Out::Array(items) => {
                    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                    for val in items {
                        bulk(&mut out, &val);
                    }
                }
            }
            if stream.write_all(&out).is_err() {
                return;
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let socket = args
        .iter()
        .position(|arg| arg == "--socket")
        .and_then(|at| args.get(at + 1))
        .expect("--socket PATH");
    let db = Db {
        compact: args.iter().any(|arg| arg == "--compact"),
        ..Db::default()
    };
    let shared: Shared = Arc::new((Mutex::new(db), Condvar::new()));
    let listener = UnixListener::bind(socket).expect("binds the socket");
    for stream in listener.incoming().flatten() {
        let shared = shared.clone();
        std::thread::spawn(move || serve(&shared, stream));
    }
}
