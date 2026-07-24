/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Finding artifacts are self-contained Python reproduction scripts. Each script
//! spawns a `redis-server` with `redisearch.so` loaded, applies the recorded
//! config, replays the (already auto-minimized) command sequence, and reports
//! whether the server crashed — directly runnable and shareable. The script *is*
//! the replay, so the fuzzer never reads a saved artifact back.

use crate::artifact::{ArgJson, Artifact};

/// The embedded Python harness — the same logic as the fuzzer's own driver, so a
/// recipient needs only `pip install redis` and a built module.
const HARNESS: &str = r#"import argparse, os, socket, subprocess, sys, time
import redis

ap = argparse.ArgumentParser()
ap.add_argument("--redis-server", required=True)
ap.add_argument("--module", required=True)
ap.add_argument("--asan", action="store_true")
args = ap.parse_args()

s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()

env = dict(os.environ)
if args.asan:
    env["ASAN_OPTIONS"] = "detect_leaks=0:detect_odr_violation=0:abort_on_error=1:halt_on_error=1"

proc = subprocess.Popen(
    [args.redis_server, "--port", str(port), "--save", "", "--appendonly", "no",
     "--daemonize", "no", "--loadmodule", args.module] + MODULE_ARGS,
    env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

con = redis.Redis(port=port, socket_timeout=5)
for _ in range(200):
    if proc.poll() is not None:
        sys.exit(proc.stdout.read().decode(errors="replace"))
    try:
        con.ping()
        break
    except redis.exceptions.ConnectionError:
        time.sleep(0.1)
else:
    sys.exit("server did not become ready")

for key, val in CONFIG:
    try:
        con.execute_command("CONFIG", "SET", key, val)
    except redis.exceptions.ResponseError:
        pass

crashed = False
for argv in COMMANDS:
    try:
        con.execute_command(*argv)
    except redis.exceptions.ConnectionError:
        crashed = True
        break
    except redis.exceptions.ResponseError:
        pass  # -ERR replies are fine; only a crash counts

if not crashed:
    try:
        con.ping()
    except Exception:
        crashed = True
time.sleep(0.5)
crashed = crashed or proc.poll() is not None

proc.kill()
log = proc.stdout.read().decode(errors="replace")

if not crashed:
    sys.exit("did not reproduce (asserts disabled? wrong build?)")
print("crashed:")
for line in log.splitlines():
    if any(k in line for k in ("signal:", "ASSERTION", "==>", "SUMMARY")):
        print("  " + line.strip())
sys.exit(1)
"#;

/// Render a finding into a self-contained Python reproduction script. `signature`
/// and `key_log` (a few salient lines from the crash log) go into the docstring.
pub fn render_script(
    artifact: &Artifact,
    signature: &str,
    key_log: &[String],
    module_args: &[String],
) -> String {
    let mut s = String::new();
    s.push_str("#!/usr/bin/env python3\n");
    s.push_str("# Copyright (c) 2006-Present, Redis Ltd.\n");

    // Docstring: what crashed, salient log lines, usage.
    s.push_str(&format!(
        "\"\"\"RediSearch {} repro: {signature}\n",
        artifact.finding_kind
    ));
    if !artifact.standalone {
        s.push_str("Not standalone: depends on prior server state.\n");
    }
    if !key_log.is_empty() {
        s.push('\n');
        for line in key_log {
            s.push_str(&format!("  {}\n", line.trim()));
        }
    }
    s.push_str("\nUsage: python3 <this file> --redis-server <redis-server> --module <redisearch.so> [--asan]\n");
    s.push_str("\"\"\"\n\n");

    // Human-readable literals (also the runtime source of truth).
    s.push_str(&format!("MODULE_ARGS = {}\n", py_str_list(module_args)));
    s.push_str(&format!("CONFIG = {}\n", py_pairs(&artifact.config)));
    s.push_str("COMMANDS = [\n");
    for c in &artifact.commands {
        s.push_str(&format!("    [{}],\n", py_args(c)));
    }
    s.push_str("]\n\n");

    s.push_str(HARNESS);
    s
}

fn py_str_list(xs: &[String]) -> String {
    let items: Vec<String> = xs.iter().map(|x| py_quote(x.as_bytes())).collect();
    format!("[{}]", items.join(", "))
}

fn py_pairs(pairs: &[(String, String)]) -> String {
    let items: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("[{}, {}]", py_quote(k.as_bytes()), py_quote(v.as_bytes())))
        .collect();
    format!("[{}]", items.join(", "))
}

fn py_args(cmd: &[ArgJson]) -> String {
    cmd.iter()
        .map(|a| match a {
            ArgJson::Text(s) => py_quote(s.as_bytes()),
            ArgJson::Bin { hex } => format!("bytes.fromhex({})", py_quote(hex.as_bytes())),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A single-quoted Python string literal with the necessary escapes.
fn py_quote(bytes: &[u8]) -> String {
    let mut out = String::from("'");
    for &b in bytes {
        match b {
            b'\'' => out.push_str("\\'"),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out.push('\'');
    out
}

/// Extract the salient crash lines from a server log for the repro docstring.
pub fn key_log_lines(log: &str) -> Vec<String> {
    log.lines()
        .filter(|l| {
            l.contains("signal:")
                || l.contains("ASSERTION")
                || l.contains("==>")
                || l.contains("SUMMARY: AddressSanitizer")
        })
        .take(4)
        .map(|l| l.trim().to_string())
        .collect()
}
