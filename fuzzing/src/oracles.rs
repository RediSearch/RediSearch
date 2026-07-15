/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Oracles: crash/sanitizer and never-hang, plus a crash-signature extractor so
//! a campaign can dedup by root cause. Each finding carries the *full* server
//! log; the signature is parsed from it (not a fixed-size tail, whose window
//! catches only the crash-handler boilerplate, not the faulting frame).

#[derive(Debug)]
pub enum Finding {
    /// Server process died or ASan/UBSan fired.
    Crash { log: String },
    /// No reply within the read timeout, or unresponsive while still alive.
    Hang { log: String },
    /// Reply violated the RESP protocol (redis-rs could not parse it).
    // TODO(phase 2+): redis-rs currently surfaces protocol violations as I/O
    // errors, so this is not yet distinguishable from a disconnect. Reserved
    // for a lower-level reply check (e.g. pipeline reply-count desync).
    #[allow(dead_code)]
    MalformedReply,
}

impl Finding {
    pub fn kind(&self) -> &'static str {
        match self {
            Finding::Crash { .. } => "crash",
            Finding::Hang { .. } => "hang",
            Finding::MalformedReply => "malformed_reply",
        }
    }

    /// The full server log captured at the time of the finding, if any.
    pub fn log(&self) -> &str {
        match self {
            Finding::Crash { log } | Finding::Hang { log } => log,
            Finding::MalformedReply => "",
        }
    }

    /// A stable signature for dedup, parsed from the full log. Tries, in order of
    /// reliability: ASan's own `SUMMARY:` one-liner (bug type + file:line + func —
    /// exactly what ASan emits for dedup), an `RS_ASSERT` location, then the first
    /// `redisearch.so` backtrace symbol. Falls back to the finding kind for a
    /// genuine hang with no report.
    pub fn signature(&self) -> String {
        let log = self.log();
        let kind = self.kind();

        // 1. ASan SUMMARY line, e.g.
        //    `SUMMARY: AddressSanitizer: heap-buffer-overflow utf8_internal.h:47 in utf8_3b`.
        //    The canonical, stable per-bug identifier; path reduced to basename.
        if let Some(rest) = log
            .lines()
            .find_map(|l| l.split("SUMMARY: AddressSanitizer:").nth(1))
        {
            let norm: Vec<String> = rest.split_whitespace().map(basename).collect();
            return format!("asan:{}", norm.join(" "));
        }

        // 2. RS_ASSERT: `# ==> /path/file.c:NN '<expr>' is not true` — points
        //    straight at the failed assertion.
        if let Some(loc) = log
            .lines()
            .find_map(|l| l.split("==>").nth(1))
            .and_then(|rest| rest.split_whitespace().next())
        {
            return format!("assert:{}", trim_repo(loc));
        }

        // 3. Plain Redis backtrace: first `redisearch.so ... Symbol + off` frame.
        if let Some(sym) = log.lines().find_map(redisearch_symbol) {
            return format!("{kind}:{sym}");
        }

        format!("{kind}:no-report")
    }
}

/// Reduce an absolute path token to its basename (leave non-paths untouched), so
/// signatures are stable regardless of the checkout location.
fn basename(tok: &str) -> String {
    tok.rsplit('/').next().unwrap_or(tok).to_string()
}

/// Strip everything up to and including `RediSearch/` and any trailing quote.
fn trim_repo(path: &str) -> String {
    path.rsplit("RediSearch/")
        .next()
        .unwrap_or(path)
        .trim_matches('\'')
        .to_string()
}

/// From a Redis crash-backtrace line like
/// `1   redisearch.so   0x... TagIndex_Open + 156`, extract the symbol name.
fn redisearch_symbol(line: &str) -> Option<String> {
    if !line.contains("redisearch.so") {
        return None;
    }
    // The symbol is the token after the 0x... address.
    let mut toks = line.split_whitespace().peekable();
    while let Some(t) = toks.next() {
        if t.starts_with("0x") {
            return toks.next().map(str::to_string);
        }
    }
    None
}
