/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Findings in a sanitizer's log.

use crate::system::Finding;

const ALLOCATORS: [&str; 3] = ["alloc", "strdup", "strndup"];

/// The failures a log reports: the first line that reports one, as an
/// AddressSanitizer error or by `failure`, since reporting a failure can
/// cause others, and each leak.
pub fn findings(log: &str, failure: impl Fn(&str) -> Option<Finding>) -> Vec<Finding> {
    let first = log
        .lines()
        .find_map(|line| error(line).or_else(|| failure(line)));
    first.into_iter().chain(leaks(log)).collect()
}

/// The finding a line with an AddressSanitizer error summary reports.
/// `heap-use-after-free /src/spec.c:123:7 in IndexSpec_Free` becomes
/// `asan:heap-use-after-free:IndexSpec_Free:spec.c:123`. Without debug
/// information the location is a binary and an offset that changes with
/// every build, so only the binary's name is kept.
fn error(line: &str) -> Option<Finding> {
    let summary = line.split("SUMMARY: AddressSanitizer: ").nth(1)?;
    if summary.contains(" leaked in ") {
        return None;
    }
    let mut words = summary.split_whitespace();
    let bug = words.next().unwrap_or_default();
    let location = words.next().unwrap_or_default();
    let location = match location.strip_prefix('(') {
        Some(binary) => binary.split(':').next().unwrap_or_default().to_owned(),
        None => basename(location)
            .split(':')
            .take(2)
            .collect::<Vec<_>>()
            .join(":"),
    };
    let key = match summary.split_once(" in ") {
        Some((_, function)) => {
            let function = function.split("+0x").next().unwrap_or(function);
            format!("{bug}:{function}:{location}")
        }
        None => format!("{bug}:{location}"),
    };
    Some(Finding { kind: "asan", key })
}

/// A finding per leak, naming its first frame outside an allocator.
fn leaks(log: &str) -> Vec<Finding> {
    log.split("leak of ")
        .skip(1)
        .filter_map(|report| {
            report
                .lines()
                .skip(1)
                .take_while(|line| !line.trim().is_empty())
                .filter_map(|line| line.split(" in ").nth(1)?.split_whitespace().next())
                .map(|symbol| symbol.split("+0x").next().unwrap_or(symbol))
                .find(|function| !ALLOCATORS.iter().any(|a| function.contains(a)))
        })
        .map(|function| Finding {
            kind: "leak",
            key: function.to_owned(),
        })
        .collect()
}

pub fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_become_signatures_without_addresses() {
        let errors = [
            (
                "SUMMARY: AddressSanitizer: heap-use-after-free /src/spec.c:123:7 in IndexSpec_Free",
                "asan:heap-use-after-free:IndexSpec_Free:spec.c:123",
            ),
            (
                "SUMMARY: AddressSanitizer: heap-buffer-overflow (redisearch.so:arm64+0x2f5564) in QueryNode_CheckIsValid+0x1374",
                "asan:heap-buffer-overflow:QueryNode_CheckIsValid:redisearch.so",
            ),
        ];
        for (line, signature) in errors {
            assert_eq!(error(line).unwrap().to_string(), signature);
        }
        let leak = "Direct leak of 16 byte(s) in 1 object(s) allocated from:\n\
                    \x20   #0 0x10 in malloc+0x9c (libclang_rt.asan_osx_dynamic.dylib:arm64e+0x)\n\
                    \x20   #1 0x20 in rm_malloc rmalloc.h:20\n\
                    \x20   #2 0x30 in NewThing thing.c:5\n\n\
                    SUMMARY: AddressSanitizer: 16 byte(s) leaked in 1 allocation(s).\n";
        assert_eq!(leaks(leak)[0].to_string(), "leak:NewThing");
        assert_eq!(leak.lines().find_map(error), None);
    }
}
