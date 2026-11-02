/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Reads file contents straight from the git object database, so any commit
//! can be analyzed without touching the working copy.

use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

/// A commit resolved to its full id.
pub struct Commit {
    pub id: String,
    /// Committer date, as `YYYY-MM-DD`.
    pub date: String,
    /// First line of the message.
    pub subject: String,
}

fn git(repo: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .context("failed to run git")?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

/// Returns the root of the repository containing the current directory.
pub fn toplevel() -> Result<String> {
    let out = git(Path::new("."), &["rev-parse", "--show-toplevel"])?;
    Ok(String::from_utf8(out)?.trim().to_owned())
}

pub fn resolve(repo: &Path, rev: &str) -> Result<Commit> {
    let spec = format!("{rev}^{{commit}}");
    let id = String::from_utf8(git(repo, &["rev-parse", "--verify", &spec])?)?
        .trim()
        .to_owned();
    let out = String::from_utf8(git(repo, &["log", "-1", "--format=%cs %s", &id])?)?;
    let (date, subject) = out.trim().split_once(' ').unwrap_or((out.trim(), ""));
    Ok(Commit {
        date: date.to_owned(),
        subject: subject.to_owned(),
        id,
    })
}

/// The first-parent history from `target` down to, and including, `baseline`,
/// newest first, with each commit's committer timestamp.
///
/// Fails unless `baseline` is on that first-parent history: a commit merged
/// in through a side branch is never reached by it.
pub fn first_parent_history(
    repo: &Path,
    baseline: &str,
    target: &str,
) -> Result<Vec<(String, i64)>> {
    let baseline_id = resolve(repo, baseline)?.id;
    let target_id = resolve(repo, target)?.id;
    // Excluding the baseline's parents rather than the baseline keeps it in.
    let log = String::from_utf8(git(
        repo,
        &[
            "log",
            "--first-parent",
            "--format=%H %ct",
            &target_id,
            "--not",
            &format!("{baseline_id}^@"),
        ],
    )?)?;
    let history = log
        .lines()
        .map(|line| {
            let (id, ts) = line.split_once(' ').context("malformed git log line")?;
            Ok((id.to_owned(), ts.parse()?))
        })
        .collect::<Result<Vec<(String, i64)>>>()?;
    if history.last().map(|(id, _)| id) != Some(&baseline_id) {
        bail!("{baseline} is not on the first-parent history of {target}");
    }
    Ok(history)
}

/// Loads every blob of `commit` whose path satisfies `keep`, keyed by path.
///
/// Submodules are not blobs and are therefore never returned. Files that are
/// not valid UTF-8 are decoded lossily: only line structure matters here.
pub fn load_files(
    repo: &Path,
    commit: &str,
    keep: impl Fn(&str) -> bool,
) -> Result<BTreeMap<String, String>> {
    let listing = git(repo, &["ls-tree", "-r", "-z", commit])?;
    // Each entry is `<mode> SP <type> SP <oid> TAB <path>`.
    let mut wanted = Vec::new();
    for entry in listing.split(|&b| b == 0).filter(|e| !e.is_empty()) {
        let entry = std::str::from_utf8(entry).context("non UTF-8 path in tree")?;
        let (meta, path) = entry.split_once('\t').context("malformed ls-tree entry")?;
        let mut fields = meta.split(' ');
        let (_mode, kind, oid) = (fields.next(), fields.next(), fields.next());
        if kind == Some("blob") && keep(path) {
            wanted.push((oid.context("missing oid")?.to_owned(), path.to_owned()));
        }
    }

    let mut child = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to spawn git cat-file")?;

    // Feed the requests from another thread so neither pipe can fill up and
    // deadlock the two processes.
    let mut stdin = child.stdin.take().context("no stdin")?;
    let oids: Vec<String> = wanted.iter().map(|(oid, _)| oid.clone()).collect();
    let writer = std::thread::spawn(move || -> std::io::Result<()> {
        for oid in oids {
            writeln!(stdin, "{oid}")?;
        }
        Ok(())
    });

    let mut stdout = BufReader::new(child.stdout.take().context("no stdout")?);
    let mut files = BTreeMap::new();
    let mut header = String::new();
    for (_, path) in wanted {
        header.clear();
        stdout.read_line(&mut header)?;
        // Header is `<oid> <type> <size>`.
        let size: usize = header
            .trim_end()
            .rsplit(' ')
            .next()
            .and_then(|s| s.parse().ok())
            .with_context(|| format!("unexpected cat-file header {header:?}"))?;
        let mut content = vec![0; size + 1];
        stdout.read_exact(&mut content)?;
        content.pop(); // trailing newline after each object
        files.insert(path, String::from_utf8_lossy(&content).into_owned());
    }

    writer.join().expect("writer thread panicked")?;
    if !child.wait()?.success() {
        bail!("git cat-file failed");
    }
    Ok(files)
}
