/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! build.rs utilities.

use std::{fs::read_dir, path::Path};

/// Return the root folder of the project containing the `.git` directory.
pub fn git_root() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let mut path = std::env::current_dir()?;
    while !path.join(".git").exists() {
        path = path
            .parent()
            .ok_or("Could not find git root")?
            .to_path_buf();
    }
    Ok(path)
}

fn rerun_if_changes(dir: &Path, extensions: &[&str]) -> std::io::Result<()> {
    for entry in read_dir(dir)? {
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        if path.is_dir() {
            return rerun_if_changes(&path, extensions);
        } else if let Some(extension) = path.extension().and_then(|ext| ext.to_str())
            && extensions.contains(&extension)
        {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    Ok(())
}

/// Walk the specified directory and emit granular `rerun-if-changed` statements,
/// scoped to `*.c` and `*.h` files.
/// It'd be nice if `cargo` supported globbing syntax natively, but that's not the
/// case today.
pub fn rerun_if_c_changes(dir: &Path) -> std::io::Result<()> {
    rerun_if_changes(dir, &["c", "h"])
}

/// Walk the specified directory and emit granular `rerun-if-changed` statements,
/// scoped to `*.rs` files.
/// It'd be nice if `cargo` supported globbing syntax natively, but that's not the
/// case today.
fn rerun_if_rust_changes(dir: &Path) -> std::io::Result<()> {
    rerun_if_changes(dir, &["rs"])
}

/// Emit granular `rerun-if-changed` statements for each files which are
/// part of the crates listed in `config.parse.include`.
/// This ensures cbindgen output is regenerated when the underlying Rust code
/// is updated.
pub fn rerun_cbinden(config: &cbindgen::Config) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(include) = &config.parse.include {
        for included_crate in include.iter() {
            let path = git_root()?
                .join("src")
                .join("redisearch_rs")
                .join(included_crate);
            if path.exists() {
                let _ = rerun_if_rust_changes(&path);
            }
        }
    }

    Ok(())
}
