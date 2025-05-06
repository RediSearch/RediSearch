/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Scan all `*.rs` files to check if they are prefixed with the expected license header.
//!
//! If invoked with `--fix` as an argument, it'll prepend the license header to
//! all files that are missing one.
use std::{
    env,
    fs::{self, read_to_string, write},
    path::Path,
};

const LICENSE_HEADER: &str = "\
/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
";

fn main() {
    let fix = env::args().any(|arg| arg == "--fix");
    let current_dir = env::current_dir().expect("Failed to get current dir");
    let mut bad_files = Vec::new();

    visit_dir(&current_dir, fix, &mut bad_files);

    if bad_files.is_empty() {
        println!("✅ All .rs files contain the license header.");
    } else {
        println!("❌ The following files are missing the license header:");
        for file in &bad_files {
            println!(" - {}", file.display());
        }
        println!("Run `cargo license-fix` to prepend the expected header to all those files.");
        if !fix {
            std::process::exit(1);
        }
    }
}

fn visit_dir(dir: &Path, fix: bool, bad_files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(dir).expect("Failed to read directory") {
        let entry = entry.expect("Failed to read entry");
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|s| s.to_str()) == Some("low_memory_thin_vec") {
                // That crate is under a different license, since it's a fork.
                continue;
            }
            visit_dir(&path, fix, bad_files);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            check_file(&path, fix, bad_files);
        }
    }
}

fn check_file(path: &Path, fix: bool, bad_files: &mut Vec<std::path::PathBuf>) {
    let content = read_to_string(path).unwrap_or_default();
    if !content.starts_with(LICENSE_HEADER) {
        if fix {
            let new_content = format!("{}\n\n{}", LICENSE_HEADER, content);
            write(path, new_content).expect("Failed to write file");
            println!("🛠️  Fixed: {}", path.display());
        } else {
            bad_files.push(path.to_path_buf());
        }
    }
}
