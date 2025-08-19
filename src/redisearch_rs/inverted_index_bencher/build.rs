/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::env;
use std::path::PathBuf;

use build_utils::{git_root, link_static_libraries, rerun_if_c_changes};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Always link the static libraries, independent of bindgen
    link_static_libraries(&[
        ("src/inverted_index", "inverted_index"),
        ("src/buffer", "buffer"),
    ]);

    // Generate C bindings - fail build if this doesn't work
    generate_c_bindings()?;

    Ok(())
}

fn generate_c_bindings() -> Result<(), Box<dyn std::error::Error>> {
    let root = git_root().expect("Could not find git root for static library linking");

    let includes = [
        root.join("deps").join("RedisModulesSDK"),
        root.join("src"),
        root.join("deps"),
        root.join("src").join("redisearch_rs").join("headers"),
        root.join("deps").join("VectorSimilarity").join("src"),
        root.join("src").join("buffer"),
    ];

    let mut bindings = bindgen::Builder::default().header(
        root.join("src")
            .join("inverted_index")
            .join("inverted_index.h")
            .to_str()
            .ok_or("Invalid path")?,
    );

    for include in includes {
        bindings = bindings.clang_arg(format!("-I{}", include.display()));
        // Re-run the build script if any of the C files in the included
        // directory changes
        let _ = rerun_if_c_changes(&include);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    bindings
        .allowlist_file(".*/inverted_index.h")
        // Don't generate the Rust exported types else we'll have a compiler issue about the wrong
        // type being used
        .blocklist_file(".*/types_rs.h")
        // Those APIs require a type carying a lifetime, such as RSIndexResult<'a>,
        // which cannot be handled by bindgen because of their lifetime.
        // We do not actually need them so let's just skip generating their bindings.
        .blocklist_item("indexIterator")
        .blocklist_item("IndexIterator")
        .blocklist_item("NewReadIterator")
        .blocklist_item("IndexReader")
        .blocklist_item("IndexDecoder")
        .blocklist_item("IndexSeeker")
        .blocklist_item("IR_Free")
        .blocklist_item("NewGenericIndexReader")
        .blocklist_item("NewTermIndexReader")
        .blocklist_item("NewTermIndexReaderEx")
        .blocklist_item("IndexReader_OnReopen")
        .blocklist_item("NewMinimalNumericReader")
        .blocklist_item("NewNumericReader")
        .blocklist_item("IndexDecoderProcs")
        .blocklist_item("InvertedIndex_GetDecoder")
        .blocklist_item("RSByteOffsets")
        .blocklist_item("RSDocumentMetadata_s")
        .generate()?
        .write_to_file(out_dir.join("bindings.rs"))?;

    Ok(())
}
