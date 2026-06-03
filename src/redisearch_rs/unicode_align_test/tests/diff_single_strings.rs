/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Hand-picked strings that exercise well-known libnu vs ICU divergences.
//!
//! These tests print their results; they don't gate CI on divergence
//! (the divergence is the point — this is a comparison harness, not a
//! regression check).

use unicode_align_test::diff::{compare, run_corpus};

fn known_cases() -> Vec<(&'static str, &'static str)> {
    vec![
        ("sharp s (German)", "Straße"),
        ("uppercase sharp s", "STRAẞE"),
        ("Greek final sigma", "ΟΔΥΣΣΕΥΣ"),
        ("Greek mixed", "Ὀδυσσεύς"),
        ("Turkish dotted I", "İstanbul"),
        ("Turkish dotless i (uppercase)", "IZMIR"),
        ("Latin ffi ligature", "ﬃ"),
        ("Latin ff ligature", "ﬀ"),
        ("Latin st ligature", "ﬆ"),
        ("Titlecase Dz with caron", "ǅ"),
        ("Cherokee letter A", "Ꭰ"),
        ("Cyrillic IO", "Ё"),
        ("Latin ASCII baseline", "Hello, World!"),
        ("Mixed Latin", "Café au Lait"),
        ("Empty string", ""),
        ("Combining acute", "e\u{0301}"),
    ]
}

#[test]
fn print_known_divergences() {
    println!("\n=== single-string known divergence cases ===");
    for (name, input) in known_cases() {
        let diff = compare(input);
        println!("\n## {name}");
        print!("{}", diff.render());
    }
}

#[test]
fn aggregate_known_cases() {
    let inputs: Vec<String> = known_cases()
        .into_iter()
        .map(|(_, s)| s.to_owned())
        .collect();
    let report = run_corpus(inputs);
    print!("{}", report.render());
}
