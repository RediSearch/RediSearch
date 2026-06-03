/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Hand-picked strings exercising known libnu vs ICU lowercase divergences.
//!
//! Sister to `single_strings` (fold). The shared cases (sharp s, sigma,
//! Turkish I, ligatures) test very different surface area under lowercase:
//! folding canonicalises Σ/σ/ς to a single codepoint regardless of position,
//! whereas lowercase preserves uppercase-vs-lowercase distinctions and is
//! context-sensitive for final sigma (in ICU) — context that libnu's
//! unconditional `nu_tolower` ignores.

use unicode_align_test::diff::{compare_lower, run_corpus_lower};

fn known_cases() -> Vec<(&'static str, &'static str)> {
    vec![
        ("sharp s (German)", "Straße"),
        ("uppercase sharp s", "STRAẞE"),
        (
            "Greek final sigma (word-final triggers ς in ICU)",
            "ΟΔΥΣΣΕΥΣ",
        ),
        ("Greek mixed", "Ὀδυσσεύς"),
        (
            "Turkish dotted I (root locale lowercases to i\u{0307})",
            "İstanbul",
        ),
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
fn print_known_lowercase_divergences() {
    println!("\n=== single-string lowercase divergence cases ===");
    for (name, input) in known_cases() {
        let diff = compare_lower(input);
        println!("\n## {name}");
        print!("{}", diff.render());
    }
}

#[test]
fn aggregate_known_lowercase_cases() {
    let inputs: Vec<String> = known_cases()
        .into_iter()
        .map(|(_, s)| s.to_owned())
        .collect();
    let report = run_corpus_lower(inputs);
    print!("{}", report.render());
}
