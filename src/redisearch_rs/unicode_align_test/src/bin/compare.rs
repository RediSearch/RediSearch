/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Ad-hoc single-string compare CLI.
//!
//! ```text
//! cargo run -p unicode_align_test --bin compare -- "Straße"
//! ```

fn main() -> anyhow::Result<()> {
    let input = std::env::args().nth(1).ok_or_else(|| {
        anyhow::anyhow!("usage: compare <string>  (the string is folded with both libnu and ICU)")
    })?;
    let diff = unicode_align_test::compare(&input);
    print!("{}", diff.render());
    Ok(())
}
