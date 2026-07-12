/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Canonical names of the built-in query expanders.

/// Default query expander (`DEFAULT`). Used when a query does not request one.
#[cheadergen::config(export)]
pub const DEFAULT_EXPANDER_NAME: &str = "DEFAULT";

/// Phonetic-matching query expander (`PHONETIC`).
#[cheadergen::config(export)]
pub const PHONETIC_EXPENDER_NAME: &str = "PHONETIC";

/// Synonym-expansion query expander (`SYNONYM`).
#[cheadergen::config(export)]
pub const SYNONYMS_EXPENDER_NAME: &str = "SYNONYM";

/// Stemming query expander (`SBSTEM`).
#[cheadergen::config(export)]
pub const STEMMER_EXPENDER_NAME: &str = "SBSTEM";
