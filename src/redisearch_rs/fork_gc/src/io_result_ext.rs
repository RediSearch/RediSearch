/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io;

use crate::util::exit_on_write_error;

pub trait IoResultExt<T> {
    fn unwrap_or_exit(self) -> T;
}

impl<T> IoResultExt<T> for io::Result<T> {
    fn unwrap_or_exit(self) -> T {
        match self {
            Ok(t) => t,
            Err(e) => exit_on_write_error(e),
        }
    }
}
