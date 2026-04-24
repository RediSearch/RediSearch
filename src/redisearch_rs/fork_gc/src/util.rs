/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use redis_module::raw::RedisModule_ExitFromChild;
use std::io;

/// Log a write error and terminate the forked child.
pub fn exit_on_write_error(err: io::Error) -> ! {
    // Write the error message to the logging mechanism as well as directly to `stderr`
    // to make sure it ends up somewhere.
    let message = format!("GC fork: broken pipe, exiting: {err}");
    eprintln!("{message}");
    tracing::warn!("{message}");

    // SAFETY: `RedisModule_ExitFromChild` is a function-pointer static
    // initialized by the Redis module loader before any module code
    // runs; it is never written after that, so reading it is sound.
    let exit_from_child = unsafe { RedisModule_ExitFromChild }
        .expect("RedisModule_ExitFromChild must be initialized");

    // SAFETY: terminates the child process; does not return.
    unsafe {
        exit_from_child(1);
    }

    unreachable!("RedisModule_ExitFromChild returned")
}
