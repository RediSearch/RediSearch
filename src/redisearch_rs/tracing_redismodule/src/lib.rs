/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A subscriber for the Rust `tracing` ecosystem that emits traces and logs to the redismodule logging system.
//!
//! # Configuring Logging Output
//!
//! Logging output can be configured by setting the `RUST_LOG` environment variable to a _filter_.
//! A filter consists of one or more comma-separated directives which match on `Span`s and `Event`s.
//! Each directive may have a corresponding maximum verbosity [`level`] which enables (e.g., _selects for_)
//! spans and events that match. Like `log`, `tracing` considers less exclusive levels (like `trace` or `info`)
//! to be more verbose than more exclusive levels (like `error` or `warn`).
//!
//! At a high level, the syntax for directives consists of several parts:
//!
//! ```text
//! target[span{field=value}]=level
//! ```
//!
//! - `target` matches the event or span's target. In general, this is the module path and/or crate name.
//!   Examples of targets `h2`, `tokio::net`, or `tide::server`. For more information on targets,
//!   please refer to [`Metadata`]'s documentation.
//! - `span` matches on the span's name. If a `span` directive is provided alongside a `target`,
//!   the `span` directive will match on spans _within_ the `target`.
//! - `field` matches on fields within spans. Field names can also be supplied without a `value`
//!   and will match on any `Span` or `Event` that has a field with that name.
//!   For example: `[span{field=\"value\"}]=debug`, `[{field}]=trace`.
//! - `value` matches on the value of a span's field. If a value is a numeric literal or a bool,
//!   it will match _only_ on that value. Otherwise, this filter matches the
//!   [`std::fmt::Debug`] output from the value.
//! - `level` sets a maximum verbosity level accepted by this directive.
//!
//! For details see the [`tracing_subscriber`] documentation.
//!
//! [`level`]: tracing_core::Level
//! [`Metadata`]: tracing_core::Metadata
//! [`tracing_subscriber`]: https://docs.rs/tracing-subscriber/0.3.20/tracing_subscriber/filter/struct.EnvFilter.html#directives

use std::cell::RefCell;
use std::error::Error;
use std::ffi::{CStr, c_char};
use std::ptr::NonNull;
use std::{io, ptr};
use tracing_core::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::FmtSpan;

type LogFunc = unsafe extern "C" fn(
    ctx: *mut ffi::RedisModuleCtx,
    level: *const c_char,
    fmt: *const c_char,
    ...
);

/// Initializes a global subscriber that reports traces through `redismodule` logging.
pub fn init(ctx: Option<NonNull<ffi::RedisModuleCtx>>) {
    try_init(ctx).expect("Unable to install global tracing subscriber")
}

/// Initializes a global subscriber that reports traces through `redismodule`
///  logging if one is not already set.
///
/// # Errors
///
/// Returns an Error if the initialization was unsuccessful, likely because
/// a global subscriber was already installed by another call to `try_init`.
pub fn try_init(
    ctx: Option<NonNull<ffi::RedisModuleCtx>>,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::FULL)
        .with_env_filter(env_filter)
        .without_time() // redis already prints timestamps
        .with_writer(MakeRedisModuleWriter {
            ctx: ctx.and_then(|ctx| {
                // Safety: We assume this static will not be written to after it has been initialized
                let detach_ctx = unsafe { ffi::RedisModule_GetDetachedThreadSafeContext.unwrap() };

                // Create a detached context, thread-safe context from the one provided, so we can keep it around
                // for logging.
                // Safety: FFI function call
                NonNull::new(unsafe { detach_ctx(ctx.as_ptr()) })
            }),
            // Safety: This static will not be written to after it has been initialized
            log: unsafe { ffi::RedisModule_Log.unwrap() },
        })
        .try_init()
}

struct MakeRedisModuleWriter {
    ctx: Option<NonNull<ffi::RedisModuleCtx>>,
    log: LogFunc,
}

// Safety: we created a thread-safe context pointer above
unsafe impl Send for MakeRedisModuleWriter {}
// Safety: we created a thread-safe context pointer above
unsafe impl Sync for MakeRedisModuleWriter {}

impl<'a> MakeWriter<'a> for MakeRedisModuleWriter {
    type Writer = RedisModuleWriter;

    fn make_writer(&'a self) -> Self::Writer {
        RedisModuleWriter {
            ctx: self.ctx,
            log: self.log,
        }
    }
}

struct RedisModuleWriter {
    ctx: Option<NonNull<ffi::RedisModuleCtx>>,
    log: LogFunc,
}

impl io::Write for RedisModuleWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        // dont bother doing any work for empty buffers, this should never happen anyway
        if input.is_empty() {
            return Ok(0);
        }

        thread_local! {
            // per-CPU "cached" allocation for formatting log messages into
            // reusing it means we dont allocate for every message which would be prohibitive
            static BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }

        BUF.with(|buf| {
            // NB: variable declarations to extend the lifetime to the entire scope
            let borrow = buf.try_borrow_mut();
            let mut a;
            let mut b;

            // If the cached buffer is already mutably borrowed by this thread (e.g. signal handler, or recursive logging)
            // we just allocate a new Vec. This should happen rarely enough that its not a performance concern and _not logging_ or even crashing
            // would be catastrophic in those circumstances.
            let buf = if let Ok(buf) = borrow {
                a = buf;

                // Important: We want to preserve the capacity but clear the current string
                a.clear();

                &mut a
            } else {
                b = Vec::new();
                &mut b
            };

            let bytes_written = buf.write(input)?;

            // NB: tracing subscriber always adds a trailing newline. We don't need this as the redismodule
            // logging system already adds one for us as well. BUT we can use this to our advantage and change it
            // for a NULL byte thereby making it a valid C string.
            debug_assert_eq!(buf[bytes_written - 1], b'\n');
            buf[bytes_written - 1] = 0;

            // Safety: Rust strings do not contain interior null bytes AND we just pushed the trailing zero.
            let cstr = CStr::from_bytes_with_nul(buf.as_mut()).unwrap();

            // <https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#redismodule_log>
            // Safety: The documentation explicitly allows ctx to be a nullptr, notice is a valid level
            // and we ensured the C string is valid above.
            unsafe {
                (self.log)(
                    self.ctx.map_or(ptr::null_mut(), |ctx| ctx.as_ptr()),
                    c"notice".as_ptr(),
                    c"%s".as_ptr(),
                    cstr.as_ptr(),
                );
            }

            Ok(bytes_written)
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
