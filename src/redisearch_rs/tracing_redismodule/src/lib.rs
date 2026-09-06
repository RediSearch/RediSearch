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
//! The maximum verbosity is set by the redis server `loglevel` config option. Its values map
//! onto `tracing` [`level`]s as follows:
//!
//! - `debug` => [`Level::TRACE`]
//! - `verbose` => [`Level::DEBUG`]
//! - `notice` => [`Level::INFO`]
//! - `warning` => [`Level::WARN`] (and [`Level::ERROR`])
//!
//! ## Output Styling
//!
//! By default the subscriber will style the terminal output to help with legibility.
//! To manually configure the styling of logging output you can set the `RUST_LOG_STYLE`
//! environment variable. Supported values are:
//!
//! - `auto` (default) will attempt to print style characters, but don’t force the issue. If the console isn’t available on Windows, if TERM=dumb, or a CI environment is detected for example, then don’t print colors.
//! - `always` will always print style characters even if they aren’t supported by the terminal. This includes emitting ANSI colors on Windows if the console API is unavailable.
//! - `never` will never print style characters.
//!
//! [`level`]: tracing_core::Level

use std::cell::RefCell;
use std::env::{self, VarError};
use std::error::Error;
use std::ffi::{CStr, c_char};
use std::io::IsTerminal;
use std::ptr::NonNull;
use std::sync::OnceLock;
use std::{io, ptr};
use tracing::Level;
use tracing_core::LevelFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Registry, reload};

const LOGLEVEL_DEBUG: &CStr = c"debug";
const LOGLEVEL_VERBOSE: &CStr = c"verbose";
const LOGLEVEL_NOTICE: &CStr = c"notice";
const LOGLEVEL_WARNING: &CStr = c"warning";

/// Handle to the reloadable level filter, installed by [`try_init`].
///
/// Used by [`set_log_level`] to update the active filter when the redis
/// `loglevel` config changes.
static FILTER_RELOAD: OnceLock<reload::Handle<LevelFilter, Registry>> = OnceLock::new();

type LogFunc = unsafe extern "C" fn(
    ctx: *mut redis_module::RedisModuleCtx,
    level: *const c_char,
    fmt: *const c_char,
    ...
);

/// Initializes a global subscriber that reports traces through `redismodule` logging.
///
/// `level` is the initial maximum verbosity the filter is set to.
pub fn init(ctx: Option<NonNull<redis_module::RedisModuleCtx>>, filter: LevelFilter) {
    try_init(ctx, filter).expect("Unable to install global tracing subscriber")
}

/// Initializes a global subscriber that reports traces through `redismodule`
///  logging if one is not already set.
///
/// `level` is the initial maximum verbosity the filter is set to.
///
/// # Errors
///
/// Returns an Error if the initialization was unsuccessful, likely because
/// a global subscriber was already installed by another call to `try_init`.
pub fn try_init(
    ctx: Option<NonNull<redis_module::RedisModuleCtx>>,
    filter: LevelFilter,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let (filter, reload_handle) = reload::Layer::new(filter);

    let fmt = tracing_subscriber::fmt::Layer::default()
        .with_file(true)
        .with_line_number(true)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::FULL)
        .with_ansi(should_print_colors())
        .without_time() // redis already prints timestamps
        .with_writer(MakeRedisModuleWriter {
            ctx: ctx.and_then(|ctx| {
                // Safety: We assume this static will not be written to after it has been initialized
                let detach_ctx =
                    unsafe { redis_module::RedisModule_GetDetachedThreadSafeContext.unwrap() };

                // Create a detached context, thread-safe context from the one provided, so we can keep it around
                // for logging.
                // Safety: FFI function call
                NonNull::new(unsafe { detach_ctx(ctx.as_ptr()) })
            }),
            // Safety: This static will not be written to after it has been initialized
            log: unsafe { redis_module::RedisModule_Log.unwrap() },
        });

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .try_init()?;

    let _ = FILTER_RELOAD.set(reload_handle);

    tracing::debug!("Tracing Subscriber Initialized!");

    Ok(())
}

/// Updates the active maximum verbosity of the log filter.
///
/// Has no effect if the subscriber is not installed yet.
pub fn set_log_level(filter: LevelFilter) {
    if let Some(reload_handle) = FILTER_RELOAD.get() {
        let _ = reload_handle.reload(filter);
    }
}

fn should_print_colors() -> bool {
    match env::var("RUST_LOG_STYLE").as_deref() {
        Ok("never") => false,
        Ok("always") => true,
        Ok("auto") | Err(VarError::NotPresent) => {
            // Attempt a "best guess" based on the terminal configuration and env vars
            // adapted from https://github.com/rust-cli/anstyle
            let clicolor = anstyle_query::clicolor();
            let clicolor_enabled = clicolor.unwrap_or(false);
            let clicolor_disabled = !clicolor.unwrap_or(true);
            if anstyle_query::no_color() {
                false
            } else if anstyle_query::clicolor_force() {
                true
            } else if clicolor_disabled ||
                // Don't use colors in non-interactive environments
                !std::io::stderr().is_terminal()
            {
                false
            } else {
                anstyle_query::term_supports_color() || clicolor_enabled || anstyle_query::is_ci()
            }
        }
        v => panic!("invalid RUST_LOG_STYLE value `{v:?}`"),
    }
}

struct MakeRedisModuleWriter {
    ctx: Option<NonNull<redis_module::RedisModuleCtx>>,
    log: LogFunc,
}

// Safety: we created a thread-safe context pointer above
unsafe impl Send for MakeRedisModuleWriter {}
// Safety: we created a thread-safe context pointer above
unsafe impl Sync for MakeRedisModuleWriter {}

impl<'a> MakeWriter<'a> for MakeRedisModuleWriter {
    type Writer = RedisModuleWriter;

    fn make_writer(&'a self) -> Self::Writer {
        unreachable!("must always be configured with a log level");
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        let level = match *meta.level() {
            Level::TRACE => LOGLEVEL_DEBUG,
            Level::DEBUG => LOGLEVEL_VERBOSE,
            Level::INFO => LOGLEVEL_NOTICE,
            Level::WARN | Level::ERROR => LOGLEVEL_WARNING,
        };

        RedisModuleWriter {
            ctx: self.ctx,
            level,
            log: self.log,
        }
    }
}

struct RedisModuleWriter {
    ctx: Option<NonNull<redis_module::RedisModuleCtx>>,
    level: &'static CStr,
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

            // NB: replace interior null bytes with spaces (ideally we would replace them with �
            // but that is a multi-byte character which means this loop wouldn't get optimized as well).
            buf.extend(input.iter().map(|b| if *b == 0 { b' ' } else { *b }));
            let bytes_written = buf.len();

            // NB: tracing subscriber always adds a trailing newline. We don't need this as the redismodule
            // logging system already adds one for us as well. BUT we can use this to our advantage and change it
            // for a NULL byte thereby making it a valid C string.
            debug_assert_eq!(buf[bytes_written - 1], b'\n');
            buf[bytes_written - 1] = 0;

            // Safety: We just replaced all interior null bytes and added the trailing one.
            let cstr = unsafe { CStr::from_bytes_with_nul_unchecked(buf.as_mut()) };

            // <https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#redismodule_log>
            // Safety: The documentation explicitly allows ctx to be a nullptr and we ensured the C string is valid above.
            unsafe {
                (self.log)(
                    self.ctx.map_or(ptr::null_mut(), |ctx| ctx.as_ptr()),
                    self.level.as_ptr(),
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
