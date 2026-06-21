//! Strong forwarders for the disk-plugin seam (option a).
//!
//! These STRONG `SearchDisk_{HasAPI,GetAPI,SetAPI}` exports override the weak C
//! stubs in `deps/RediSearch/src/search_disk.c` at link time, so the C core is
//! UNCHANGED. On first use they locate the sibling `redisearch_disk.so` (next to
//! this module on disk, found via `dladdr`), `dlopen` it `RTLD_NOW|RTLD_GLOBAL`,
//! and resolve the plugin's distinct entry points `SearchDiskPlugin_*`.

#[cfg(debug_assertions)]
use std::ffi::c_uint;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::PathBuf;
use std::sync::OnceLock;

/// Type of the `RedisModule_Log` API entry. It is a function *pointer* C global,
/// not a function, so it must be called through the pointer's value. We only ever
/// pass the fixed `(ctx, level, fmt, one %s arg)` prefix, so a non-variadic 4-arg
/// signature is ABI-compatible with the variadic C declaration for these calls.
type RedisModuleLogFn =
    unsafe extern "C" fn(*mut c_void, *const c_char, *const c_char, *const c_char);

unsafe extern "C" {
    fn dlopen(filename: *const c_char, flag: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dladdr(addr: *const c_void, info: *mut DlInfo) -> c_int;
    fn dlerror() -> *mut c_char;
    // `RedisModule_Log` is a function-pointer C global in `redisearch.so`, populated
    // by Redis at `OnLoad`. Bind it as a `static` (a pointer variable), then call
    // through its value — binding it as a `fn` would jump to the variable's address
    // and crash. A null context logs without an associated command context.
    static RedisModule_Log: Option<RedisModuleLogFn>;
    // `RedisModule_GetApi` is the C core's API-bootstrap pointer (also a C-core
    // global in `redisearch.so`, populated by Redis at `OnLoad`). Read its value to
    // initialize the dlopened plugin's own `RedisModule_*` table. Resolves to the
    // copy in this `.so` because the forwarder is linked with the C core.
    static RedisModule_GetApi: *const c_void;
}

#[repr(C)]
struct DlInfo {
    dli_fname: *const c_char,
    dli_fbase: *mut c_void,
    dli_sname: *const c_char,
    dli_saddr: *mut c_void,
}

const RTLD_NOW: c_int = 0x2;
const RTLD_GLOBAL: c_int = 0x100;
const RTLD_NOLOAD: c_int = 0x4;

type HasApiFn = unsafe extern "C" fn() -> bool;
type GetApiFn = unsafe extern "C" fn() -> *mut c_void;
type SetApiFn = unsafe extern "C" fn();
type InitRedisModuleApiFn = unsafe extern "C" fn(*const c_void);

// Fork × compaction debug coordinator entry-point signatures. Debug builds only,
// matching the `#[cfg(debug_assertions)]` exports in the plugin and the
// `FT.DEBUG REPL_COMPACTION_COORDINATOR` call sites in the C core.
#[cfg(debug_assertions)]
type DebugCoordinatorArmPauseFn = unsafe extern "C" fn(c_int, bool);
#[cfg(debug_assertions)]
type DebugCoordinatorSetWakeFn = unsafe extern "C" fn(c_int, c_int);
#[cfg(debug_assertions)]
type DebugCoordinatorReleaseFn = unsafe extern "C" fn(c_int);
#[cfg(debug_assertions)]
type DebugCoordinatorReachedFn = unsafe extern "C" fn(c_int) -> c_uint;
#[cfg(debug_assertions)]
type DebugResetCompactionControllerFn = unsafe extern "C" fn();

/// Log a `warning`-level diagnostic through the C core's `RedisModule_Log`, so disk
/// seam failures are visible in the server log instead of silently swallowed.
fn log_warning(msg: &str) {
    // SAFETY: reading the C-core function-pointer global. `Option<fn>` is null when
    // the API table is not yet populated, in which case we skip logging.
    let Some(log) = (unsafe { RedisModule_Log }) else {
        // Fallback so the diagnostic is never fully lost if the logger is unset.
        eprintln!("Search Disk plugin: {msg}");
        return;
    };
    let Ok(level) = CString::new("warning") else {
        return;
    };
    // `%s` keeps the message a single, non-format-interpreted argument.
    let Ok(fmt) = CString::new("Search Disk plugin: %s") else {
        return;
    };
    let Ok(cmsg) = CString::new(msg) else {
        return;
    };
    // SAFETY: `log` is the populated `RedisModule_Log` pointer; a null context is
    // accepted (logs without a command context). The format string has exactly one
    // `%s` matching the single C-string argument.
    unsafe {
        log(
            std::ptr::null_mut(),
            level.as_ptr(),
            fmt.as_ptr(),
            cmsg.as_ptr(),
        );
    }
}

/// The current `dlerror()` text, or a placeholder when none is set.
fn last_dlerror() -> String {
    // SAFETY: `dlerror` returns either null or a valid C string owned by libdl.
    let p = unsafe { dlerror() };
    if p.is_null() {
        return "(no dlerror)".to_string();
    }
    // SAFETY: non-null `dlerror` result is a valid NUL-terminated C string.
    unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

struct PluginEntries {
    has_api: Option<HasApiFn>,
    get_api: Option<GetApiFn>,
    set_api: Option<SetApiFn>,
    #[cfg(debug_assertions)]
    debug_arm_pause: Option<DebugCoordinatorArmPauseFn>,
    #[cfg(debug_assertions)]
    debug_set_wake: Option<DebugCoordinatorSetWakeFn>,
    #[cfg(debug_assertions)]
    debug_release: Option<DebugCoordinatorReleaseFn>,
    #[cfg(debug_assertions)]
    debug_reached: Option<DebugCoordinatorReachedFn>,
    #[cfg(debug_assertions)]
    debug_reset: Option<DebugResetCompactionControllerFn>,
}

impl PluginEntries {
    /// All-`None` value used as the error/early-return default before a
    /// successful `dlopen` populates the real entry points.
    const fn empty() -> Self {
        PluginEntries {
            has_api: None,
            get_api: None,
            set_api: None,
            #[cfg(debug_assertions)]
            debug_arm_pause: None,
            #[cfg(debug_assertions)]
            debug_set_wake: None,
            #[cfg(debug_assertions)]
            debug_release: None,
            #[cfg(debug_assertions)]
            debug_reached: None,
            #[cfg(debug_assertions)]
            debug_reset: None,
        }
    }
}

// SAFETY: function pointers into a dlopened-RTLD_GLOBAL library that is never
// unloaded for the process lifetime.
unsafe impl Send for PluginEntries {}
unsafe impl Sync for PluginEntries {}

static PLUGIN: OnceLock<PluginEntries> = OnceLock::new();

/// Marker so a function's own address can be passed to `dladdr`.
extern "C" fn anchor() {}

/// Filesystem path of `redisearch.so` (this object), discovered via `dladdr` on a
/// local function address.
fn self_path() -> Option<PathBuf> {
    let mut info = DlInfo {
        dli_fname: std::ptr::null(),
        dli_fbase: std::ptr::null_mut(),
        dli_sname: std::ptr::null(),
        dli_saddr: std::ptr::null_mut(),
    };
    // SAFETY: `anchor` is a valid function address; `info` is a valid out-param.
    let rc = unsafe { dladdr(anchor as *const c_void, &mut info) };
    if rc == 0 || info.dli_fname.is_null() {
        return None;
    }
    // SAFETY: dladdr populated dli_fname with a valid C string.
    let self_path = unsafe { CStr::from_ptr(info.dli_fname) }.to_str().ok()?;
    Some(PathBuf::from(self_path))
}

fn sibling_plugin_path() -> Option<PathBuf> {
    let dir = self_path()?.parent()?.to_path_buf();
    Some(dir.join("redisearch_disk.so"))
}

/// Promote `redisearch.so` (this object) into the global symbol scope.
///
/// Redis loads modules with `RTLD_LOCAL`, so this object's symbols — including the
/// C-core globals the plugin needs at load time (`RSDummyContext`, the
/// `PLUGIN_EXPORTED_CORE_SYMBOLS`, the C++ VecSim/iterator symbols) — are NOT in
/// the global scope. Re-`dlopen`ing the already-loaded object with
/// `RTLD_GLOBAL | RTLD_NOLOAD` adds its symbols to the global scope (a documented
/// glibc behavior) WITHOUT reloading it. Without this, the subsequent
/// `RTLD_NOW` plugin `dlopen` fails with `undefined symbol: RSDummyContext`.
///
/// Returns `true` on success.
fn promote_self_global() -> bool {
    let Some(path) = self_path() else {
        log_warning("could not locate redisearch.so via dladdr to promote it global");
        return false;
    };
    let Ok(cpath) = CString::new(path.to_string_lossy().as_bytes()) else {
        return false;
    };
    let _ = last_dlerror();
    // SAFETY: cpath is a valid C string; RTLD_NOLOAD only re-references an
    // already-loaded object, promoting it to the global scope.
    let h = unsafe { dlopen(cpath.as_ptr(), RTLD_NOW | RTLD_GLOBAL | RTLD_NOLOAD) };
    if h.is_null() {
        log_warning(&format!(
            "failed to promote redisearch.so to global scope: {}",
            last_dlerror()
        ));
        return false;
    }
    true
}

/// Resolve `name` in `handle`, logging a `dlerror` diagnostic on failure.
///
/// # Safety
/// `handle` must be a live `dlopen` handle.
unsafe fn resolve(handle: *mut c_void, name: &str) -> *mut c_void {
    let Ok(cname) = CString::new(name) else {
        return std::ptr::null_mut();
    };
    // Clear any stale error so `dlerror()` after `dlsym` reflects this lookup.
    let _ = last_dlerror();
    // SAFETY: `handle` is a live dlopen handle (caller contract); `cname` is a
    // valid C string.
    let sym = unsafe { dlsym(handle, cname.as_ptr()) };
    if sym.is_null() {
        log_warning(&format!("dlsym(\"{name}\") failed: {}", last_dlerror()));
    }
    sym
}

fn load_plugin() -> PluginEntries {
    let mut entries = PluginEntries::empty();
    let Some(path) = sibling_plugin_path() else {
        log_warning("could not locate sibling redisearch_disk.so (dladdr failed)");
        return entries;
    };
    let Ok(cpath) = CString::new(path.to_string_lossy().as_bytes()) else {
        return entries;
    };
    // Make this object's symbols (C-core globals like `RSDummyContext`, plus the
    // C++ VecSim/iterator symbols the plugin links against) reachable from the
    // plugin's `RTLD_NOW` resolution. Redis loaded us `RTLD_LOCAL`, so without this
    // the plugin `dlopen` aborts with `undefined symbol: RSDummyContext`.
    promote_self_global();
    // Clear any stale error so a subsequent `dlerror()` reflects this `dlopen`.
    let _ = last_dlerror();
    // SAFETY: cpath is a valid C string.
    let handle = unsafe { dlopen(cpath.as_ptr(), RTLD_NOW | RTLD_GLOBAL) };
    if handle.is_null() {
        log_warning(&format!(
            "dlopen(\"{}\") failed: {}",
            path.display(),
            last_dlerror()
        ));
        return entries;
    }

    // Bootstrap the plugin's own `redis_module` API table BEFORE any plugin entry
    // runs. The plugin has a separate, uninitialized copy of the `RedisModule_*`
    // table; pass it the C core's already-populated `RedisModule_GetApi` pointer so
    // it can query every symbol from Redis. Without this, the first plugin call that
    // touches `RedisModule_*` (e.g. `RedisModule_BigWriteBufferBudgetInit` during
    // disk open) would dereference a null pointer and abort.
    // SAFETY: `handle` is a live dlopen handle.
    let init_sym = unsafe { resolve(handle, "SearchDiskPlugin_InitRedisModuleAPI") };
    if !init_sym.is_null() {
        // SAFETY: `RedisModule_GetApi` is the C core's bootstrap pointer in this
        // `.so`, populated by Redis at `OnLoad`. The resolved symbol has the
        // declared `extern "C" fn(*const c_void)` signature.
        unsafe {
            let init = std::mem::transmute::<*mut c_void, InitRedisModuleApiFn>(init_sym);
            init(RedisModule_GetApi);
        }
    }

    // SAFETY: handle is a valid dlopen handle; the resolved symbols have the
    // declared `extern "C"` signatures.
    unsafe {
        let hp = resolve(handle, "SearchDiskPlugin_HasAPI");
        let gp = resolve(handle, "SearchDiskPlugin_GetAPI");
        let sp = resolve(handle, "SearchDiskPlugin_SetAPI");
        if !hp.is_null() {
            entries.has_api = Some(std::mem::transmute::<*mut c_void, HasApiFn>(hp));
        }
        if !gp.is_null() {
            entries.get_api = Some(std::mem::transmute::<*mut c_void, GetApiFn>(gp));
        }
        if !sp.is_null() {
            entries.set_api = Some(std::mem::transmute::<*mut c_void, SetApiFn>(sp));
        }
    }

    // Resolve the fork × compaction debug coordinator entry points. Debug builds
    // only; these back the `FT.DEBUG REPL_COMPACTION_COORDINATOR` subcommands.
    #[cfg(debug_assertions)]
    // SAFETY: handle is a valid dlopen handle; the resolved symbols have the
    // declared `extern "C"` signatures matching the plugin's exports.
    unsafe {
        let ap = resolve(handle, "SearchDiskPlugin_DebugCoordinatorArmPause");
        let sw = resolve(handle, "SearchDiskPlugin_DebugCoordinatorSetWake");
        let rl = resolve(handle, "SearchDiskPlugin_DebugCoordinatorRelease");
        let rc = resolve(handle, "SearchDiskPlugin_DebugCoordinatorReached");
        let rs = resolve(handle, "SearchDiskPlugin_DebugResetCompactionController");
        if !ap.is_null() {
            entries.debug_arm_pause =
                Some(std::mem::transmute::<*mut c_void, DebugCoordinatorArmPauseFn>(ap));
        }
        if !sw.is_null() {
            entries.debug_set_wake =
                Some(std::mem::transmute::<*mut c_void, DebugCoordinatorSetWakeFn>(sw));
        }
        if !rl.is_null() {
            entries.debug_release =
                Some(std::mem::transmute::<*mut c_void, DebugCoordinatorReleaseFn>(rl));
        }
        if !rc.is_null() {
            entries.debug_reached =
                Some(std::mem::transmute::<*mut c_void, DebugCoordinatorReachedFn>(rc));
        }
        if !rs.is_null() {
            entries.debug_reset = Some(std::mem::transmute::<
                *mut c_void,
                DebugResetCompactionControllerFn,
            >(rs));
        }
    }
    entries
}

fn plugin() -> &'static PluginEntries {
    PLUGIN.get_or_init(load_plugin)
}

/// Strong override of the weak C stub. Returns true iff the plugin is present
/// and reports its API available.
#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_HasAPI() -> bool {
    match plugin().has_api {
        // SAFETY: resolved plugin entry with the declared signature.
        Some(f) => unsafe { f() },
        None => false,
    }
}

/// Strong override of the weak C stub. Returns the plugin's disk-API vtable.
#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_GetAPI() -> *mut c_void {
    match plugin().get_api {
        // SAFETY: resolved plugin entry with the declared signature.
        Some(f) => unsafe { f() },
        None => std::ptr::null_mut(),
    }
}

/// Strong override of the weak C stub. Installs the shared
/// `SEARCH_ENTERPRISE_ITERATORS` via the plugin.
#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_SetAPI() {
    if let Some(f) = plugin().set_api {
        // SAFETY: resolved plugin entry with the declared signature.
        unsafe { f() }
    }
}

// --- Fork × compaction debug coordinator forwarders --------------------------
//
// Strong overrides of the weak C stubs in `search_disk.c`, mirroring the main
// three above. Debug builds only (the C stubs and call sites in
// `debug_commands.c` are themselves debug-only), so `FT.DEBUG
// REPL_COMPACTION_COORDINATOR` reaches the plugin's real coordinator instead of
// the no-op stubs. When the plugin is absent the entry is `None` and these
// behave like the original weak no-op stubs.

/// Strong override of the weak C stub. Arms a single-shot pause at `site`.
#[cfg(debug_assertions)]
#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_DebugCoordinatorArmPause(site: c_int, armed: bool) {
    if let Some(f) = plugin().debug_arm_pause {
        // SAFETY: resolved plugin entry with the declared signature.
        unsafe { f(site, armed) }
    }
}

/// Strong override of the weak C stub. Configures a cross-wake link.
#[cfg(debug_assertions)]
#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_DebugCoordinatorSetWake(trigger: c_int, target: c_int) {
    if let Some(f) = plugin().debug_set_wake {
        // SAFETY: resolved plugin entry with the declared signature.
        unsafe { f(trigger, target) }
    }
}

/// Strong override of the weak C stub. Releases a parked site.
#[cfg(debug_assertions)]
#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_DebugCoordinatorRelease(site: c_int) {
    if let Some(f) = plugin().debug_release {
        // SAFETY: resolved plugin entry with the declared signature.
        unsafe { f(site) }
    }
}

/// Strong override of the weak C stub. Returns how many times `site` was
/// reached, or 0 when the plugin is absent.
#[cfg(debug_assertions)]
#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_DebugCoordinatorReached(site: c_int) -> c_uint {
    match plugin().debug_reached {
        // SAFETY: resolved plugin entry with the declared signature.
        Some(f) => unsafe { f(site) },
        None => 0,
    }
}

/// Strong override of the weak C stub. Resets the compaction coordinator.
#[cfg(debug_assertions)]
#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_DebugResetCompactionController() {
    if let Some(f) = plugin().debug_reset {
        // SAFETY: resolved plugin entry with the declared signature.
        unsafe { f() }
    }
}
