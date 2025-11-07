use crate::RedisModuleCtx;

pub fn dummy_context() {}

unsafe extern "C" {
    static mut RSDummyContext: *mut RedisModuleCtx;
}

/// Get the RediSearch module context.
///
/// Safety:
/// - The Redis module must be initialized. Therefore,
///   this function is Undefined Behavior in unit tests.
#[inline]
pub unsafe fn redisearch_module_context() -> *mut RedisModuleCtx {
    RSDummyContext
}
