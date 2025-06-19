use std::ffi::{c_char, c_int};

pub const REDISMODULE_OK: i32 = 0;
pub const REDISMODULE_ERR: i32 = 1;

macro_rules! reply_func {
    ($basename:ident, $($arg:ty),*) => {
        #[allow(non_snake_case)]
        #[allow(dead_code)]
        unsafe extern "C" fn $basename(_ctx: *mut ffi::RedisModuleCtx, $(_: $arg),*) -> c_int {
            REDISMODULE_OK
        }
    };
}
reply_func!(RedisModule_ReplyWithLongLong, i64);
reply_func!(RedisModule_ReplyWithSimpleString, *const c_char);
reply_func!(RedisModule_ReplyWithError, *const c_char);
reply_func!(RedisModule_ReplyWithArray, usize);
reply_func!(RedisModule_ReplyWithDouble, f64);

/*

REPLY_FUNC(WithStringBuffer, const char *, size_t)
REPLY_FUNC(WithString, RedisModuleString)
*/
