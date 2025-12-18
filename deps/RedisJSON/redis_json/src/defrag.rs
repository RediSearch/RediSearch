use std::{
    alloc::Layout,
    os::raw::{c_int, c_void},
};

use ijson::{Defrag, DefragAllocator};
use lazy_static::lazy_static;
use redis_module::{
    defrag::DefragContext, raw, redisvalue::RedisValueKey, Context, RedisGILGuard, RedisResult,
    RedisValue,
};
use redis_module_macros::{defrag_end_function, defrag_start_function};

use crate::redisjson::RedisJSON;

#[derive(Default)]
pub(crate) struct DefragStats {
    defrag_started: usize,
    defrag_ended: usize,
    keys_defrag: usize,
}

lazy_static! {
    pub(crate) static ref DEFRAG_STATS: RedisGILGuard<DefragStats> = RedisGILGuard::default();
}

struct DefragCtxAllocator<'dc> {
    defrag_ctx: &'dc DefragContext,
}

impl<'dc> DefragAllocator for DefragCtxAllocator<'dc> {
    unsafe fn realloc_ptr<T>(&mut self, ptr: *mut T, _layout: Layout) -> *mut T {
        self.defrag_ctx.defrag_realloc(ptr)
    }

    /// Allocate memory for defrag
    unsafe fn alloc(&mut self, layout: Layout) -> *mut u8 {
        self.defrag_ctx.defrag_alloc(layout)
    }

    /// Free memory for defrag
    unsafe fn free<T>(&mut self, ptr: *mut T, layout: Layout) {
        self.defrag_ctx.defrag_dealloc(ptr, layout)
    }
}

#[defrag_start_function]
fn defrag_start(defrag_ctx: &DefragContext) {
    let mut defrag_stats = DEFRAG_STATS.lock(defrag_ctx);
    defrag_stats.defrag_started += 1;
    ijson::reinit_shared_string_cache();
}

#[defrag_end_function]
fn defrag_end(defrag_ctx: &DefragContext) {
    let mut defrag_stats = DEFRAG_STATS.lock(defrag_ctx);
    defrag_stats.defrag_ended += 1;
}

#[allow(non_snake_case, unused)]
pub unsafe extern "C" fn defrag(
    ctx: *mut raw::RedisModuleDefragCtx,
    key: *mut raw::RedisModuleString,
    value: *mut *mut c_void,
) -> c_int {
    let defrag_ctx = DefragContext::new(ctx);

    let mut defrag_stats = DEFRAG_STATS.lock(&defrag_ctx);
    defrag_stats.keys_defrag += 1;

    let mut defrag_allocator = DefragCtxAllocator {
        defrag_ctx: &defrag_ctx,
    };
    let value = value.cast::<*mut RedisJSON<ijson::IValue>>();
    let new_val = defrag_allocator.realloc_ptr(*value, Layout::new::<RedisJSON<ijson::IValue>>());
    if !new_val.is_null() {
        std::ptr::write(value, new_val);
    }
    std::ptr::write(
        &mut (**value).data as *mut ijson::IValue,
        std::ptr::read(*value).data.defrag(&mut defrag_allocator),
    );
    0
}

pub(crate) fn defrag_info(ctx: &Context) -> RedisResult {
    let defrag_stats = DEFRAG_STATS.lock(ctx);
    Ok(RedisValue::OrderedMap(
        [
            (
                RedisValueKey::String("defrag_started".to_owned()),
                RedisValue::Integer(defrag_stats.defrag_started as i64),
            ),
            (
                RedisValueKey::String("defrag_ended".to_owned()),
                RedisValue::Integer(defrag_stats.defrag_ended as i64),
            ),
            (
                RedisValueKey::String("keys_defrag".to_owned()),
                RedisValue::Integer(defrag_stats.keys_defrag as i64),
            ),
        ]
        .into_iter()
        .collect(),
    ))
}
