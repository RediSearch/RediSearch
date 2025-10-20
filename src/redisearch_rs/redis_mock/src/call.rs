use std::ffi::{CString, c_char, c_int};

fn init_mock_call_reply_functions() {
    // Register call reply functions
    unsafe { redis_module::raw::RedisModule_CallReplyType = Some(RedisModule_CallReplyType) };
    unsafe { redis_module::raw::RedisModule_CallReplyLength = Some(RedisModule_CallReplyLength) };
    unsafe {
        redis_module::raw::RedisModule_CallReplyArrayElement =
            Some(RedisModule_CallReplyArrayElement)
    };
    unsafe {
        redis_module::raw::RedisModule_CallReplyStringPtr = Some(RedisModule_CallReplyStringPtr)
    };
    unsafe { redis_module::raw::RedisModule_FreeCallReply = Some(RedisModule_FreeCallReply) };

    // Cast the variadic C function pointer to the expected Redis module function pointer type.
    //
    // The C function signature is: RedisModuleCallReply* RedisModule_CallImpl(RedisModuleCtx *ctx, const char *cmdname, const char *fmt, ...)
    // We use transmute to cast between function pointer types since Rust doesn't support variadic function types
    // in function pointer signatures, but the C ABI will handle the variadic arguments correctly.
    //
    // First cast to raw pointer, then transmute to the expected function pointer type.
    unsafe {
        let raw_ptr = RedisModule_CallImpl as *const ();
        redis_module::raw::RedisModule_Call = Some(std::mem::transmute(raw_ptr))
    }
}

// Mock reply structure to simulate RedisModuleCallReply
#[repr(C)]
struct MockCallReply {
    reply_type: c_int,
    string_data: CString,
    array_data: Vec<CString>,
}

impl MockCallReply {
    fn new_array_from_strings(strings: Vec<CString>) -> Self {
        Self {
            reply_type: redis_module::raw::REDISMODULE_REPLY_ARRAY as c_int,
            string_data: CString::new("").unwrap(), // Empty for arrays
            array_data: strings,
        }
    }

    fn new_string(s: &str) -> Self {
        Self {
            reply_type: redis_module::raw::REDISMODULE_REPLY_STRING as c_int,
            string_data: CString::new(s).unwrap(),
            array_data: Vec::new(),
        }
    }
}

#[allow(non_snake_case)]
#[unsafe(export_name = "RedisModule_CallImpl")]
pub extern "C" fn RedisModule_CallImpl(
    ctx: *mut ffi::RedisModuleCtx,
    cmdname: *const c_char,
    _fmt: *const c_char,
    _keyname: *mut ffi::RedisModuleString,
) -> *mut redis_module::raw::RedisModuleCallReply {
    // Check if this is an HGETALL command
    let cmd_str = unsafe { std::ffi::CStr::from_ptr(cmdname) };
    if cmd_str.to_bytes() != b"HGETALL" {
        return std::ptr::null_mut();
    }

    let ctx: &LoadDocumentTestContext = unsafe { &*(ctx as *const LoadDocumentTestContext) };

    // Create array elements: [key1, value1, key2, value2, ...]
    let mut elements = Vec::new();
    for (k, v) in ctx.access_key_values().iter() {
        elements.push(k.clone());
        let value_str = v.as_str().unwrap_or("");
        let value_str = CString::new(value_str).unwrap();
        elements.push(value_str);
    }

    let reply = Box::new(MockCallReply::new_array_from_strings(elements));
    Box::into_raw(reply) as *mut redis_module::raw::RedisModuleCallReply
}

#[allow(non_snake_case)]
// Mock functions to handle the call reply operations
#[unsafe(export_name = "_RedisModule_CallReplyType.1")]
pub extern "C" fn RedisModule_CallReplyType(
    reply: *mut redis_module::raw::RedisModuleCallReply,
) -> c_int {
    let mock_reply = unsafe { &*(reply as *const MockCallReply) };
    mock_reply.reply_type
}

#[allow(non_snake_case)]
#[unsafe(export_name = "_RedisModule_CallReplyLength.1")]
pub extern "C" fn RedisModule_CallReplyLength(
    reply: *mut redis_module::raw::RedisModuleCallReply,
) -> usize {
    let mock_reply = unsafe { &*(reply as *const MockCallReply) };
    mock_reply.array_data.len()
}

#[allow(non_snake_case)]
#[unsafe(export_name = "_RedisModule_CallReplyArrayElement.3")]
pub extern "C" fn RedisModule_CallReplyArrayElement(
    reply: *mut redis_module::raw::RedisModuleCallReply,
    idx: usize,
) -> *mut redis_module::raw::RedisModuleCallReply {
    let mock_reply = unsafe { &*(reply as *const MockCallReply) };
    if idx >= mock_reply.array_data.len() {
        return std::ptr::null_mut();
    }

    // Create a boxed string element and leak it (Redis will handle cleanup)
    let element = MockCallReply::new_string(mock_reply.array_data[idx].to_str().unwrap_or(""));
    let boxed = Box::new(element);
    Box::into_raw(boxed) as *mut redis_module::raw::RedisModuleCallReply
}

#[allow(non_snake_case)]
#[unsafe(export_name = "_RedisModule_CallReplyStringPtr.1")]
pub extern "C" fn RedisModule_CallReplyStringPtr(
    reply: *mut redis_module::raw::RedisModuleCallReply,
    len: *mut usize,
) -> *const c_char {
    let mock_reply = unsafe { &*(reply as *const MockCallReply) };
    if !len.is_null() {
        unsafe {
            *len = mock_reply.string_data.as_bytes().len();
        }
    }
    mock_reply.string_data.as_ptr()
}

#[allow(non_snake_case)]
#[unsafe(export_name = "_RedisModule_FreeCallReply.1")]
pub extern "C" fn RedisModule_FreeCallReply(reply: *mut redis_module::raw::RedisModuleCallReply) {
    unsafe {
        drop(Box::from_raw(reply as *mut MockCallReply));
    }
}
