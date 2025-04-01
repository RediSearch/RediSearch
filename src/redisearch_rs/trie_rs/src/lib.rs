#[cfg(feature = "ffi")]
pub mod ffi;

pub mod trie;
pub(crate) mod node;



/// Registers the Redis module allocator
/// as the global allocator for the application.
/// Disabled in tests.
#[cfg(all(feature = "redis_allocator", not(test)))]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;


#[cfg(test)]
pub(crate) mod test {
    use std::ffi::c_char;

    pub trait ToCCharArray<const N: usize> {
        /// Convenience method to convert a byte array to a C-compatible character array.    
        fn c_chars(self) -> [c_char; N];
    }

    impl<const N: usize> ToCCharArray<N> for [u8; N] {
        #![allow(dead_code)]
        fn c_chars(self) -> [c_char; N] {
            self.map(|b| b as c_char)
        }
    }
}