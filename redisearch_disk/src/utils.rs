use std::path::Path;
use tracing::{debug, error};

/// Copies elements from `src` to `dst`, returning the number of elements copied.
///
/// This function copies as many elements as possible from `src` to `dst`, limited by
/// the smaller of the two slice lengths.
///
/// # Arguments
/// * `dst` - The destination buffer to copy into
/// * `src` - The source buffer to copy from
///
/// # Returns
/// The number of elements actually copied (min of `dst.len()` and `src.len()`)
///
/// # Examples
/// ```
/// use redisearch_disk::utils::fill_buf;
///
/// let src = vec![1, 2, 3, 4, 5];
/// let mut dst = vec![0; 3];
///
/// let copied = fill_buf(&mut dst, &src);
/// assert_eq!(copied, 3);
/// assert_eq!(dst, vec![1, 2, 3]);
/// ```
pub fn fill_buf<T>(dst: &mut [T], src: &[T]) -> usize
where
    T: Copy,
{
    let count = dst.len().min(src.len());
    dst[..count].copy_from_slice(&src[..count]);
    count
}

/// Retrieves a Redis configuration value by name.
///
/// This function calls `CONFIG GET <config_name>` and extracts the value from the response.
/// Similar to the C function `getRedisConfigValue` in config.c.
///
/// # Arguments
/// * `ctx` - Redis module context
/// * `config_name` - Name of the configuration to retrieve
///
/// # Returns
/// * `Some(String)` - The configuration value if found
/// * `None` - If the configuration is not found or an error occurred
pub fn get_redis_config_value(ctx: &redis_module::Context, config_name: &str) -> Option<String> {
    match ctx.call("config", &["get", config_name]) {
        Ok(redis_module::RedisValue::Array(arr)) => {
            // CONFIG GET returns an array of [name, value]
            // We need the value at index 1
            if arr.len() >= 2 {
                match &arr[1] {
                    redis_module::RedisValue::BulkString(value) => Some(value.clone()),
                    redis_module::RedisValue::SimpleString(value) => Some(value.clone()),
                    _ => None,
                }
            } else {
                None
            }
        }
        Ok(_) => {
            debug!("CONFIG GET {} returned unexpected type", config_name);
            None
        }
        Err(e) => {
            error!("Failed to get config value '{}': {:?}", config_name, e);
            None
        }
    }
}

/// Computes the disk storage path from the bigredis-path configuration.
///
/// Takes the bigredis-path (e.g., "/var/opt/redislabs/flash/bigstore-1"),
/// extracts the parent directory, and appends "/redisearch".
///
/// # Arguments
/// * `bigredis_path` - The bigredis-path configuration value
///
/// # Returns
/// * `Some(String)` - The computed disk path (e.g., "/var/opt/redislabs/flash/redisearch")
/// * `None` - If the path is invalid (no parent directory)
///
/// # Examples
/// ```
/// use redisearch_disk::utils::compute_disk_path;
///
/// assert_eq!(
///     compute_disk_path("/var/opt/redislabs/flash/bigstore-1"),
///     Some("/var/opt/redislabs/flash/redisearch".to_string())
/// );
/// assert_eq!(compute_disk_path("/"), None);
/// assert_eq!(compute_disk_path(""), None);
/// ```
pub fn compute_disk_path(bigredis_path: &str) -> Option<String> {
    let path = Path::new(bigredis_path);

    // Get the parent directory
    let parent = path.parent()?;

    // Parent must not be empty (i.e., bigredis_path must have at least one component after root)
    if parent.as_os_str().is_empty() {
        return None;
    }

    // Append "/redisearch" to the parent directory
    let disk_path = parent.join("redisearch");

    disk_path.to_str().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_disk_path_with_trailing_slash() {
        // Path::parent handles trailing slashes correctly
        assert_eq!(
            compute_disk_path("/var/opt/redislabs/flash/bigstore-1/"),
            Some("/var/opt/redislabs/flash/redisearch".to_string())
        );
    }

    #[test]
    fn test_compute_disk_path_root_only() {
        // Root path "/" has no parent we can use
        assert_eq!(compute_disk_path("/"), None);
    }

    #[test]
    fn test_compute_disk_path_empty() {
        assert_eq!(compute_disk_path(""), None);
    }

    #[test]
    fn test_compute_disk_path_single_component() {
        // "/foo" -> parent is "/" which is not empty, so we get "/redisearch"
        assert_eq!(compute_disk_path("/foo"), Some("/redisearch".to_string()));
    }
}
