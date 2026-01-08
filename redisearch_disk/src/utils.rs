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
