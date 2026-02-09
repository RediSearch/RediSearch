#[cfg(test)]
pub fn rs_array<const N: usize, T: Copy>(fields: [T; N]) -> *mut T {
    let arr = unsafe {
        let size_t_u16 = const { size_of::<T>() as u16 };
        let len_u32 = const { N as u32 };

        ffi::array_new_sz(size_t_u16, 0, len_u32).cast::<T>()
    };

    unsafe {
        let elements = std::slice::from_raw_parts_mut(arr, fields.len());

        elements.copy_from_slice(&fields);
    }

    arr
}
