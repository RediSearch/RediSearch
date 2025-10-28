use rsvalue_bencher as _;

unsafe extern "C" {
    fn RSValue_NewNumber(num: f64) -> *mut std::ffi::c_void;
    fn RSValue_Number_Get(num: *mut std::ffi::c_void) -> f64;
}

fn main() {
    let ptr = unsafe { RSValue_NewNumber(123456.123456) };
    let number = unsafe { RSValue_Number_Get(ptr) };

    println!("ptr: {ptr:?}");
    println!("number: {number:?}");
}
