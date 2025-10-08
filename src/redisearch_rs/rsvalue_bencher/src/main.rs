use rsvalue_bencher as _;

unsafe extern "C" {
    fn RSValue_NewNumber(num: f64) -> *mut std::ffi::c_void;
}

fn main() {
    let ptr = unsafe { RSValue_NewNumber(0.0) };

    println!("ptr: {ptr:?}");
}
