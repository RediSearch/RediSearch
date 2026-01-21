fn main() {
    #[cfg(feature = "unittest")]
    build_utils::bind_foreign_c_symbols();
}
