use cbindgen::{self, Config};
use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(Config::from_file("cbindgen.toml").expect("Failed to find cbindgen config"))
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("../headers/triemap.h");
}
