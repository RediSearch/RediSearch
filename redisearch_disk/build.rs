fn main() {
    // Prevent both `link_oss` and `no_oss` features from being enabled simultaneously
    #[cfg(all(feature = "link_oss", feature = "no_oss"))]
    compile_error!("Cannot enable both 'link_oss' and 'no_oss' features at the same time");

    // Link static libraries needed by the tests
    #[cfg(feature = "link_oss")]
    {
        // The location of the static libraries is not the same as the OSS (redisearch) project.
        let root = build_utils::repository_root()
            .expect("Could not find git root for static library linking");
        let bin_dir = root.join("build").join("deps").join("RediSearch");

        // This requires us to set the BINDIR environment variable to point to the correct location
        // for the build_utils::bind_foreign_c_symbols function to find them.
        unsafe {
            std::env::set_var("BINDIR", &bin_dir);
        }

        build_utils::link_static_lib(&root.join("build"), "vecsim_disk", "vecsim_disk").unwrap();
        build_utils::bind_foreign_c_symbols();

        // MKL is linked separately from libredisearch_all.a (see RediSearch commit 355cb37f).
        // build_utils::link_mkl() (called by bind_foreign_c_symbols) looks for MKL relative
        // to BINDIR (build/deps/RediSearch/_deps/svs-src/lib/), but CMake's FetchContent
        // places it at the top-level build dir (build/_deps/svs-src/lib/).
        // Link it from the correct location.
        let mkl_dir = root.join("build/_deps/svs-src/lib");
        let mkl = mkl_dir.join("libmkl_static_library.a");
        if mkl.exists() {
            println!("cargo::rerun-if-changed={}", mkl.display());
            println!("cargo::rustc-link-search=native={}", mkl_dir.display());
            println!("cargo::rustc-link-lib=static:-bundle=mkl_static_library");
        }
    }
}
