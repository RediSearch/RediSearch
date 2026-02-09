fn main() {
    // Link static libraries needed by the tests
    #[cfg(feature = "link_oss")]
    {
        // The location of the static libraries is not the same as the OSS (redisearch) project.
        let root =
            build_utils::git_root().expect("Could not find git root for static library linking");
        let bin_dir = root.join("build").join("deps").join("RediSearch");

        // This requires us to set the BINDIR environment variable to point to the correct location
        // for the build_utils::bind_foreign_c_symbols function to find them.
        unsafe {
            std::env::set_var("BINDIR", &bin_dir);
        }

        build_utils::link_static_lib(&root.join("build"), "vecsim_disk", "vecsim_disk").unwrap();
        build_utils::bind_foreign_c_symbols();
    }
}
