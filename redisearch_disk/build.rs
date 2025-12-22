fn main() {
    // Link static libraries needed by the tests
    #[cfg(feature = "unittest")]
    {
        // The location of the static libraries is not the same as the OSS (redisearch) project.
        // This requires us to set the BINDIR environment variable to point to the correct location
        // for the build_utils::link_static_libraries function to find them.
        let root =
            build_utils::git_root().expect("Could not find git root for static library linking");
        let bind_dir = root.join("build").join("deps").join("RediSearch");

        unsafe {
            std::env::set_var("BINDIR", bind_dir);
        }

        build_utils::link_static_libraries(&[
            ("src/util/arr", "arr"),
            ("src/util/dict", "dict"),
            ("src/ttl_table", "ttl_table"),
        ]);
    }
}
