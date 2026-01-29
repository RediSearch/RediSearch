fn main() {
    // Link static libraries needed by the tests
    #[cfg(feature = "unittest")]
    {
        // The location of the static libraries is not the same as the OSS (redisearch) project.
        let root =
            build_utils::git_root().expect("Could not find git root for static library linking");
        let bin_dir = root.join("build").join("deps").join("RediSearch");

        let libs = [
            ("src/util/arr", "arr"),
            ("src/util/dict", "dict"),
            ("src/ttl_table", "ttl_table"),
        ];

        for (subdir, lib_name) in libs {
            // Ignore errors - library may not exist yet during initial build
            let _ = build_utils::link_static_lib(&bin_dir, subdir, lib_name);
        }
    }
}
