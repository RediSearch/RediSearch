fn main() {
    #[cfg(feature = "unittest")]
    build_utils::link_redisearch_c();
}
