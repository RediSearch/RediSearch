# A list of available rules and their signatures can be found here: https://buck2.build/docs/prelude/globals/

cxx_library(
    name = "RediSearch",
    headers = glob(["src/**/*.h", "src/**/*.hpp"]),
    srcs = glob(["src/**/*.c", "src/**/*.cpp"]),
    include_directories = ["src", "src/coord", "src/buffer", "src/value", "src/redisearch_rs/headers"],
    deps = [
        "//deps:rmalloc",
        "//deps:thpool",
        "//deps:RedisModulesSDK",
        "//deps:rmutil",
        "//deps:hiredis",
        "//deps:hiredis_ssl",
        "//deps:VectorSimilarity",
        "//deps:libuv",
        "//deps:libnu",
        "//deps:fast_float",
        "//deps:snowball",
        "//deps:friso",
        "//deps:geohash",
        "//deps:miniz",
        "//deps:phonetics",
        "//deps:openssl",
        "//deps:boost",
        "//src/redisearch_rs:redisearch_rs",
        # "//src/redisearch_rs:redis-module[staticlib]",
    ],
    lang_compiler_flags = {
        "cxx": ["-std=c++20"],
    },
    preprocessor_flags = [
        "-DREDISEARCH_MODULE_NAME=\"${MODULE_NAME}\"",
        "-DGIT_VERSPEC=\"${GIT_VERSPEC}\"",
        "-DGIT_SHA=\"${GIT_SHA}\"",
        "-DREDISMODULE_SDK_RLEC",
        "-D_GNU_SOURCE"
    ],
    visibility = ["PUBLIC"],
)
