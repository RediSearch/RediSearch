/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

fn main() {
    #[cfg(any(feature = "unittest", feature = "c_bench"))]
    build_utils::bind_foreign_c_symbols();
    #[cfg(feature = "c_bench")]
    compile_baseline();
}

#[cfg(feature = "c_bench")]
fn compile_baseline() {
    use std::{env, fs, path::PathBuf, process::Command};
    let root = build_utils::repository_root().unwrap();
    let revision = Command::new("git")
        .args(["rev-parse", "master"])
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(
        revision.status.success(),
        "the C benchmark requires a local master revision"
    );
    let revision = String::from_utf8(revision.stdout).unwrap();
    let revision = revision.trim();
    println!("cargo:rustc-env=GEO_C_BASELINE={revision}");
    let mut source = String::from("#define REDIS_MODULE_TARGET\n");
    for symbol in [
        "NewGeoFilter",
        "GeoFilter_SetField",
        "GeoFilter_Free",
        "LegacyGeoFilter_Free",
        "GeoFilter_LegacyParse",
        "GeoFilter_Validate",
        "GeoDistance_Parse",
        "GeoDistance_Parse_Buffer",
        "GeoDistance_ToString",
        "isWithinRadius",
        "RegisterGeoFunctions",
    ] {
        source.push_str(&format!("#define {symbol} C_{symbol}\n"));
    }
    for path in ["src/geo_index.c", "src/aggregate/functions/geo.c"] {
        let contents = Command::new("git")
            .args(["show", &format!("{revision}:{path}")])
            .current_dir(&root)
            .output()
            .unwrap();
        assert!(
            contents.status.success(),
            "C benchmark source unavailable: {path}"
        );
        let contents = std::str::from_utf8(&contents.stdout)
            .unwrap()
            .replace(
                "#include \"function.h\"",
                "#include \"aggregate/functions/function.h\"",
            )
            .replace("static int geofunc_distance(", "int C_geofunc_distance(")
            .replace(
                "\"geodistance\", geofunc_distance,",
                "\"geodistance\", C_geofunc_distance,",
            );
        source.push_str(&contents);
        source.push('\n');
    }
    let shim = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("geo_master.c");
    fs::write(&shim, source).unwrap();
    build_utils::compile_c_bench_shim(shim.to_str().unwrap());
}
