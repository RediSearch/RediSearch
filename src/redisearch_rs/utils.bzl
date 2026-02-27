load("@prelude//rules.bzl", "genrule")

def cbindgen(
    name,
    crate,
    workspace_metadata,
    srcs):
    genrule(
        name = name,
        cmd = f"""
        cbindgen \
            --config $SRCS \
            --crate {crate} \
            --metadata $(location {workspace_metadata}) \
            --output $OUT/generated.h \
            --depfile $OUT/DEFILE \
            --symfile $OUT/SYMFILE
        """,
        srcs = srcs,
        outs = {
            "header": ["generated.h"],
            "deps": ["DEPFILE"],
            "syms": ["SYMFILE"]
        }
    )
