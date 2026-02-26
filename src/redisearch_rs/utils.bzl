load("@prelude//rules.bzl", "genrule")

def cbindgen(
    name,
    crate,
    workspace_metadata,
    srcs,
    header,
    visibility):
    genrule(
        name = name,
        cmd = f"""
        cbindgen \
            --config $SRCS \
            --crate {crate} \
            --metadata $(location {workspace_metadata}) \
            --output $OUT/{header} \
            --depfile $OUT/DEFILE \
            --symfile $OUT/SYMFILE
        """,
        srcs = srcs,
        outs = {
            "header": [header],
            "deps": ["DEPFILE"],
            "syms": ["SYMFILE"]
        },
        visibility = visibility
    )
