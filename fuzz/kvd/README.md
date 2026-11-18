# kvd

`procfuzz`'s own test target: a small multi-threaded RESP server with three
planted bugs (a crash, a wrong answer behind the `--compact` launch flag, and
a hang), and its model. It exercises the core end to end, with real
processes, sockets, healing, crashes, and hangs, in about two seconds and
without a sanitizer build: `cargo test -p kvd` reproduces each bug from a
fixed program and shrinks padded programs to exactly the planted ones.
