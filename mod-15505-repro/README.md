# MOD-15505 ingest regression repro harness

End-to-end harness for reproducing the MS MARCO ingest regression reported in
**MOD-15505** (v2.10.12 → v8.6.4, ~30% slower indexing). Builds Search from
git tags inside a single Docker builder image, loads each `.so` into a
matching Redis container, and times `FT.CREATE` over a pre-loaded slice of
the public MS MARCO dataset.

Investigated on macOS Docker; results inconclusive due to a noisy host (see
[Findings](#findings)). This branch exists so the harness can be re-run on a
quiet Linux box (EC2).

## Prereqs

- Docker (Linux daemon strongly preferred; macOS Docker Desktop is too noisy
  to bisect — see [Findings](#findings))
- `python3` with `redis` (`pip install redis`)
- `redis-cli` on the host (any recent version)
- ~5 GB free disk (build worktrees + module artifacts + data slice)
- Outbound HTTPS access (public S3, no AWS creds needed)

## One-time setup

From inside this directory:

```bash
# 1. Builder image (one-time, ~5 min, ~1.5 GB)
docker build -f Dockerfile.builder -t mod15505-builder:latest .

# 2. Public MS MARCO slice (~700 MB raw → ~89k docs)
./fetch-data.sh                  # or: ./fetch-data.sh 1400 for ~180k docs

# 3. (Sanity) point at a clean RediSearch checkout
export REPO_ROOT=/path/to/RediSearch     # if not running from inside one
```

Worktrees, modules, logs and data all go to `$PWD/mod-15505-work/`
(override with `WORK_DIR=...`). Nothing is written into the source repo.

## Build the tags you want

```bash
for tag in v2.10.12 v2.10.20 v2.10.22 v2.10.24 v2.10.26 v2.10.28 v2.10.30; do
  ./build-tag.sh "$tag"
done
```

Each build takes ~1–3 min in the builder image. Outputs land at
`mod-15505-work/modules/redisearch-<tag>.so`.

- Tags use `git worktree add --detach <tag>` so a single checkout supports
  many concurrent tag worktrees.
- 2.10.x is built standalone (`make build`, not the coordinator) — the
  coordinator artifact would fail with `ERRCLUSTER Uninitialized cluster
  state` on a single node.
- `apply-boost-patch.sh` is invoked automatically when a worktree is
  created. It backports the boost-1.88 SHA1 API fix (officially landed in
  v8.4.9) to tags that need it: v2.10.16..v2.10.30, v8.0.x, v8.2.x. Tags
  pre-dating `src/util/hash/hash.cpp` (v2.10.0..v2.10.14) and tags newer
  than v8.4.9 are unaffected.
- Some 8.x tags pin a Rust nightly via `.rust-nightly`; `build-tag.sh`
  installs the requested toolchain on demand.

## Run the ladder

```bash
# 30k docs, 3 runs per tag, index-only timing
./run-bisect.sh results-ladder.csv \
    v2.10.12 v2.10.20 v2.10.22 v2.10.24 v2.10.26 v2.10.28 v2.10.30

# Bigger / longer / more runs to fight noise:
N_DOCS=70000 N_RUNS=5 ./run-bisect.sh results-large.csv \
    v2.10.12 v2.10.20 v2.10.30
```

The script writes `<csv>` with raw per-run numbers and prints a median
summary at the end. 2.10.x tags are loaded into `redis:7.4`, 8.x tags into
`redis:8.6`.

Run order matters on noisy hosts — see [Findings](#findings). Always include
both ends of the regression (baseline + target) in **every** ladder so
they share environmental drift.

## Layout

```
mod-15505-repro/
├── Dockerfile.builder      # builder image (ubuntu:22.04 + all deps)
├── build-tag.sh            # build one tag -> mod-15505-work/modules/
├── apply-boost-patch.sh    # boost-1.88 SHA1 fix, auto-applied
├── fetch-data.sh           # download MS MARCO slice (no creds)
├── load.py                 # ingest+index timer (live / index modes)
├── run-bisect.sh           # ladder driver
└── patches/                # patched hash.{cpp,h} from v8.4.9+
```

## Findings to carry into the Linux re-run

The macOS Docker investigation produced data too noisy to trust commit-by-commit
(the host flipped between two stable performance modes ~20 % apart mid-run,
contaminating every bisect). Two qualitative observations are still worth
inheriting:

1. **At 30k docs, v2.10.12 vs v2.10.30 reproduced a ~+25 % gap** that
   matches the ticket's ~30 % cloud-benchmark regression in spirit. v8.4.9
   and newer looked fully recovered.
2. **At 70k docs the gap shrank to noise** (~46 s vs ~50 s, well inside
   per-run variance). If real, this is consistent with the regression being
   a **fixed per-`FT.CREATE` setup cost** (~4–5 s), not a per-doc throughput
   loss — its percentage contribution would shrink with larger N.

Treat both as hypotheses, not confirmations.

### Open questions

1. **Does the regression survive in a quiet env?** Re-run the initial
   7-tag ladder at 30k docs on EC2; expect the v2.10.12 → v2.10.30 step to
   reappear cleanly (variance should drop to a few %).
2. **Is it setup or throughput?** Repeat the v2.10.12 vs v2.10.30 pair at
   N = 10k, 30k, 70k, 150k. If the **absolute** gap (seconds) is roughly
   constant, it's setup overhead. If the **relative** gap (%) is
   constant, it's per-doc.
3. **If setup overhead**: code-diff `v2.10.12..v2.10.30 -- src/spec.c
   src/document.c src/module-init src/indexer.c` for new init paths.
4. **If per-doc**: commit-bisect with `run-bisect.sh` (`-N 70000 -R 5`),
   passing 4–6 tags at a time, then 4–6 commits within the offending
   patch window. The harness handles arbitrary commit SHAs as tag args.

### Suggested EC2 sizing

- `c7g.2xlarge` (8 vCPU Graviton3, arm64) or `c7i.2xlarge` (8 vCPU x86)
- Ubuntu 22.04+, install Docker, clone the repo, run setup steps above.
- Run **nothing else** during the ladder; `htop` should show idle outside
  of redis-server.

## Notes / quirks

- The harness creates git worktrees as **detached HEAD** — it never alters
  a tag's ref or your current branch.
- `--cpus=8` is hard-coded in `run-bisect.sh`'s docker invocation; tune if
  your host has fewer cores.
- macOS host: `head -n -1` doesn't exist — `fetch-data.sh` uses python to
  trim partial trailing lines, which works everywhere.
- The CSV uses `>9 KB` rows; pipeline size of 200 produces ~2 MB packets,
  which is comfortable for redis on a local socket.
