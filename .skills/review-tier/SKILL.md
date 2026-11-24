---
name: review-tier
description: Assign a RediSearch, RediSearchEnterprise, or VectorSimilarity pull request its review risk tier (risk:T0 to risk:T3, plus experimental) under the team's "Search code review procedure", and explain which rule and which file decided it. Use whenever someone asks what tier or risk label a PR should get, whether a PR needs a code-owner or peer review, whether it can self-merge, whether the experimental exception applies, or what must happen before it merges — even if they only paste a PR number or say "how risky is this change". Reads the current procedure from Confluence each run, so it follows the draft as it changes.
---

# Review tier

Decide which tier a single PR belongs in, and show the evidence. The tier decides who reviews
the PR and what has to happen before merge, so a wrong low tier lets risky code through with
only a self-review. When the evidence is ambiguous, choose the higher tier and say why. The
procedure lets anyone raise a tier but never lower it, so erring high is cheap and erring low
is not.

## Inputs

`$ARGUMENTS` is a PR number or URL, a commit or ref range, or nothing (then use the current
branch against `master`). The default repo is `RediSearch/RediSearch`. For a PR, collect:

```bash
gh pr view <N> --json number,title,body,labels,additions,deletions,files,headRefOid
gh pr diff <N>
```

For a merged PR that is already on `master`, `git show --stat <sha>` and `git show <sha>` work
too, and they don't use the network.

## Step 1: Read the procedure

Fetch the page with the Atlassian MCP tool `getConfluencePage`:
- cloudId: `redislabs.atlassian.net`
- pageId: `6996099089` ("Search code review procedure")
- contentFormat: `markdown`

From the page, take the tier table, the tie-break rules, the T3 path list for the PR's repo,
the experimental conditions, and the "Required before merge" items. The page is a draft that
changes, so use what it says today. Paths, the size limit, and tier names can all move. If the
page can't be fetched, tell the user and stop. A tier computed from remembered rules looks
authoritative and may be wrong.

## Step 2: Measure the change

Count changed lines (additions plus deletions) without generated files and test data, because
the size limits exclude them. In RediSearch, generated files include
`src/redisearch_rs/headers/*.h`, `Cargo.lock`, `src/query_parser/v2/parser.c` (from
`parser.y`), and `src/aggregate/expr/lexer.c` and other Ragel output (from `.rl`). Report both
numbers: the raw total and the counted total.

Also note which modules the PR touches. "Inside one module" is part of the T1 test.

## Step 3: Check the T3 paths, hunk by hunk

For each file, decide whether the change touches a T3 path. Many T3 entries name only part of
a file. In that case, read the hunks, because the file name alone doesn't settle it:
- `src/spec.c` is T3 only in its RDB save and load code (the `aux_save` and `aux_load` paths,
  the `*_RdbLoad` and `*_RdbSave` functions) and in the encoding versions in `spec.h`.
  Argument parsing in `spec.c` is not T3.
- `src/module.c` is T3 for command registration, arity, ACL categories, and `_FT.*`
  registrations.
- `src/config.c` is T3 for config names and defaults, and for the
  `ENABLE_UNSTABLE_FEATURES` check.
- Concurrency is defined by what the code does, not by path: adding or reordering locks, or
  sharing state across threads. Look for GIL calls (`RedisModule_ThreadSafeContextLock`), the
  spec lock, `concurrent_ctx`, workers, `src/gc/`, and `index_gc/src/fork/`, and also for new
  threads or atomics anywhere.
- Any file that writes or reads RDB is T3 even if the page doesn't list it. Grep the hunks for
  `RedisModule_Save`, `RedisModule_Load`, `LoadUnsigned_IOError`, and encoding-version
  constants.

Quote the function or hunk that makes a file T3. "Touches `spec.c`" is not evidence;
"changes `IndexSpec_RdbLoad` to read a new field" is.

The path list is not the whole T3 definition. If the page also gives a general rule (for
example, anything that affects compatibility, contracts, concurrency, shared state, or resource
limits), apply it to every file, including files on no list. Small diffs often fall under it.
Read the PR description and commit messages for the reason behind the change, because authors
often state the concurrency reason there and not in the code. Phrases like "without the spec
read lock", "outlive a lock release", "dangling", "another thread", or a new `unsafe` block
justified by lock state all mean the change is about shared state, whatever its size.

## Step 4: Check the T2 triggers

These raise a PR to at least T2 whatever its size:
- It deletes a test, or loosens an assertion: a wider tolerance, a removed check, an
  `assert` turned into a log, or a skip added. A test-only PR that does this is T2, not T0.
- It changes agent instruction files: `.skills/`, `.claude/`, `AGENTS.md`, `CLAUDE.md`,
  `.cursor/`, `.github/copilot-instructions.md`, `.amp/` policy and prompt files, and similar
  files. Markdown here is still T2.
- It changes features or behavior in query, indexing, result-processing, or cluster code.

## Step 5: Check the experimental exception

Apply it only when the page's conditions all hold, and check each one against the diff:
- The new code can't run with the flag off. Find the `enableUnstableFeatures` check that guards
  each entry point.
- A test runs the entry points with the flag off and shows they are rejected. Name the test.
  If there isn't one, the exception doesn't apply.
- Nothing persisted changes with the flag off. A bumped encoding version counts as a change
  even when the payload bytes stay the same, because every RDB written from then on carries
  the new version.
- The PR description says the code is experimental and names the flag check.

To find the shared code, look for every hook the feature adds to a path that runs for all
indexes or queries, wherever it is in the diff: a GC scanner wired into each cycle, a new
check in a config setter or at module load, a new case in a shared parser or dispatcher. That
code runs with the flag off, so the flag doesn't protect it.

Even when the exception applies, some parts keep their normal tier: the flag check itself,
shared code outside the experimental path, and any persisted-format or FFI-contract change.
Those stay T3 even when only experimental code uses them. So an experimental PR often gets a
split verdict, such as "T1 for the experimental path, T3 for the RDB encoding in
`spec.c`". Report it that way. The PR's label is the highest tier among its parts.

## Step 6: Decide

Apply the page's tie-break rules. The highest tier of any touched part wins. A T1 PR over the
size limit becomes T2, unless it is experimental and meets the conditions. Then compare with
the PR's current `risk:` label, if it has one. An author's label can be raised but never
lowered, so if the label is higher than your result, keep the label and say that the author
chose it.

When two rows of the tier table both match and the page doesn't say which wins (for example,
a 40-line bug fix in result-processing code fits both T1's size test and T2's "behavior
change"), pick the higher tier and name the gap in the page.

Also note when the page requires an approved design first: a T3 change, a new feature, a new
user-visible command or option, or a contract change between repos. Note too when the PR is
over the size the author checklist asks for and should be split into a stack.

## Output

Write it as short prose that a reviewer can read in under a minute, not as a form:

1. The tier, in the form of the label to set: `risk:T2`, or `risk:T3` + `experimental`.
2. The deciding rule, with its evidence: the rule's wording from the page, the file, and the
   function or hunk.
3. Other rules that matched, and the ones you checked that didn't, in a line or two.
4. What the page requires before merge at this tier, as a checklist the author can work
   through. Mark the items the PR already meets, such as a linked design or a flag-off test.
5. A suggested "where to look" list: the one to three places that carry the risk.
6. Any ambiguity in the page that this PR exposed.

Don't set the label or comment on the PR unless the user asks. Labels and comments are visible
to the team, and this skill only gives an assessment.
