---
name: writing-tests
description: Writes and reviews tests that catch a specific bug - assertion strength, the seam each test binds at, writing a test from an acceptance criterion, and checking that a test would notice a break. Use when writing or changing any test, when writing the test for a criterion, or when reviewing whether a test proves anything.
---
<!-- yours to edit. Copied once from the default template by outerlayer init 0.42.0; never overwritten. -->

# Writing tests

A test earns its place by catching a specific bug. Before keeping one, ask:
what is the smallest change to production code that would still pass this
test? If the answer is "almost anything", the test checks wiring, not
behavior. Strengthen it or delete it.

The rules here hold in any language and any test framework. The examples
use TypeScript with a Jest-style `expect`. Read each one as the equivalent
assertion in the repository's own framework.

## Assertion strength

These pass for almost any implementation, so they are never the only proof
in a test:

- An "is defined" or "is not null" check passes for any value at all.
- A truthy or falsy check. Assert the exact value instead.
- "Was called" proves it ran, not what it ran with.
- "Does not throw" on its own. Assert the return value too.

Negative forms state a real expectation and are fine: "was never called",
"is absent". Prefer:

- Exact equality on the actual return value or the whole object shape.
- "Called with" and the full argument shape.
- Positional equality on lists. It catches reordering and off-by-one.
- A negative assertion that pins an injection path shut, such as
  `expect(query).not.toContain(rawUserValue)`.
- On an error path, the message or status a user or operator sees, not only
  that something failed.

Do not pin brittle internals: exact log strings, incidental call counts, the
order of unrelated calls. Those break on refactors without catching bugs.

## Choose the seam

Name the seam before writing the test: the boundary the test drives the code
through.

- Business behavior is proven below the user interface, in the service or
  the pure function.
- A user interface contract is proven at the component level, through what
  the user sees and does.
- Behavior that lives in the data store, such as an access rule, a trigger
  or the real shape of a query, is proven against the real store.
- A criterion that spans two seams gets two tests, one at each.

Check two blind spots every time:

- **Fake-seam blindness.** A fake that returns every field whatever the query
  asked for proves nothing about the query. Pin the contract: assert the
  request the code sent, make the fake honor what was asked for, or test
  against the real dependency.
- **Structural pinning.** "X is never called for Y" needs a spy that is
  present and asserts zero calls. A missing fake skips silently, exactly like
  the real skip.

A field that crosses a module or package boundary needs an assertion at every
hop: writer, relay, reader. Pinning only the ends leaves the middle free to
drop it.

## Write a test from its criterion

When a spec has acceptance criteria, the criteria judge finds the test that
proves each one by reading the tests and the code they run. Name the test
for the behavior it checks, so someone reading a failure knows what broke:

```ts
it("refuses a password reset for an unknown address without saying so", () => {
  // ...
});
```

Write the test from the criterion's words, at the seam it names, before the
implementation exists. Run it and read the failure. It must fail because the
behavior is absent, not because of a typo or a missing import.

A criterion with a proof form such as `(proof: screenshot)` is proven by the
evidence a reviewer sees, not by a test. It may still have one.

## Check that the test would notice

A green suite proves the tests run, not that they would catch a bug. Check
the code the change adds or alters.

For every condition, guard, limit and assignment the change adds, break it:
invert it, widen it, drop half of an `and`, ignore the field. Run the one
test file that should catch it. If nothing fails, write the assertion that
would. Then take each criterion and break the production code it describes.
Its test must fail. A test bound to a criterion that survives the break is
not coverage.

## The repository's own rules

The instructions file may name test commands, required checks, mocking rules
or coverage thresholds. Those come first where they are stricter than this
skill. Add them here when they are the team's standard, because this file is
the team's to edit.
