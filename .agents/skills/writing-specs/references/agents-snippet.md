<!-- outerlayer:writing-rules -->
## Write for a reader who was not in the session

This applies to anything a person reads: replies, commit messages, pull
request titles and bodies, issue bodies, review comments, docs and code
comments.

The reader test: someone who knows this codebase but has never seen this
session, the skill that ran it, or any label coined while working. If they
would have to ask what a word means, explain it or cut it. Names made up
during a session never appear in anything published: reviewer names, round
labels, finding ids like C1 or N3, stage names, internal agent roles.
Describe the thing instead.

Lead with the outcome. One sentence: what changed, or what you found. Then
only the detail that changes what the reader does next. Keep exact paths,
numbers, identifiers and caveats. Cut the packaging around them.

A sentence carries one idea and stays under about 20 words. A sentence with
three clauses becomes three sentences. Break paragraphs often.

Say what you mean. Use the literal phrase, not a metaphor or a flourish:
"a parameter worth varying", not "a dial worth turning". Use everyday words:
"use", not "leverage"; "start", not "initiate"; "so", not "thereby".

Session telemetry, review-round tables, agent counts and raw command output
belong in the report to the person who asked. They never go in a pull
request or issue body.

Before posting, read the text as that outsider. If they would stall on a
term or a sentence, rewrite it.

## Code comments

A comment states a present-tense fact the code cannot express: a why, a
constraint, a gotcha or an invariant. Everything else is noise that leaks
the history of how the code was written.

Never put these in a comment, a test title or a user-facing string:

- Change narration: "previously…", "now uses…", "removed X", "renamed from
  Y". Version control holds what changed. The comment says why the current
  code is the way it is.
- Issue, pull request or ticket numbers. They mean nothing to a reader
  without access. If the context matters, restate its substance.
- Project codenames, phase labels and spec section references. Describe
  the behavior instead.
- Labels from a working session: reviewer names, round labels, finding ids.

One exception: an acceptance-criterion id may sit in a comment directly
above the test that proves it, so a reader can find the test from the
criterion. Never put it in the test's title.

Keep comments that document constraints, invariants, footguns, security
posture and real rationale, and do not weaken them when editing nearby. If
a comment narrates a change but carries a real reason, rewrite it as
present-tense rationale rather than deleting it.
