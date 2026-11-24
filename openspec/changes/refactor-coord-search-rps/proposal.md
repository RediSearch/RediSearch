# Refactor coordinator search reduction

Use the shared result processor pipeline for distributed FT.SEARCH. This
lets the coordinator reuse sorting, pagination, request cleanup, and reply
handling while retaining the shard reply representations.

The compatibility requirements record the intended behavior and the approved
changes to KNN ordering and error precedence. Resuming reduction on the main
thread after a strict timeout belongs to the separate Drain work.
