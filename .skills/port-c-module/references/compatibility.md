# C-to-Rust compatibility checklist

Use this during planning and review. Select relevant risks, add those found in
the code, and record evidence/checks. This is not an exhaustive list or a demand
to test every item on every port.

| Area | Questions that can change the migration |
| --- | --- |
| Numbers | Are overflow, narrowing casts, signedness, shifts, division, NaN/infinity, signed zero, or float comparisons observable? Is behavior different across build modes or platforms? |
| Representation | Do C callers depend on size, alignment, field offsets, padding, unions, bitfields, or packed data? Are bytes persisted or sent over a wire? `repr(C)` alone does not establish serialization compatibility. |
| Pointers and ownership | Who allocates, borrows, mutates, retains, and frees each value? Can pointers be null, unaligned, overlapping, interior, or invalidated by reallocation? Are Rust references valid for the entire borrow? |
| Lifetimes and cleanup | Do callbacks retain pointers? What happens on errors, cancellation, deletion, or shutdown? Are allocator/deallocator pairs and destructor timing preserved? |
| Concurrency | Which state is shared? Check locks, atomic ordering, callback reentrancy, thread affinity, and teardown. Do proposed `Send`/`Sync` implementations have a safety argument? |
| Inputs and failures | Are byte strings assumed to be UTF-8 or NUL-terminated? Check embedded NULs, malformed encodings, empty input, extreme lengths, errors, and new panic paths. Invalid UTF-8 does not inherently panic; conversions and their handling matter. |
| Search behavior | Where relevant, check RESP2/RESP3, dialects, standalone/cluster replies, ordering, GC, expiration, RDB loading/saving, and allocation accounting. Mark irrelevant variants with a short reason. |
| ABI and FFI | Verify calling conventions, exported symbols, integer/enum widths, ownership transfer, out-parameters, and how failures cross the boundary. Do not allow an unexpected panic to unwind through C. |

Add tests for uncovered edge cases. Compare outputs, errors, accepted inputs,
side effects, and persisted bytes where those are part of the contract. Use
controlled processes for crash-prone reproducers. Static review, sanitizers,
and applicable Rust tools complement tests; none alone proves memory safety.

Distinguish defined behavior, implementation-dependent behavior, suspected bugs,
and undefined operations. Do not use a crashing or undefined C execution as a
golden expected result. Preserve legitimate behavior through a safe implementation.
When the intended outcome is unclear, record evidence, affected inputs/users,
less-breaking alternatives, and the decision required in a scoped batch finding.
Adding coverage does not authorize changing product behavior.
