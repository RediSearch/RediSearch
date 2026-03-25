# Enhanced TOLIST: What Does TOLIST List?

After reviewing the PRD and investigating the codebase, we found several design issues that seem to share a root cause.

A discussion with Adriano clarified the expected behavior for `TOLIST *`: the response should contain entries identified by their Redis key (`key: payload` structure). This is not stated in the PRD.

---

## Root Question

What does TOLIST collect?

- `TOLIST @field` collects **values**. Two docs with `rating = 8.5` produce one entry.
- `TOLIST *` collects **documents**, each identified by its Redis key. Two docs are always two entries.

Given 5 movies (movie:4 and movie:5 have identical content):

**Collecting values** — `REDUCE TOLIST 1 @title`:
```
scifi:  ["Inception", "Matrix", "Alien"]
comedy: ["Groundhog Day"]                  ← two docs, one value
```

**Collecting documents** — `REDUCE TOLIST 1 *`:
```
scifi:  {movie:1: {title: "Inception", ...}, movie:2: {title: "Matrix", ...}, ...}
comedy: {movie:4: {title: "Groundhog Day", ...}, movie:5: {title: "Groundhog Day", ...}}
```

---

## Consequences

- **Response format** — values produce a flat array (`["Inception", "Matrix"]`), documents produce a keyed map (`{movie:1: {...}, movie:2: {...}}`). The shape differs because the collected "thing" differs. Both come from the same `TOLIST` reducer.

- **Dedup** — values can repeat across documents (e.g., two docs with `title = "Groundhog Day"`), so dedup is meaningful and well-defined. For documents, Redis keys are unique by definition — but if we dedup by content, two different documents with identical fields would collapse into one, and the system would implicitly choose which document (which key) to keep and show to the user. That selection is implicit and potentially confusing.

- **SORTBY** — each document owns its sort-key value: `movie:1` has `rating = 8.8`, unambiguous (1-to-1). A collected value can come from multiple documents with different sort-key values: `"Groundhog Day"` appears in movie:4 (`rating = 8.0`) and movie:5 (`rating = 7.5`) — which rating represents that title? (1-to-many).

---

`TOLIST @field` and `TOLIST *` answer "what do we collect?" differently. This needs a decision before we proceed.
