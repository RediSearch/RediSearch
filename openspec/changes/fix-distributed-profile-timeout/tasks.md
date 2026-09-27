# Implementation checklist

- [x] Inspect ticket, master dispatch, affected release refs, and backport labels.
- [x] Record the behavior-change proposal on Jira and prepare review artifacts.
- [ ] Obtain maintainer agreement on the proposal and design before implementation.
- [ ] Apply the request-local coordinator profile fallback before callback selection.
- [ ] Document the user-visible timeout exception alongside the implementation.
- [ ] Add focused RESP2/RESP3 cluster regression coverage for completion,
      responsiveness, both profile modes, and preserved global configuration.
- [ ] Validate the final build and relevant suites using repository verify skills;
      retain logs and report any gaps accurately.
- [ ] Complete independent review and address actionable findings.
- [ ] Mark the implementation PR ready, require release notes, apply the four
      affected-release backport labels, and verify its metadata.
- [ ] Address and resolve actionable PR threads; verify final CI before handoff.
