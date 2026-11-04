# Implementation checklist

- [x] Inspect ticket, master dispatch, affected release refs, and backport labels.
- [x] Record the behavior-change proposal on Jira and prepare review artifacts.
- [x] Incorporate maintainer direction: keep STRICT and drain only buffered replies.
- [x] Use thread-safe try-pop for STRICT draining until the first empty pop.
- [x] Document best-effort profile completeness without changing timeout policy.
- [x] Add focused RESP2/RESP3 cluster regression coverage for completion,
      responsiveness, both profile modes, and preserved global configuration.
- [x] Validate the final build and relevant suites using repository verify skills;
      retain logs and report any gaps accurately.
- [x] Complete independent review and address actionable findings.
- [x] Mark the implementation PR ready, require release notes, apply the four
      affected-release backport labels, and verify its metadata.
- [ ] Address and resolve actionable PR threads; verify final CI before handoff.
