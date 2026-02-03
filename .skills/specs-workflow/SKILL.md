---
name: specs-workflow
description: Guidelines for using and maintaining specification files in the specs/ folder
---

# Specs Workflow

This skill defines how agents should use and maintain specification markdown files in the `specs/` folder when developing new features.

## Purpose

Specification files serve as living documentation that:
- Capture feature requirements and design decisions
- Provide context for agents working on features across multiple sessions
- Document the evolution of features as they are developed
- Enable better collaboration between agents and developers
- Serve as a single source of truth for feature implementation status

## When to Create a Spec

Create a new spec file when:
- Starting development of a new feature
- Planning a significant refactoring or architectural change
- Porting a complex C module to Rust that requires multi-step planning
- Working on a feature that will span multiple development sessions
- The user explicitly requests a spec to be created

Do NOT create a spec for:
- Simple bug fixes
- Minor code cleanups
- Single-file changes
- Tasks that can be completed in one session without complex planning

## Spec File Structure

### Location
All spec files should be placed in the `specs/` folder at the repository root.

### Naming Convention
- Use kebab-case: `feature-name.md`
- Be descriptive but concise: `hybrid-search-api.md`, `port-triemap-to-rust.md`
- Include the module or component name when relevant

### Required Sections

Every spec file MUST include these sections:

```markdown
# Feature Name

## Status
Current status: [Planning | In Progress | Completed | Blocked]
Last updated: YYYY-MM-DD

## Overview
Brief description of the feature, its purpose, and goals.

## Requirements
- Functional requirements
- Non-functional requirements (performance, compatibility, etc.)
- Constraints and limitations

## Design
High-level design decisions and approach.
Include architecture diagrams if helpful (using Mermaid).

## Implementation Plan
- [ ] Step 1: Description
- [ ] Step 2: Description
- [ ] Step 3: Description

## Dependencies
- External dependencies (libraries, modules)
- Internal dependencies (other features, modules)
- Blockers or prerequisites

## Testing Strategy
- Unit tests required
- Integration tests required
- Performance benchmarks (if applicable)

## Open Questions
- Unresolved design decisions
- Areas needing clarification

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| YYYY-MM-DD | Decision made | Why it was made |

## References
- Related issues, PRs, or documentation
- External resources
```

## Agent Responsibilities

### When Starting Work on a Feature

1. **Check for existing spec**: Look in `specs/` for a relevant specification
2. **Read the spec thoroughly**: Understand the context, decisions, and current status
3. **Update status**: Mark the spec status as "In Progress" if starting work
4. **Review implementation plan**: Understand what has been completed and what remains

### During Development

1. **Update progress**: Check off completed items in the Implementation Plan
2. **Document decisions**: Add significant design decisions to the Decisions Log
3. **Track blockers**: Update the status to "Blocked" and document blockers if encountered
4. **Add open questions**: Document uncertainties or areas needing user input
5. **Keep it current**: Update the "Last updated" date whenever making changes

### When Completing Work

1. **Final update**: Mark all completed items in the Implementation Plan
2. **Update status**: Change status to "Completed" when feature is fully implemented
3. **Document outcomes**: Add any final notes about implementation differences from the plan
4. **Verify testing**: Ensure the Testing Strategy section reflects what was actually tested

### Best Practices

- **Be concise**: Specs should be readable, not exhaustive
- **Link to code**: Reference specific files or modules when relevant
- **Use task lists**: Leverage markdown checkboxes for tracking progress
- **Version control**: Specs are committed alongside code changes
- **Ask before major changes**: If the implementation needs to deviate significantly from the spec, ask the user first
- **Cross-reference**: Link to related specs when features interact

## Example Workflow

```
User: "I want to add hybrid search support to RediSearch"

Agent:
1. Creates specs/hybrid-search.md with initial structure
2. Fills in Overview, Requirements, and Design sections based on discussion
3. Creates detailed Implementation Plan
4. Begins work, updating the spec as progress is made
5. Documents design decisions in the Decisions Log
6. Marks tasks complete as they finish
7. Updates status to "Completed" when done
```

## Integration with Existing Skills

Specs complement existing skills:
- `/port-c-module`: Create a spec for complex module ports
- `/verify`: Check that implementation matches the spec
- `/write-rust-tests`: Reference the Testing Strategy section

## Notes

- Specs are living documents - they should evolve as understanding improves
- Not every detail needs to be in the spec - focus on decisions and context
- When in doubt, ask the user if a spec would be helpful

