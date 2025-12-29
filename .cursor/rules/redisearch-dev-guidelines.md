# RediSearch Development Guidelines

## Professional C/C++/Rust Development Standards

### Code Quality Principles
- **Performance First**: Always consider memory allocation patterns, cache efficiency, and algorithmic complexity
- **Safety & Correctness**: Prefer explicit error handling, validate inputs, use assertions for invariants
- **Human Readability**: Code should tell a story - use descriptive names, clear control flow, and meaningful comments
- **Maintainability**: Follow existing patterns in the codebase, avoid clever tricks that obscure intent

### Code Analysis Approach
1. **Understand Before Acting**: Always read and understand existing code patterns before making changes
2. **Preserve Intent**: When fixing bugs, maintain the original design intent unless fundamentally flawed
3. **Minimal Changes**: Fix the specific issue without unnecessary refactoring
4. **Consistent Style**: Match existing code style in the file/module you're working on

### Decision Making Framework
When multiple solutions exist:
1. **Present Options**: Clearly enumerate 2-3 viable approaches
2. **Provide Analysis**: 
   - Performance implications (memory, CPU, I/O)
   - Maintainability impact
   - Risk assessment
   - Code complexity changes
3. **Recommend with Justification**: State preferred approach and reasoning
4. **Seek Guidance**: Ask for user preference when trade-offs are significant

### Task Breakdown Strategy
For complex changes:
1. **Identify Independent Components**: 
   - Core logic changes (can be developed in parallel)
   - Test additions/updates (can be done separately)
   - Documentation updates (can be done independently)
2. **Define Interfaces First**: Establish function signatures and data structures before implementation
3. **Suggest Parallel Work**: When possible, break into sub-tasks that different agents could handle:
   - Agent A: Core implementation
   - Agent B: Test coverage
   - Agent C: Error handling and edge cases
   - Agent D: Performance optimizations

### RediSearch-Specific Patterns
- **Memory Management**: Use `rm_malloc`/`rm_free`, follow existing patterns for cleanup
- **Error Handling**: Use `QueryError` infrastructure, follow existing timeout/OOM policies
- **Threading**: Respect existing synchronization patterns, use atomic operations appropriately
- **Testing**: Follow existing test patterns, ensure both unit and integration test coverage
- **Coordination**: Understand shard vs coordinator responsibilities, respect existing communication patterns

### Forbidden Actions Without Explicit Permission
- Large-scale refactoring across multiple modules
- Changing established APIs without backward compatibility consideration
- Performance optimizations that significantly increase code complexity
- Adding new dependencies without justification
- Modifying core data structures without understanding full impact

### Code Review Mindset
- **Question Assumptions**: "Is this change necessary?"
- **Consider Alternatives**: "What other approaches exist?"
- **Assess Impact**: "What could this break?"
- **Think Long-term**: "How does this affect future development?"

### Communication Style
- Be explicit about trade-offs and assumptions
- Provide concrete examples when explaining approaches
- Use precise technical language appropriate for the domain
- Always explain the "why" behind recommendations, not just the "what"
