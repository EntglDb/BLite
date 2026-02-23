# DocumentDb Project Rules

## Performance Constraints

### Tool Usage
- **NEVER** use the `&` operator to combine commands in PowerShell (not supported). Use `;` or separate `run_command` calls instead.

### Development
- **ALWAYS** involve the user for refactoring tasks on sample and test files to focus on the core logic
- Ask the user for build or test to get stack trace

### Zero-Reflection Policy
- **NEVER** use reflection in hot paths or runtime code
- Source generators are preferred for code generation
- All type information must be resolved at compile-time

### Zero-Allocation Goals
- Minimize heap allocations in critical paths
- Use `Span<T>`, `ReadOnlySpan<T>`, and `Memory<T>` for buffer operations
- Leverage stack allocation where appropriate (`stackalloc`)
- Reuse buffers through `ArrayPool<T>` or custom pooling
- Avoid LINQ in hot paths (use manual loops instead)
- Prefer value types over reference types when possible
- Use `ref struct` for temporary data structures

### High-Performance Requirements
- Memory-mapped files for efficient I/O
- Lock-free or minimal locking strategies
- Batch operations where possible
- Optimize for CPU cache locality
- Profile and benchmark all critical paths

## Code Quality

### Design Principles
- Prefer composition over inheritance
- Use interfaces for abstraction
- Keep public API surface minimal and intentional
- Use `readonly struct` for immutable value types

### Testing
- All public APIs must have unit tests
- Performance-critical code must have benchmarks
- Verify zero-allocation with BenchmarkDotNet memory diagnostics

## Agent Rules Maintenance

- **ALWAYS** update `.agent/rules.md` at the end of every development session to reflect new features, fixes, architectural decisions, and any new conventions introduced
- **ALWAYS** update `.agent/context.md` at the end of every development session: version history, new/changed APIs, new entities, new test files, new website pages
- Document new packages, new projects, new entities, new test files, and relevant API changes
- Keep both files accurate so any agent can onboard the project from scratch by reading them alone