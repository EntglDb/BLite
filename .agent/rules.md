# DocumentDb Project Rules

## Performance Constraints

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
