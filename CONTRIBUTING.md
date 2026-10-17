# Contributing to DocumentDb

First off, thank you for considering contributing to DocumentDb! It's people like you that make DocumentDb such a great tool.

## Code of Conduct

By participating in this project, you are expected to uphold our Code of Conduct. We expect everyone to be respectful, inclusive, and professional.

## How Can I Contribute?

### Reporting Bugs

This section guides you through submitting a bug report for BLite. Following these guidelines helps maintainers and the community understand your report, reproduce the behavior, and find related reports.

- **Check existing issues**: Verify that your issue hasn't already been reported.
- **Use a clear title**: Describe the problem concisely.
- **Provide a reproduction**: Include a minimal code sample that demonstrates the bug.
- **Describe the environment**: OS version, .NET version, etc.

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion for DocumentDb, including completely new features and minor improvements to existing functionality.

- **Use a clear title**: Describe the suggestion concisely.
- **Provide a step-by-step description**: Explain how the feature would work.
- **Explain why this enhancement would be useful**: What problem does it solve?

### Pull Requests

1.  **Fork the repository** and create your branch from `main`.
2.  **Clone the repository** to your local machine.
3.  **Create a new branch**: `git checkout -b my-new-feature`
4.  **Make your changes** and commit them: `git commit -m 'Add some feature'`
5.  **Push to the branch**: `git push origin my-new-feature`
6.  **Submit a pull request**.

## Styleguides

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

### C# Styleguide

- We follow standard .NET coding conventions.
- Use `Span<T>` and `Memory<T>` where possible to minimize allocations.
- Avoid reflection; use Source Generators instead.
- Write unit tests for all new features.

## Testing

We use `xUnit` for testing. Please ensure all tests pass before submitting your PR.

```bash
dotnet test
```

### Oracle tests

An oracle test checks BLite against an independent answer instead of hand-written expectations: `DifferentialQueryTests` runs ~330 query shapes on an indexed collection, an unindexed one and LINQ-to-Objects; `StoreModelTests` replays random writes, transactions, checkpoints, backups and reopens against an in-memory model. They live in `tests/BLite.Tests/Oracles/`; [docs/ORACLE_TESTS.md](docs/ORACLE_TESTS.md) describes them, their seeds and the candidate registry.

- **Adding one**: put `<Name>Tests.cs` under `Oracles/` (entities in `BLite.Shared`), seeded and deterministic, with the seed and the failing row's name in every failure message.
- **Every oracle test ships a seed**: `Oracles/Seeds/<Name>Tests.<defect>.patch`, a change to `src/` only (a revert of a past fix, or a hand-made defect) that the test was shown to catch by applying it locally. Describe the defect and the failures it causes above the diff. `OracleSeedsTests` checks in CI that each oracle has a seed and that every seed still applies; `dotnet run tools/oracle-seeds.cs` re-proves locally that each one is caught. Refresh a seed when a refactor strands it.
- **Known candidates**: a row that disagrees and is not fixed yet goes into the test's `Candidates` registry with its family label and the issue that tracks it. While its `Issue` is empty the row keeps failing, labelled `[known candidate: <family>]`; once the issue is filled in the row passes and writes `SKIPPED [known candidate: <family>, <issue>]` to the test output, which is the only trace a run shows because xunit 2 has no dynamic skip. When the fix lands the row agrees and fails with `candidate fixed: remove the entry for '<name>'`: remove the entry in the fix's PR.

### Commit trailers

Fix commits name how the defect was found and what kind it is, as git trailers after the body:

```
Found-By: OT
Defect-Class: IDX
```

- `Found-By:` `RV` code review, `OT` oracle test, `IS` issue report, `US` user, `CA` analyst.
- `Defect-Class:` `IDX` index serving diverges from scan, `WAL` durability and replay, `TXN` isolation, `BSON` encoding, `LINQ` translation, `MP` multi-process, `PLT` platform.

`git log --format='%(trailers:key=Found-By,valueonly)' | grep . | sort | uniq -c` counts the finders.

Thank you/Grazie per il tuo contributo! 🚀
