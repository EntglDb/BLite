# Contributing to DocumentDb

First off, thank you for considering contributing to DocumentDb! It's people like you that make DocumentDb such a great tool.

## Code of Conduct

By participating in this project, you are expected to uphold our Code of Conduct. We expect everyone to be respectful, inclusive, and professional.

## How Can I Contribute?

### Reporting Bugs

This section guides you through submitting a bug report for DocumentDb. Following these guidelines helps maintainers and the community understand your report, reproduce the behavior, and find related reports.

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

Thank you/Grazie per il tuo contributo! 🚀
