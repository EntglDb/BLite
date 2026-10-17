using System.Collections;
using Xunit.Abstractions;

namespace BLite.Tests.Oracles;

/// <summary>A row of an oracle test known to disagree, the family of defects it belongs to and the issue that tracks it.</summary>
public sealed record Candidate(string Name, string Family, string Issue);

/// <summary>
/// The known candidates of an oracle test. While a listed row disagrees: with an empty
/// <see cref="Candidate.Issue"/> it fails with its family label, so a candidate is never quarantined
/// before it is filed; with an issue it passes and reports <c>SKIPPED</c> with the label and the issue
/// through the test output (xunit 2 has no dynamic skip). Once a listed row agrees it fails with
/// "candidate fixed", so the entry is removed in the fix's PR and the list never goes stale.
/// </summary>
public sealed class Candidates : IEnumerable<Candidate>
{
    private readonly Dictionary<string, Candidate> _entries = new();

    /// <summary>Lists <paramref name="names"/> under one family; <paramref name="issue"/> stays empty until it is filed.</summary>
    public void Add(string family, string issue, params string[] names)
    {
        foreach (var name in names)
            _entries.Add(name, new Candidate(name, family, issue));
    }

    /// <summary>Passes a row that agrees and is not listed, or that disagrees and is listed with an issue; fails every other row.</summary>
    /// <param name="failure">The disagreement, or null when the row agrees with its oracle.</param>
    /// <param name="output">Receives the <c>SKIPPED</c> line of a listed row with an issue.</param>
    public void Verify(string name, string? failure, ITestOutputHelper output)
    {
        var known = _entries.TryGetValue(name, out var c);
        if (failure == null)
        {
            Assert.False(known, $"candidate fixed: remove the entry for '{name}'");
            return;
        }
        if (!known) Assert.Fail(failure);
        if (c!.Issue.Length == 0) Assert.Fail($"[known candidate: {c.Family}] {failure}");
        output.WriteLine($"SKIPPED [known candidate: {c.Family}, {c.Issue}] {failure}");
    }

    /// <summary>Fails when an entry names no row of the test, e.g. after a row was renamed.</summary>
    public void AssertAllIn(IEnumerable<string> rows)
    {
        var stale = _entries.Keys.Except(rows).ToList();
        Assert.True(stale.Count == 0, $"candidates that name no row: {string.Join(", ", stale.Select(s => $"'{s}'"))}");
    }

    public IEnumerator<Candidate> GetEnumerator() => _entries.Values.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
