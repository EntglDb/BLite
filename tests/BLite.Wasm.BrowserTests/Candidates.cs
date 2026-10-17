using System.Text.RegularExpressions;

namespace BLite.Wasm.BrowserTests;

/// <summary>
/// One row of the registry table in <c>candidates.md</c>: a finding that is known and
/// not yet fixed. <see cref="Row"/> names the test rows it covers (test method and
/// label, <c>*</c> matches anything), <see cref="Match"/> is a substring of the failure
/// the row produces, <see cref="Issue"/> the issue that tracks it (empty until filed).
/// <see cref="Kind"/> is <c>always</c> (the row fails every run; a pass means the
/// candidate is fixed) or <c>sometimes</c> (the row fails in some runs; a pass proves nothing).
/// </summary>
public sealed record Candidate(string Id, string Kind, string Row, string Match, string Issue, string Summary)
{
    private Regex? _rowPattern;

    public bool Covers(string row)
    {
        _rowPattern ??= new Regex("^" + Regex.Escape(Row).Replace("\\*", ".*") + "$", RegexOptions.CultureInvariant);
        return _rowPattern.IsMatch(row);
    }
}

/// <summary>
/// Reads the registry table of <c>candidates.md</c> (copied next to the test assembly).
/// A row is <c>| BW&lt;n&gt; | always or sometimes | `row` | `match` | #issue | summary |</c>.
/// </summary>
public static class Candidates
{
    private static readonly Lazy<IReadOnlyList<Candidate>> s_all = new(Load);

    public static IReadOnlyList<Candidate> All => s_all.Value;

    public static IReadOnlyList<Candidate> Covering(string row) => All.Where(c => c.Covers(row)).ToList();

    private static IReadOnlyList<Candidate> Load()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "candidates.md");
        if (!File.Exists(path))
            return Array.Empty<Candidate>();

        var rows = new List<Candidate>();
        foreach (var line in File.ReadLines(path))
        {
            var cells = line.Split('|');
            if (cells.Length < 7 || !Regex.IsMatch(cells[1].Trim(), "^BW[0-9]+$"))
                continue;
            rows.Add(new Candidate(
                Id: cells[1].Trim(),
                Kind: cells[2].Trim(),
                Row: Backticked(cells[3]),
                Match: Backticked(cells[4]),
                Issue: cells[5].Trim(),
                Summary: cells[6].Trim()));
        }
        return rows;
    }

    private static string Backticked(string cell)
    {
        var start = cell.IndexOf('`');
        var end = cell.LastIndexOf('`');
        return start >= 0 && end > start ? cell.Substring(start + 1, end - start - 1) : cell.Trim();
    }
}
