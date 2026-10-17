using System.Diagnostics;

namespace BLite.Tests.Oracles;

/// <summary>
/// Every oracle test ships at least one seed: a patch under <c>Oracles/Seeds/</c> named
/// <c>&lt;TestClass&gt;.&lt;defect&gt;.patch</c> that plants a known defect in <c>src/</c> and that the
/// test was shown to catch by applying it locally. These checks keep the seeds present and
/// applicable without building them; <c>tools/oracle-seeds.cs</c> proves locally that they are caught.
/// </summary>
public class OracleSeedsTests
{
    private static readonly string s_root = FindRepoRoot();
    private static readonly string s_oracles = Path.Combine(s_root, "tests", "BLite.Tests", "Oracles");

    public static TheoryData<string> OracleTests =>
        new(Directory.GetFiles(s_oracles, "*Tests.cs").Select(Path.GetFileNameWithoutExtension).Where(n => n != nameof(OracleSeedsTests))!);

    public static TheoryData<string> Seeds => new(Directory.GetFiles(Path.Combine(s_oracles, "Seeds"), "*.patch").Select(Path.GetFileName)!);

    [Theory, MemberData(nameof(OracleTests))]
    public void OracleTest_HasASeed(string testClass) =>
        Assert.True(Directory.GetFiles(Path.Combine(s_oracles, "Seeds"), testClass + ".*.patch").Length > 0,
            $"{testClass} has no seed: add Oracles/Seeds/{testClass}.<defect>.patch and prove it fails the test by local revert");

    [Theory, MemberData(nameof(Seeds))]
    public void Seed_TouchesOnlySrc(string seed)
    {
        var (_, paths, error) = Git("apply", "--numstat", Path.Combine(s_oracles, "Seeds", seed));
        var files = paths.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => l.Split('\t')[^1]).ToList();
        Assert.True(files.Count > 0, $"{seed} changes no file: {error}");
        Assert.All(files, f => Assert.StartsWith("src/", f));
    }

    [Theory, MemberData(nameof(Seeds))]
    public void Seed_AppliesToTheCurrentTree(string seed)
    {
        var (exit, _, error) = Git("apply", "--check", Path.Combine(s_oracles, "Seeds", seed));
        Assert.True(exit == 0, $"{seed} no longer applies, refresh it against src/: {error}");
    }

    private static (int Exit, string Out, string Error) Git(params string[] args)
    {
        using var git = Process.Start(new ProcessStartInfo("git", args)
            { WorkingDirectory = s_root, RedirectStandardOutput = true, RedirectStandardError = true })!;
        var error = git.StandardError.ReadToEndAsync();
        var output = git.StandardOutput.ReadToEnd();
        git.WaitForExit();
        return (git.ExitCode, output, error.Result);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "BLite.slnx"))) dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("BLite.slnx not found above " + AppContext.BaseDirectory);
    }
}
