// Proves that every oracle test catches its seeded defects. Local only, never run in CI.
//
//   dotnet run tools/oracle-seeds.cs [name filter]
//
// For each tests/BLite.Tests/Oracles/Seeds/<TestClass>.<defect>.patch: a scratch git worktree of
// HEAD under the OS temp directory, the patch applied, tests/BLite.Tests built, the test class run.
// A seed is caught when at least one row fails that is not a known candidate (a "[known candidate:"
// failure fails without the seed too). Uncommitted changes are not part of the scratch worktree.
// Prints one line per seed and exits 1 if any seed is missed or cannot be run.

using System.Diagnostics;
using System.Xml.Linq;

var root = Run(Environment.CurrentDirectory, "git", "rev-parse", "--show-toplevel").Output.Trim();
var seedsDir = Path.Combine(root, "tests", "BLite.Tests", "Oracles", "Seeds");
var filter = args.Length > 0 ? args[0] : "";
var seeds = Directory.GetFiles(seedsDir, "*.patch").Where(s => Path.GetFileName(s).Contains(filter)).Order().ToList();
var allCaught = seeds.Count > 0;

foreach (var seed in seeds)
{
    var name = Path.GetFileName(seed);
    var testClass = name[..name.IndexOf('.')];
    var scratch = Path.Combine(Path.GetTempPath(), $"blite-seed-{Guid.NewGuid():N}");
    var verdict = "";
    try
    {
        Require(Run(root, "git", "worktree", "add", "--detach", scratch, "HEAD"), "worktree");
        Require(Run(scratch, "git", "apply", seed), "apply");
        Require(Run(scratch, "dotnet", "build", "tests/BLite.Tests", "-v", "q", "-nologo"), "build");
        var trx = Path.Combine(scratch, "seed.trx");
        Run(scratch, "dotnet", "test", "tests/BLite.Tests", "--no-build", "-nologo",
            "--filter", $"FullyQualifiedName~BLite.Tests.Oracles.{testClass}", "--logger", $"trx;LogFileName={trx}");
        if (!File.Exists(trx)) throw new SeedError("test: no results");

        XNamespace ns = "http://microsoft.com/schemas/VisualStudio/TeamTest/2010";
        var failed = XDocument.Load(trx).Descendants(ns + "UnitTestResult")
            .Where(r => (string?)r.Attribute("outcome") == "Failed")
            .Select(r => (Test: (string?)r.Attribute("testName") ?? "?", Message: r.Descendants(ns + "Message").FirstOrDefault()?.Value ?? ""))
            .ToList();
        var caught = failed.Where(f => !f.Message.Contains("[known candidate:")).ToList();
        allCaught &= caught.Count > 0;
        verdict = caught.Count > 0
            ? $"CAUGHT  {name}: {caught.Count} new failure(s), e.g. {caught[0].Test}"
            : $"MISSED  {name}: no failure of {testClass} beyond its {failed.Count} known candidate(s)";
    }
    catch (SeedError e)
    {
        allCaught = false;
        verdict = $"ERROR   {name}: {e.Message}";
    }
    finally
    {
        Run(root, "git", "worktree", "remove", "--force", scratch);
    }
    Console.WriteLine(verdict);
}

if (seeds.Count == 0) Console.WriteLine($"no seed matches '{filter}' in {seedsDir}");
return allCaught ? 0 : 1;

static (int Exit, string Output) Run(string dir, string file, params string[] args)
{
    using var p = Process.Start(new ProcessStartInfo(file, args)
        { WorkingDirectory = dir, RedirectStandardOutput = true, RedirectStandardError = true })!;
    var err = p.StandardError.ReadToEndAsync();
    var output = p.StandardOutput.ReadToEnd();
    p.WaitForExit();
    return (p.ExitCode, output + err.Result);
}

static void Require((int Exit, string Output) result, string step)
{
    if (result.Exit != 0)
        throw new SeedError($"{step} failed: {result.Output.Trim().Split('\n').LastOrDefault(l => l.Trim().Length > 0)?.Trim()}");
}

sealed class SeedError(string message) : Exception(message);
