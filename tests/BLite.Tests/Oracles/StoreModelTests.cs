using System.Diagnostics;
using System.Text;
using BLite.Core.Storage;
using Xunit.Abstractions;

namespace BLite.Tests.Oracles;

/// <summary>
/// Model-based test of a typed collection: a seeded random sequence of Insert, Update, Delete,
/// Upsert, InsertBulk, DeleteBulk, FindById, explicit transaction Begin / Commit / Rollback,
/// Checkpoint, Backup, and Dispose + reopen runs against the collection and against an in-memory
/// dictionary model. After every step the collection's committed contents (a full scan, the count,
/// point lookups and the two secondary indexes) must equal the model's committed view; inside a
/// transaction, reads through it must equal the model's pending view. A failing sequence is shrunk
/// to a shorter one that still fails and printed step by step.
/// </summary>
/// <remarks>
/// The contract the model encodes (README "Transactions &amp; ACID", the write methods' docs):
/// a write without a transaction commits on its own; a write in a caller-owned transaction is
/// visible through that transaction only until <c>CommitAsync</c>; <c>RollbackAsync</c> and a
/// <c>Dispose</c> of the context discard it; a <c>BLiteValidationException</c> (duplicate key)
/// leaves the transaction usable while any other failure rolls it
/// back; a unique index violation is also a BLiteValidationException (checked before any write);
/// a checkpoint, a backup and a reopen change nothing.
/// </remarks>
public class StoreModelTests
{
    private readonly ITestOutputHelper _output;

    public StoreModelTests(ITestOutputHelper output) => _output = output;

    private const int Steps = 300;

    public static TheoryData<int, bool> Seeds => new()
    {
        { 1, false }, { 2, false }, { 3, false }, { 4, false }, { 5, false },
        { 1, true }, { 2, true }, { 3, true },
    };

    /// <summary>Runs known to fail, by <see cref="Label"/>, see <see cref="Candidates"/>. None on upstream 0450b97.</summary>
    private static readonly Candidates s_candidates = new();

    private static string Label(int seed, bool multiProcess) => $"seed {seed}, multiProcess={multiProcess}";

    [Fact]
    public void Candidates_NameExistingRuns() => s_candidates.AssertAllIn(Seeds.Select(r => Label((int)r[0], (bool)r[1])));

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task RandomOperations_MatchTheModel(int seed, bool multiProcess)
    {
        var ops = Generate(seed, Steps);
        var label = Label(seed, multiProcess);

        var failure = await Replay(ops, multiProcess);
        if (failure == null)
        {
            s_candidates.Verify(label, null, _output);
            return;
        }

        var shrunk = await Shrink(ops.Take(failure.Step + 1).ToList(), multiProcess, TimeSpan.FromSeconds(45));
        var sb = new StringBuilder();
        sb.AppendLine($"{label}: step {failure.Step + 1} of {ops.Count} ({ops[failure.Step]}) failed: {failure.Message}");
        sb.AppendLine($"Shortest failing sequence found ({shrunk.Ops.Count} of {failure.Step + 1} steps), failing at its step {shrunk.Failure.Step + 1}: {shrunk.Failure.Message}");
        for (int i = 0; i < shrunk.Ops.Count; i++)
            sb.AppendLine($"  {i + 1,3}. {shrunk.Ops[i]}");
        s_candidates.Verify(label, sb.ToString(), _output);
    }

    // ── Replay against the model ──────────────────────────────────────────────

    private sealed record Failure(int Step, string Message);

    private sealed record ShrinkResult(List<StoreOp> Ops, Failure Failure);

    private static List<StoreOp> Generate(int seed, int steps) => StoreModel.Generate(seed, steps);

    private static async Task<Failure?> Replay(List<StoreOp> ops, bool multiProcess)
    {
        var path = Path.Combine(Path.GetTempPath(), $"store_model_{Guid.NewGuid():N}.db");
        var backupPath = Path.Combine(Path.GetTempPath(), $"store_model_backup_{Guid.NewGuid():N}.db");
        var config = multiProcess ? PageFileConfig.Default with { AllowMultiProcessAccess = true } : PageFileConfig.Default;

        var model = new StoreModelRunner(path, backupPath, config);
        try
        {
            for (int step = 0; step < ops.Count; step++)
            {
                var op = ops[step];
                try
                {
                    await model.ApplyAsync(op);
                    await model.VerifyAsync($"after {op}");
                }
                catch (StoreModelMismatchException ex)
                {
                    return new Failure(step, ex.Message);
                }
                catch (Exception ex)
                {
                    return new Failure(step, $"unexpected {ex.GetType().Name}: {ex.Message}");
                }
            }
            return null;
        }
        finally
        {
            model.Dispose();
            StoreModel.DeleteDatabase(path);
            StoreModel.DeleteDatabase(backupPath);
        }
    }

    /// <summary>
    /// Shrinks by removing chunks of steps (half the sequence, then quarters, ... down to single
    /// steps), keeping a removal if the sequence still fails (any failure counts), until a pass
    /// over single steps removes nothing or the time is up. The last step is never removed.
    /// </summary>
    private static async Task<ShrinkResult> Shrink(List<StoreOp> ops, bool multiProcess, TimeSpan budget)
    {
        var sw = Stopwatch.StartNew();
        var current = ops;
        var failure = (await Replay(current, multiProcess))!;
        int chunk = Math.Max(1, (current.Count - 1) / 2);
        while (sw.Elapsed < budget)
        {
            bool changed = false;
            for (int start = current.Count - 1 - chunk; start >= 0 && sw.Elapsed < budget; start -= chunk)
            {
                var candidate = current.Where((_, j) => j < start || j >= start + chunk).ToList();
                var f = await Replay(candidate, multiProcess);
                if (f != null)
                {
                    current = candidate;
                    failure = f;
                    changed = true;
                }
            }
            if (chunk == 1 && !changed) break;
            if (!changed) chunk = Math.Max(1, chunk / 2);
        }
        return new ShrinkResult(current, failure);
    }
}
