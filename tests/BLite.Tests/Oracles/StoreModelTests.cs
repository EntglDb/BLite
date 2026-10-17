using System.Diagnostics;
using System.Text;
using BLite.Core;
using BLite.Core.Query;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;
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
    private const int IdPool = 120;
    private static readonly string[] s_codes = Enumerable.Range(0, 40).Select(i => $"c{i:00}").ToArray();
    private static readonly int[] s_payloadSizes = [0, 0, 20, 20, 300, 300, 2500, 2500, 18000];

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

    // ── Operations ────────────────────────────────────────────────────────────

    private enum OpKind { Insert, Update, Delete, Upsert, InsertBulk, DeleteBulk, FindById, Begin, Commit, Rollback, Checkpoint, Backup, Reopen }

    /// <summary>One step. Only inputs are recorded; what the step must do follows from the model at replay.</summary>
    private sealed record Op(OpKind Kind, int Id = 0, int Count = 0, string? Code = null, int PayloadLength = 0, int[]? Ids = null, StoreDoc[]? Docs = null)
    {
        public override string ToString() => Kind switch
        {
            OpKind.Insert or OpKind.Update or OpKind.Upsert => $"{Kind}(id={Id}, count={Count}, code={Code ?? "null"}, payload={PayloadLength} chars)",
            OpKind.InsertBulk => $"InsertBulk({string.Join(", ", Docs!.Select(d => $"id={d.Id}/count={d.Count}/code={d.Code ?? "null"}/payload={d.Payload.Length}"))})",
            OpKind.DeleteBulk => $"DeleteBulk(ids={string.Join(",", Ids!)})",
            OpKind.FindById or OpKind.Delete => $"{Kind}(id={Id})",
            _ => Kind.ToString(),
        };
    }

    private static List<Op> Generate(int seed, int steps)
    {
        var rnd = new Random(seed);
        var ops = new List<Op>(steps);
        var live = new HashSet<int>(); // the generator's own guess of which ids exist (to aim at them)

        StoreDoc Doc(int id) => new()
        {
            Id = id,
            Name = $"n{rnd.Next(1000)}",
            Count = rnd.Next(10),
            Code = rnd.Next(3) == 0 ? null : s_codes[rnd.Next(s_codes.Length)],
            Payload = new string((char)('a' + rnd.Next(26)), s_payloadSizes[rnd.Next(s_payloadSizes.Length)]),
        };
        int ExistingOrAny() => live.Count > 0 && rnd.Next(100) < 70 ? live.ElementAt(rnd.Next(live.Count)) : rnd.Next(1, IdPool + 1);

        for (int i = 0; i < steps; i++)
        {
            var roll = rnd.Next(100);
            if (roll < 22)
            {
                var d = Doc(rnd.Next(100) < 15 ? ExistingOrAny() : rnd.Next(1, IdPool + 1));
                ops.Add(new Op(OpKind.Insert, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
                live.Add(d.Id);
            }
            else if (roll < 36)
            {
                var d = Doc(ExistingOrAny());
                ops.Add(new Op(OpKind.Update, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
            }
            else if (roll < 48)
            {
                var id = ExistingOrAny();
                ops.Add(new Op(OpKind.Delete, id));
                live.Remove(id);
            }
            else if (roll < 56)
            {
                var d = Doc(ExistingOrAny());
                ops.Add(new Op(OpKind.Upsert, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
                live.Add(d.Id);
            }
            else if (roll < 60)
            {
                var docs = Enumerable.Range(0, rnd.Next(2, 9)).Select(_ => Doc(rnd.Next(1, IdPool + 1))).DistinctBy(d => d.Id).ToArray();
                ops.Add(new Op(OpKind.InsertBulk, Docs: docs));
                foreach (var d in docs) live.Add(d.Id);
            }
            else if (roll < 63)
            {
                var ids = Enumerable.Range(0, rnd.Next(2, 9)).Select(_ => ExistingOrAny()).Distinct().ToArray();
                ops.Add(new Op(OpKind.DeleteBulk, Ids: ids));
                foreach (var id in ids) live.Remove(id);
            }
            else if (roll < 71) ops.Add(new Op(OpKind.FindById, ExistingOrAny()));
            else if (roll < 78) ops.Add(new Op(OpKind.Begin));
            else if (roll < 86) ops.Add(new Op(OpKind.Commit));
            else if (roll < 90) ops.Add(new Op(OpKind.Rollback));
            else if (roll < 94) ops.Add(new Op(OpKind.Checkpoint));
            else if (roll < 97) ops.Add(new Op(OpKind.Backup));
            else ops.Add(new Op(OpKind.Reopen));
        }
        return ops;
    }

    // ── Replay against the model ──────────────────────────────────────────────

    private sealed record Failure(int Step, string Message);

    private sealed record ShrinkResult(List<Op> Ops, Failure Failure);

    private sealed class ModelMismatchException : Exception
    {
        public ModelMismatchException(string message) : base(message) { }
    }

    private static async Task<Failure?> Replay(List<Op> ops, bool multiProcess)
    {
        var path = Path.Combine(Path.GetTempPath(), $"store_model_{Guid.NewGuid():N}.db");
        // A directory of its own: a backup writes backup.manifest.json next to it, under that fixed
        // name, and other tests back up into the temp directory at the same time.
        var backupDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), $"store_model_backup_{Guid.NewGuid():N}")).FullName;
        var backupPath = Path.Combine(backupDir, "backup.db");
        var config = multiProcess ? PageFileConfig.Default with { AllowMultiProcessAccess = true } : PageFileConfig.Default;

        var committed = new Dictionary<int, StoreDoc>();
        var pending = new Dictionary<int, StoreDoc>();
        ITransaction? tx = null;
        var db = new StoreModelDbContext(path, config);
        try
        {
            for (int step = 0; step < ops.Count; step++)
            {
                var op = ops[step];
                try
                {
                    await Apply(op);
                    await Verify($"after {op}");
                }
                catch (ModelMismatchException ex)
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
            tx?.Dispose();
            db.Dispose();
            DeleteDatabase(path);
            try { Directory.Delete(backupDir, recursive: true); } catch { /* best-effort */ }
        }

        // The view a write goes to and is read back from: the open transaction's, else the committed one.
        Dictionary<int, StoreDoc> View() => tx != null ? pending : committed;

        bool CodeTaken(StoreDoc d) => d.Code != null && View().Values.Any(o => o.Id != d.Id && o.Code == d.Code);

        async Task Apply(Op op)
        {
            switch (op.Kind)
            {
                case OpKind.Insert:
                {
                    var d = Clone(op.Docs![0]);
                    if (View().ContainsKey(d.Id))
                    {
                        await ExpectDuplicateKey(() => db.Docs.InsertAsync(d, tx).AsTask(), op);
                    }
                    else if (CodeTaken(d))
                    {
                        await ExpectUniqueViolation(() => db.Docs.InsertAsync(d, tx).AsTask(), op);
                    }
                    else
                    {
                        var id = await db.Docs.InsertAsync(d, tx);
                        Check(id == d.Id, $"{op} returned id {id}");
                        View()[d.Id] = Clone(d);
                    }
                    break;
                }
                case OpKind.Update:
                {
                    var d = Clone(op.Docs![0]);
                    if (!View().ContainsKey(d.Id))
                    {
                        var updated = await db.Docs.UpdateAsync(d, tx);
                        Check(!updated, $"{op} returned true for a document that does not exist");
                    }
                    else if (CodeTaken(d))
                    {
                        await ExpectUniqueViolation(() => db.Docs.UpdateAsync(d, tx).AsTask(), op);
                    }
                    else
                    {
                        var updated = await db.Docs.UpdateAsync(d, tx);
                        Check(updated, $"{op} returned false for an existing document");
                        View()[d.Id] = Clone(d);
                    }
                    break;
                }
                case OpKind.Delete:
                {
                    var deleted = await db.Docs.DeleteAsync(op.Id, tx);
                    var expected = View().Remove(op.Id);
                    Check(deleted == expected, $"{op} returned {deleted}, expected {expected}");
                    break;
                }
                case OpKind.Upsert:
                {
                    var d = Clone(op.Docs![0]);
                    if (CodeTaken(d))
                    {
                        await ExpectUniqueViolation(() => db.Docs.UpsertAsync(d, tx).AsTask(), op);
                    }
                    else
                    {
                        var result = await db.Docs.UpsertAsync(d, tx);
                        var expectedInserted = !View().ContainsKey(d.Id);
                        Check(result.Inserted == expectedInserted, $"{op} reported Inserted={result.Inserted}, expected {expectedInserted}");
                        Check(result.Id == d.Id, $"{op} returned id {result.Id}");
                        View()[d.Id] = Clone(d);
                    }
                    break;
                }
                case OpKind.InsertBulk:
                {
                    // Only documents the model accepts: free ids and codes that are free among them too.
                    var batch = new List<StoreDoc>();
                    foreach (var d in op.Docs!)
                        if (!View().ContainsKey(d.Id) && !CodeTaken(d) && (d.Code == null || batch.All(b => b.Code != d.Code)))
                            batch.Add(Clone(d));
                    if (batch.Count == 0) break;
                    var ids = await db.Docs.InsertBulkAsync(batch, tx);
                    Check(ids.SequenceEqual(batch.Select(b => b.Id)), $"{op} returned ids [{string.Join(",", ids)}]");
                    foreach (var d in batch) View()[d.Id] = Clone(d);
                    break;
                }
                case OpKind.DeleteBulk:
                {
                    var deleted = await db.Docs.DeleteBulkAsync(op.Ids!, tx);
                    int expected = 0;
                    foreach (var id in op.Ids!) if (View().Remove(id)) expected++;
                    Check(deleted == expected, $"{op} returned {deleted}, expected {expected}");
                    break;
                }
                case OpKind.FindById:
                {
                    var found = await db.Docs.FindByIdAsync(op.Id);
                    CheckDoc(committed.GetValueOrDefault(op.Id), found, $"{op} (no transaction)");
                    if (tx != null)
                    {
                        var inTx = await db.Docs.FindByIdAsync(op.Id, tx);
                        CheckDoc(pending.GetValueOrDefault(op.Id), inTx, $"{op} (through the transaction)");
                    }
                    break;
                }
                case OpKind.Begin:
                    if (tx != null) break;
                    tx = await db.BeginTransactionAsync();
                    pending = committed.ToDictionary(kv => kv.Key, kv => Clone(kv.Value));
                    break;
                case OpKind.Commit:
                    if (tx == null) break;
                    await tx.CommitAsync();
                    Check(tx.State == TransactionState.Committed, $"{op}: transaction state is {tx.State} after CommitAsync");
                    tx.Dispose(); tx = null;
                    committed = pending;
                    break;
                case OpKind.Rollback:
                    if (tx == null) break;
                    await tx.RollbackAsync();
                    tx.Dispose(); tx = null;
                    break;
                case OpKind.Checkpoint:
                    await db.CheckpointAsync();
                    break;
                case OpKind.Backup:
                {
                    DeleteDatabase(backupPath);
                    await db.Storage.BackupAsync(backupPath);
                    using var copy = new StoreModelDbContext(backupPath, config);
                    var contents = await ReadAll(copy.Docs.FindAllAsync());
                    CheckSame(committed, contents, "the backup");
                    break;
                }
                case OpKind.Reopen:
                    // An open transaction dies with the context: nothing of it may survive.
                    tx?.Dispose(); tx = null;
                    db.Dispose();
                    db = new StoreModelDbContext(path, config);
                    break;
            }
        }

        // A duplicate id is rejected up front with BLiteValidationException: nothing is written and
        // a caller-owned transaction stays active.
        Task ExpectDuplicateKey(Func<Task> write, Op op) => ExpectRejected(write, op, "the duplicate id");

        async Task ExpectRejected(Func<Task> write, Op op, string why)
        {
            var stateBefore = tx?.State;
            try
            {
                await write();
                Fail($"{op} should have thrown BLiteValidationException for {why}");
            }
            catch (BLiteValidationException) { }
            catch (ModelMismatchException) { throw; }
            catch (Exception ex) { Fail($"{op} threw {ex.GetType().Name} instead of BLiteValidationException for {why}: {ex.Message}"); }
            if (tx != null)
                Check(tx.State == TransactionState.Active, $"{op}: a BLiteValidationException must leave the transaction active, state is {tx.State} (was {stateBefore})");
        }

        // A unique index violation is checked before any page is written and reported like a
        // duplicate id: BLiteValidationException, and a caller-owned transaction stays active.
        Task ExpectUniqueViolation(Func<Task> write, Op op) => ExpectRejected(write, op, "the duplicate unique code");

        async Task Verify(string when)
        {
            var scanned = await ReadAll(db.Docs.FindAllAsync());
            CheckSame(committed, scanned, $"{when}: FindAllAsync (committed view)");

            var count = await db.Docs.CountAsync();
            Check(count == committed.Count, $"{when}: CountAsync returned {count}, the model has {committed.Count} committed documents");

            if (tx != null)
            {
                var inTx = await ReadAll(db.Docs.FindAllAsync(tx));
                CheckSame(pending, inTx, $"{when}: FindAllAsync(tx) (pending view)");
            }

            // The secondary indexes, through the LINQ provider (no transaction: committed view).
            IQueryable<StoreDoc> q = db.Docs.AsQueryable();
            for (int k = 0; k < 10; k += 3)
            {
                var kk = k;
                var byCount = (await q.Where(d => d.Count == kk).ToListAsync()).Select(d => d.Id).OrderBy(i => i).ToList();
                var expected = committed.Values.Where(d => d.Count == kk).Select(d => d.Id).OrderBy(i => i).ToList();
                Check(byCount.SequenceEqual(expected), $"{when}: Where(Count == {kk}) returned [{string.Join(",", byCount)}], the model has [{string.Join(",", expected)}]");
            }
            var range = (await q.Where(d => d.Count >= 3 && d.Count < 7).ToListAsync()).Select(d => d.Id).OrderBy(i => i).ToList();
            var expectedRange = committed.Values.Where(d => d.Count >= 3 && d.Count < 7).Select(d => d.Id).OrderBy(i => i).ToList();
            Check(range.SequenceEqual(expectedRange), $"{when}: Where(3 <= Count < 7) returned [{string.Join(",", range)}], the model has [{string.Join(",", expectedRange)}]");

            var rangeCount = await q.CountAsync(d => d.Count >= 5);
            var expectedRangeCount = committed.Values.Count(d => d.Count >= 5);
            Check(rangeCount == expectedRangeCount, $"{when}: Count(Count >= 5) returned {rangeCount}, the model has {expectedRangeCount}");

            foreach (var code in committed.Values.Select(d => d.Code).Where(c => c != null).Distinct().Take(3))
            {
                var byCode = (await q.Where(d => d.Code == code).ToListAsync()).Select(d => d.Id).ToList();
                var expected = committed.Values.Where(d => d.Code == code).Select(d => d.Id).ToList();
                Check(byCode.SequenceEqual(expected), $"{when}: Where(Code == {code}) returned [{string.Join(",", byCode)}], the model has [{string.Join(",", expected)}]");
            }
        }
    }

    /// <summary>
    /// Shrinks by removing chunks of steps (half the sequence, then quarters, ... down to single
    /// steps), keeping a removal if the sequence still fails (any failure counts), until a pass
    /// over single steps removes nothing or the time is up. The last step is never removed.
    /// </summary>
    private static async Task<ShrinkResult> Shrink(List<Op> ops, bool multiProcess, TimeSpan budget)
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

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static StoreDoc Clone(StoreDoc d) => new() { Id = d.Id, Name = d.Name, Count = d.Count, Code = d.Code, Payload = d.Payload };

    private static async Task<Dictionary<int, StoreDoc>> ReadAll(IAsyncEnumerable<StoreDoc> docs)
    {
        var result = new Dictionary<int, StoreDoc>();
        await foreach (var d in docs)
        {
            if (!result.TryAdd(d.Id, d))
                Fail($"the scan returned id {d.Id} twice");
        }
        return result;
    }

    private static void CheckSame(Dictionary<int, StoreDoc> model, Dictionary<int, StoreDoc> actual, string what)
    {
        var missing = model.Keys.Where(k => !actual.ContainsKey(k)).OrderBy(k => k).ToList();
        var extra = actual.Keys.Where(k => !model.ContainsKey(k)).OrderBy(k => k).ToList();
        if (missing.Count > 0 || extra.Count > 0)
            Fail($"{what}: ids differ from the model; missing [{string.Join(",", missing)}], unexpected [{string.Join(",", extra)}] (model has {model.Count}, store returned {actual.Count})");
        foreach (var (id, expected) in model)
            CheckDoc(expected, actual[id], $"{what}: document {id}");
    }

    private static void CheckDoc(StoreDoc? expected, StoreDoc? actual, string what)
    {
        if (expected == null && actual == null) return;
        if (expected == null) Fail($"{what}: expected no document, got {Describe(actual)}");
        if (actual == null) Fail($"{what}: expected {Describe(expected)}, got null");
        if (expected!.Id != actual!.Id || expected.Name != actual.Name || expected.Count != actual.Count
            || expected.Code != actual.Code)
            Fail($"{what}: expected {Describe(expected)}, got {Describe(actual)}");
        if (expected.Payload != actual.Payload)
        {
            int i = 0;
            while (i < expected.Payload.Length && i < actual.Payload.Length && expected.Payload[i] == actual.Payload[i]) i++;
            Fail($"{what}: payload differs at index {i}: expected {Describe(expected)} with '{Snippet(expected.Payload, i)}', got {actual.Payload.Length} chars with '{Snippet(actual.Payload, i)}'");
        }
    }

    private static string Snippet(string s, int at) => at >= s.Length ? "<end>" : s.Substring(at, Math.Min(12, s.Length - at)).Replace('\0', '.');

    private static string Describe(StoreDoc? d)
        => d == null ? "null" : $"{{id={d.Id} name={d.Name} count={d.Count} code={d.Code ?? "null"} payload={d.Payload.Length} chars{(d.Payload.Length > 0 ? " of '" + d.Payload[0] + "'" : "")}}}";

    private static void Check(bool condition, string message)
    {
        if (!condition) Fail(message);
    }

    private static void Fail(string message) => throw new ModelMismatchException(message);

    private static void DeleteDatabase(string path)
    {
        foreach (var f in new[] { path, Path.ChangeExtension(path, ".wal"), path + "-shm", Path.ChangeExtension(path, ".wal-shm"), Path.ChangeExtension(path, ".manifest.json") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
        var collDir = path + "_coll";
        if (Directory.Exists(collDir))
            try { Directory.Delete(collDir, recursive: true); } catch { /* best-effort */ }
    }
}
