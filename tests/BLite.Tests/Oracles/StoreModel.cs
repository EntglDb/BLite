using BLite.Core;
using BLite.Core.Query;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;

namespace BLite.Tests.Oracles;

/// <summary>
/// The operations, the generator and the dictionary model of the model-based store test, shared by
/// <see cref="StoreModelTests"/> and <see cref="WalCrashTests"/>. The contract the model encodes is
/// described on <see cref="StoreModelTests"/>.
/// </summary>
internal static class StoreModel
{
    public const int IdPool = 120;
    private static readonly string[] s_codes = Enumerable.Range(0, 40).Select(i => $"c{i:00}").ToArray();
    private static readonly int[] s_payloadSizes = [0, 0, 20, 20, 300, 300, 2500, 2500, 18000];

    /// <summary>A seeded random sequence of <paramref name="steps"/> operations.</summary>
    public static List<StoreOp> Generate(int seed, int steps)
    {
        var rnd = new Random(seed);
        var ops = new List<StoreOp>(steps);
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
                ops.Add(new StoreOp(StoreOpKind.Insert, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
                live.Add(d.Id);
            }
            else if (roll < 36)
            {
                var d = Doc(ExistingOrAny());
                ops.Add(new StoreOp(StoreOpKind.Update, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
            }
            else if (roll < 48)
            {
                var id = ExistingOrAny();
                ops.Add(new StoreOp(StoreOpKind.Delete, id));
                live.Remove(id);
            }
            else if (roll < 56)
            {
                var d = Doc(ExistingOrAny());
                ops.Add(new StoreOp(StoreOpKind.Upsert, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
                live.Add(d.Id);
            }
            else if (roll < 60)
            {
                var docs = Enumerable.Range(0, rnd.Next(2, 9)).Select(_ => Doc(rnd.Next(1, IdPool + 1))).DistinctBy(d => d.Id).ToArray();
                ops.Add(new StoreOp(StoreOpKind.InsertBulk, Docs: docs));
                foreach (var d in docs) live.Add(d.Id);
            }
            else if (roll < 63)
            {
                var ids = Enumerable.Range(0, rnd.Next(2, 9)).Select(_ => ExistingOrAny()).Distinct().ToArray();
                ops.Add(new StoreOp(StoreOpKind.DeleteBulk, Ids: ids));
                foreach (var id in ids) live.Remove(id);
            }
            else if (roll < 71) ops.Add(new StoreOp(StoreOpKind.FindById, ExistingOrAny()));
            else if (roll < 78) ops.Add(new StoreOp(StoreOpKind.Begin));
            else if (roll < 86) ops.Add(new StoreOp(StoreOpKind.Commit));
            else if (roll < 90) ops.Add(new StoreOp(StoreOpKind.Rollback));
            else if (roll < 94) ops.Add(new StoreOp(StoreOpKind.Checkpoint));
            else if (roll < 97) ops.Add(new StoreOp(StoreOpKind.Backup));
            else ops.Add(new StoreOp(StoreOpKind.Reopen));
        }
        return ops;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    public static StoreDoc Clone(StoreDoc d) => new() { Id = d.Id, Name = d.Name, Count = d.Count, Code = d.Code, Payload = d.Payload };

    public static async Task<Dictionary<int, StoreDoc>> ReadAll(IAsyncEnumerable<StoreDoc> docs)
    {
        var result = new Dictionary<int, StoreDoc>();
        await foreach (var d in docs)
        {
            if (!result.TryAdd(d.Id, d))
                Fail($"the scan returned id {d.Id} twice");
        }
        return result;
    }

    public static void CheckSame(Dictionary<int, StoreDoc> model, Dictionary<int, StoreDoc> actual, string what)
    {
        var missing = model.Keys.Where(k => !actual.ContainsKey(k)).OrderBy(k => k).ToList();
        var extra = actual.Keys.Where(k => !model.ContainsKey(k)).OrderBy(k => k).ToList();
        if (missing.Count > 0 || extra.Count > 0)
            Fail($"{what}: ids differ from the model; missing [{string.Join(",", missing)}], unexpected [{string.Join(",", extra)}] (model has {model.Count}, store returned {actual.Count})");
        foreach (var (id, expected) in model)
            CheckDoc(expected, actual[id], $"{what}: document {id}");
    }

    /// <summary>Whether two document sets are equal, by the comparison of <see cref="CheckSame"/>.</summary>
    public static bool Same(Dictionary<int, StoreDoc> a, Dictionary<int, StoreDoc> b)
        => a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var o) && SameDoc(kv.Value, o));

    private static bool SameDoc(StoreDoc a, StoreDoc b)
        => a.Id == b.Id && a.Name == b.Name && a.Count == b.Count && a.Code == b.Code && a.Payload == b.Payload;

    public static void CheckDoc(StoreDoc? expected, StoreDoc? actual, string what)
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

    public static string Describe(StoreDoc? d)
        => d == null ? "null" : $"{{id={d.Id} name={d.Name} count={d.Count} code={d.Code ?? "null"} payload={d.Payload.Length} chars{(d.Payload.Length > 0 ? " of '" + d.Payload[0] + "'" : "")}}}";

    public static void Check(bool condition, string message)
    {
        if (!condition) Fail(message);
    }

    public static void Fail(string message) => throw new StoreModelMismatchException(message);

    public static void DeleteDatabase(string path)
    {
        foreach (var f in new[] { path, Path.ChangeExtension(path, ".wal"), path + "-shm", Path.ChangeExtension(path, ".wal-shm"), Path.ChangeExtension(path, ".manifest.json") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
        var collDir = path + "_coll";
        if (Directory.Exists(collDir))
            try { Directory.Delete(collDir, recursive: true); } catch { /* best-effort */ }
    }
}

internal enum StoreOpKind { Insert, Update, Delete, Upsert, InsertBulk, DeleteBulk, FindById, Begin, Commit, Rollback, Checkpoint, Backup, Reopen }

/// <summary>One step. Only inputs are recorded; what the step must do follows from the model at replay.</summary>
internal sealed record StoreOp(StoreOpKind Kind, int Id = 0, int Count = 0, string? Code = null, int PayloadLength = 0, int[]? Ids = null, StoreDoc[]? Docs = null)
{
    public override string ToString() => Kind switch
    {
        StoreOpKind.Insert or StoreOpKind.Update or StoreOpKind.Upsert => $"{Kind}(id={Id}, count={Count}, code={Code ?? "null"}, payload={PayloadLength} chars)",
        StoreOpKind.InsertBulk => $"InsertBulk({string.Join(", ", Docs!.Select(d => $"id={d.Id}/count={d.Count}/code={d.Code ?? "null"}/payload={d.Payload.Length}"))})",
        StoreOpKind.DeleteBulk => $"DeleteBulk(ids={string.Join(",", Ids!)})",
        StoreOpKind.FindById or StoreOpKind.Delete => $"{Kind}(id={Id})",
        _ => Kind.ToString(),
    };
}

internal sealed class StoreModelMismatchException : Exception
{
    public StoreModelMismatchException(string message) : base(message) { }
}

/// <summary>
/// A <see cref="StoreModelDbContext"/> and the dictionary model it is checked against: the committed
/// view, and the pending view of the open transaction. <see cref="ApplyAsync"/> runs one operation on
/// both, <see cref="VerifyAsync"/> compares them; a disagreement throws <see cref="StoreModelMismatchException"/>.
/// </summary>
internal sealed class StoreModelRunner : IDisposable
{
    private readonly string _backupPath;
    private readonly PageFileConfig _config;
    private readonly Func<StoreModelDbContext> _open;
    private readonly Func<Func<Task>, Task> _aroundBackup;
    private Dictionary<int, StoreDoc> _committed;
    private Dictionary<int, StoreDoc> _pending = new();
    private ITransaction? _tx;
    private StoreModelDbContext _db;

    /// <param name="open">Opens the database; called now and by every <see cref="StoreOpKind.Reopen"/>.</param>
    /// <param name="committed">What the database already holds; empty for a new one.</param>
    /// <param name="aroundBackup">Runs the backup of the open database (default: just runs it).</param>
    public StoreModelRunner(Func<StoreModelDbContext> open, string backupPath, PageFileConfig config,
        Dictionary<int, StoreDoc>? committed = null, Func<Func<Task>, Task>? aroundBackup = null)
    {
        _open = open;
        _backupPath = backupPath;
        _config = config;
        _committed = committed ?? new Dictionary<int, StoreDoc>();
        _aroundBackup = aroundBackup ?? (backup => backup());
        _db = open();
    }

    public StoreModelRunner(string path, string backupPath, PageFileConfig config)
        : this(() => new StoreModelDbContext(path, config), backupPath, config) { }

    /// <summary>The committed view of the model.</summary>
    public Dictionary<int, StoreDoc> Committed => _committed;

    public StoreModelDbContext Db => _db;

    public void Dispose()
    {
        _tx?.Dispose();
        _db.Dispose();
    }

    // The view a write goes to and is read back from: the open transaction's, else the committed one.
    private Dictionary<int, StoreDoc> View() => _tx != null ? _pending : _committed;

    private bool CodeTaken(StoreDoc d) => d.Code != null && View().Values.Any(o => o.Id != d.Id && o.Code == d.Code);

    public async Task ApplyAsync(StoreOp op)
    {
        var db = _db;
        switch (op.Kind)
        {
            case StoreOpKind.Insert:
            {
                var d = StoreModel.Clone(op.Docs![0]);
                if (View().ContainsKey(d.Id))
                {
                    await ExpectDuplicateKey(() => db.Docs.InsertAsync(d, _tx).AsTask(), op);
                }
                else if (CodeTaken(d))
                {
                    await ExpectUniqueViolation(() => db.Docs.InsertAsync(d, _tx).AsTask(), op);
                }
                else
                {
                    var id = await db.Docs.InsertAsync(d, _tx);
                    StoreModel.Check(id == d.Id, $"{op} returned id {id}");
                    View()[d.Id] = StoreModel.Clone(d);
                }
                break;
            }
            case StoreOpKind.Update:
            {
                var d = StoreModel.Clone(op.Docs![0]);
                if (!View().ContainsKey(d.Id))
                {
                    var updated = await db.Docs.UpdateAsync(d, _tx);
                    StoreModel.Check(!updated, $"{op} returned true for a document that does not exist");
                }
                else if (CodeTaken(d))
                {
                    await ExpectUniqueViolation(() => db.Docs.UpdateAsync(d, _tx).AsTask(), op);
                }
                else
                {
                    var updated = await db.Docs.UpdateAsync(d, _tx);
                    StoreModel.Check(updated, $"{op} returned false for an existing document");
                    View()[d.Id] = StoreModel.Clone(d);
                }
                break;
            }
            case StoreOpKind.Delete:
            {
                var deleted = await db.Docs.DeleteAsync(op.Id, _tx);
                var expected = View().Remove(op.Id);
                StoreModel.Check(deleted == expected, $"{op} returned {deleted}, expected {expected}");
                break;
            }
            case StoreOpKind.Upsert:
            {
                var d = StoreModel.Clone(op.Docs![0]);
                if (CodeTaken(d))
                {
                    await ExpectUniqueViolation(() => db.Docs.UpsertAsync(d, _tx).AsTask(), op);
                }
                else
                {
                    var result = await db.Docs.UpsertAsync(d, _tx);
                    var expectedInserted = !View().ContainsKey(d.Id);
                    StoreModel.Check(result.Inserted == expectedInserted, $"{op} reported Inserted={result.Inserted}, expected {expectedInserted}");
                    StoreModel.Check(result.Id == d.Id, $"{op} returned id {result.Id}");
                    View()[d.Id] = StoreModel.Clone(d);
                }
                break;
            }
            case StoreOpKind.InsertBulk:
            {
                // Only documents the model accepts: free ids and codes that are free among them too.
                var batch = new List<StoreDoc>();
                foreach (var d in op.Docs!)
                    if (!View().ContainsKey(d.Id) && !CodeTaken(d) && (d.Code == null || batch.All(b => b.Code != d.Code)))
                        batch.Add(StoreModel.Clone(d));
                if (batch.Count == 0) break;
                var ids = await db.Docs.InsertBulkAsync(batch, _tx);
                StoreModel.Check(ids.SequenceEqual(batch.Select(b => b.Id)), $"{op} returned ids [{string.Join(",", ids)}]");
                foreach (var d in batch) View()[d.Id] = StoreModel.Clone(d);
                break;
            }
            case StoreOpKind.DeleteBulk:
            {
                var deleted = await db.Docs.DeleteBulkAsync(op.Ids!, _tx);
                int expected = 0;
                foreach (var id in op.Ids!) if (View().Remove(id)) expected++;
                StoreModel.Check(deleted == expected, $"{op} returned {deleted}, expected {expected}");
                break;
            }
            case StoreOpKind.FindById:
            {
                var found = await db.Docs.FindByIdAsync(op.Id);
                StoreModel.CheckDoc(_committed.GetValueOrDefault(op.Id), found, $"{op} (no transaction)");
                if (_tx != null)
                {
                    var inTx = await db.Docs.FindByIdAsync(op.Id, _tx);
                    StoreModel.CheckDoc(_pending.GetValueOrDefault(op.Id), inTx, $"{op} (through the transaction)");
                }
                break;
            }
            case StoreOpKind.Begin:
                if (_tx != null) break;
                _tx = await db.BeginTransactionAsync();
                _pending = _committed.ToDictionary(kv => kv.Key, kv => StoreModel.Clone(kv.Value));
                break;
            case StoreOpKind.Commit:
                if (_tx == null) break;
                await _tx.CommitAsync();
                StoreModel.Check(_tx.State == TransactionState.Committed, $"{op}: transaction state is {_tx.State} after CommitAsync");
                _tx.Dispose(); _tx = null;
                _committed = _pending;
                break;
            case StoreOpKind.Rollback:
                if (_tx == null) break;
                await _tx.RollbackAsync();
                _tx.Dispose(); _tx = null;
                break;
            case StoreOpKind.Checkpoint:
                await db.CheckpointAsync();
                break;
            case StoreOpKind.Backup:
            {
                StoreModel.DeleteDatabase(_backupPath);
                await _aroundBackup(() => db.Storage.BackupAsync(_backupPath));
                using var copy = new StoreModelDbContext(_backupPath, _config);
                var contents = await StoreModel.ReadAll(copy.Docs.FindAllAsync());
                StoreModel.CheckSame(_committed, contents, "the backup");
                break;
            }
            case StoreOpKind.Reopen:
                // An open transaction dies with the context: nothing of it may survive.
                _tx?.Dispose(); _tx = null;
                _db.Dispose();
                _db = _open();
                break;
        }
    }

    // A duplicate id is rejected up front with BLiteValidationException: nothing is written and
    // a caller-owned transaction stays active.
    private Task ExpectDuplicateKey(Func<Task> write, StoreOp op) => ExpectRejected(write, op, "the duplicate id");

    private async Task ExpectRejected(Func<Task> write, StoreOp op, string why)
    {
        var stateBefore = _tx?.State;
        try
        {
            await write();
            StoreModel.Fail($"{op} should have thrown BLiteValidationException for {why}");
        }
        catch (BLiteValidationException) { }
        catch (StoreModelMismatchException) { throw; }
        catch (Exception ex) { StoreModel.Fail($"{op} threw {ex.GetType().Name} instead of BLiteValidationException for {why}: {ex.Message}"); }
        if (_tx != null)
            StoreModel.Check(_tx.State == TransactionState.Active, $"{op}: a BLiteValidationException must leave the transaction active, state is {_tx.State} (was {stateBefore})");
    }

    // A unique index violation is checked before any page is written and reported like a
    // duplicate id: BLiteValidationException, and a caller-owned transaction stays active.
    private Task ExpectUniqueViolation(Func<Task> write, StoreOp op) => ExpectRejected(write, op, "the duplicate unique code");

    public async Task VerifyAsync(string when)
    {
        var db = _db;
        var committed = _committed;
        var scanned = await StoreModel.ReadAll(db.Docs.FindAllAsync());
        StoreModel.CheckSame(committed, scanned, $"{when}: FindAllAsync (committed view)");

        var count = await db.Docs.CountAsync();
        StoreModel.Check(count == committed.Count, $"{when}: CountAsync returned {count}, the model has {committed.Count} committed documents");

        if (_tx != null)
        {
            var inTx = await StoreModel.ReadAll(db.Docs.FindAllAsync(_tx));
            StoreModel.CheckSame(_pending, inTx, $"{when}: FindAllAsync(tx) (pending view)");
        }

        // The secondary indexes, through the LINQ provider (no transaction: committed view).
        IQueryable<StoreDoc> q = db.Docs.AsQueryable();
        for (int k = 0; k < 10; k += 3)
        {
            var kk = k;
            var byCount = (await q.Where(d => d.Count == kk).ToListAsync()).Select(d => d.Id).OrderBy(i => i).ToList();
            var expected = committed.Values.Where(d => d.Count == kk).Select(d => d.Id).OrderBy(i => i).ToList();
            StoreModel.Check(byCount.SequenceEqual(expected), $"{when}: Where(Count == {kk}) returned [{string.Join(",", byCount)}], the model has [{string.Join(",", expected)}]");
        }
        var range = (await q.Where(d => d.Count >= 3 && d.Count < 7).ToListAsync()).Select(d => d.Id).OrderBy(i => i).ToList();
        var expectedRange = committed.Values.Where(d => d.Count >= 3 && d.Count < 7).Select(d => d.Id).OrderBy(i => i).ToList();
        StoreModel.Check(range.SequenceEqual(expectedRange), $"{when}: Where(3 <= Count < 7) returned [{string.Join(",", range)}], the model has [{string.Join(",", expectedRange)}]");

        var rangeCount = await q.CountAsync(d => d.Count >= 5);
        var expectedRangeCount = committed.Values.Count(d => d.Count >= 5);
        StoreModel.Check(rangeCount == expectedRangeCount, $"{when}: Count(Count >= 5) returned {rangeCount}, the model has {expectedRangeCount}");

        foreach (var code in committed.Values.Select(d => d.Code).Where(c => c != null).Distinct().Take(3))
        {
            var byCode = (await q.Where(d => d.Code == code).ToListAsync()).Select(d => d.Id).ToList();
            var expected = committed.Values.Where(d => d.Code == code).Select(d => d.Id).ToList();
            StoreModel.Check(byCode.SequenceEqual(expected), $"{when}: Where(Code == {code}) returned [{string.Join(",", byCode)}], the model has [{string.Join(",", expected)}]");
        }
    }
}
