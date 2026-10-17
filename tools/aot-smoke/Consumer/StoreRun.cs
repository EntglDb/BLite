using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace AotSmoke;

/// <summary>
/// Runs a generated store sequence against <see cref="AotDbContext"/> and the dictionary model of
/// StoreModelRunner (tests/BLite.Tests/Oracles/StoreModel.cs), with the same expectations, but
/// verifies through the AOT-safe APIs only: FindAllAsync, CountAsync, FindByIdAsync,
/// QueryIndexAsync over the model-declared indexes and ScanAsync with a static delegate. Every
/// step's outcome goes to the dump, so the JIT and the AOT build are compared step by step.
/// </summary>
internal sealed class StoreRun : IDisposable
{
    private readonly Func<AotDbContext> _open;
    private readonly Func<string, AotDbContext> _openCopy;
    private readonly string _backupPath;
    private readonly Dump _dump;
    private Dictionary<int, StoreDoc> _committed = new();
    private Dictionary<int, StoreDoc> _pending = new();
    private ITransaction? _tx;
    private AotDbContext _db;

    public StoreRun(Func<AotDbContext> open, Func<string, AotDbContext> openCopy, string backupPath, Dump dump)
    {
        _open = open;
        _openCopy = openCopy;
        _backupPath = backupPath;
        _dump = dump;
        _db = open();
    }

    public AotDbContext Db => _db;
    public Dictionary<int, StoreDoc> Committed => _committed;

    public void Dispose()
    {
        _tx?.Dispose();
        _db.Dispose();
    }

    public async Task RunAsync(List<StoreOp> ops)
    {
        for (int i = 0; i < ops.Count; i++)
        {
            var outcome = await ApplyAsync(ops[i]);
            _dump.Line($"{i + 1,4} {ops[i]} -> {outcome}");
            await VerifyAsync($"after step {i + 1} ({ops[i]})");
        }
        if (_tx != null)
        {
            await ApplyAsync(new StoreOp(StoreOpKind.Commit));
            _dump.Line("   - final Commit");
        }
    }

    private Dictionary<int, StoreDoc> View() => _tx != null ? _pending : _committed;

    private bool CodeTaken(StoreDoc d) => d.Code != null && View().Values.Any(o => o.Id != d.Id && o.Code == d.Code);

    private async Task<string> ApplyAsync(StoreOp op)
    {
        var db = _db;
        switch (op.Kind)
        {
            case StoreOpKind.Insert:
            {
                var d = StoreModel.Clone(op.Docs![0]);
                if (View().ContainsKey(d.Id) || CodeTaken(d))
                    return await ExpectRejected(() => db.Docs.InsertAsync(d, _tx).AsTask(), op);
                var id = await db.Docs.InsertAsync(d, _tx);
                Check(id == d.Id, $"{op} returned id {id}");
                View()[d.Id] = StoreModel.Clone(d);
                return $"inserted {id}";
            }
            case StoreOpKind.Update:
            {
                var d = StoreModel.Clone(op.Docs![0]);
                if (!View().ContainsKey(d.Id))
                {
                    var missing = await db.Docs.UpdateAsync(d, _tx);
                    Check(!missing, $"{op} returned true for a document that does not exist");
                    return "not found";
                }
                if (CodeTaken(d))
                    return await ExpectRejected(() => db.Docs.UpdateAsync(d, _tx).AsTask(), op);
                var updated = await db.Docs.UpdateAsync(d, _tx);
                Check(updated, $"{op} returned false for an existing document");
                View()[d.Id] = StoreModel.Clone(d);
                return "updated";
            }
            case StoreOpKind.Delete:
            {
                var deleted = await db.Docs.DeleteAsync(op.Id, _tx);
                var expected = View().Remove(op.Id);
                Check(deleted == expected, $"{op} returned {deleted}, expected {expected}");
                return deleted ? "deleted" : "not found";
            }
            case StoreOpKind.Upsert:
            {
                var d = StoreModel.Clone(op.Docs![0]);
                if (CodeTaken(d))
                    return await ExpectRejected(() => db.Docs.UpsertAsync(d, _tx).AsTask(), op);
                var result = await db.Docs.UpsertAsync(d, _tx);
                var expectedInserted = !View().ContainsKey(d.Id);
                Check(result.Inserted == expectedInserted, $"{op} reported Inserted={result.Inserted}, expected {expectedInserted}");
                Check(result.Id == d.Id, $"{op} returned id {result.Id}");
                View()[d.Id] = StoreModel.Clone(d);
                return result.Inserted ? "upsert inserted" : "upsert updated";
            }
            case StoreOpKind.InsertBulk:
            {
                var batch = new List<StoreDoc>();
                foreach (var d in op.Docs!)
                    if (!View().ContainsKey(d.Id) && !CodeTaken(d) && (d.Code == null || batch.All(b => b.Code != d.Code)))
                        batch.Add(StoreModel.Clone(d));
                if (batch.Count == 0) return "nothing to insert";
                var ids = await db.Docs.InsertBulkAsync(batch, _tx);
                Check(ids.SequenceEqual(batch.Select(b => b.Id)), $"{op} returned ids [{string.Join(",", ids)}]");
                foreach (var d in batch) View()[d.Id] = StoreModel.Clone(d);
                return $"inserted [{string.Join(",", ids)}]";
            }
            case StoreOpKind.DeleteBulk:
            {
                var deleted = await db.Docs.DeleteBulkAsync(op.Ids!, _tx);
                int expected = 0;
                foreach (var id in op.Ids!) if (View().Remove(id)) expected++;
                Check(deleted == expected, $"{op} returned {deleted}, expected {expected}");
                return $"deleted {deleted}";
            }
            case StoreOpKind.FindById:
            {
                var found = await db.Docs.FindByIdAsync(op.Id);
                CheckDoc(_committed.GetValueOrDefault(op.Id), found, $"{op} (no transaction)");
                if (_tx == null) return found == null ? "null" : Dump.Describe(found);
                var inTx = await db.Docs.FindByIdAsync(op.Id, _tx);
                CheckDoc(_pending.GetValueOrDefault(op.Id), inTx, $"{op} (through the transaction)");
                return $"{(found == null ? "null" : Dump.Describe(found))} / tx {(inTx == null ? "null" : Dump.Describe(inTx))}";
            }
            case StoreOpKind.Begin:
                if (_tx != null) return "already open";
                _tx = await db.BeginTransactionAsync();
                _pending = _committed.ToDictionary(kv => kv.Key, kv => StoreModel.Clone(kv.Value));
                return "begun";
            case StoreOpKind.Commit:
                if (_tx == null) return "no transaction";
                await _tx.CommitAsync();
                Check(_tx.State == TransactionState.Committed, $"{op}: transaction state is {_tx.State} after CommitAsync");
                _tx.Dispose(); _tx = null;
                _committed = _pending;
                return "committed";
            case StoreOpKind.Rollback:
                if (_tx == null) return "no transaction";
                await _tx.RollbackAsync();
                _tx.Dispose(); _tx = null;
                return "rolled back";
            case StoreOpKind.Checkpoint:
                await db.CheckpointAsync();
                return "checkpointed";
            case StoreOpKind.Backup:
            {
                Program.DeleteDatabase(_backupPath);
                await db.Engine.BackupAsync(_backupPath);
                using var copy = _openCopy(_backupPath);
                var contents = await ReadAll(copy.Docs.FindAllAsync());
                CheckSame(_committed, contents, "the backup");
                return $"backup holds {contents.Count}";
            }
            case StoreOpKind.Reopen:
                _tx?.Dispose(); _tx = null;
                _db.Dispose();
                _db = _open();
                return "reopened";
        }
        throw new InvalidOperationException(op.Kind.ToString());
    }

    // A duplicate id or a taken unique code is rejected with BLiteValidationException before
    // anything is written, and a caller-owned transaction stays active.
    private async Task<string> ExpectRejected(Func<Task> write, StoreOp op)
    {
        try
        {
            await write();
        }
        catch (BLiteValidationException)
        {
            if (_tx != null)
                Check(_tx.State == TransactionState.Active, $"{op}: a BLiteValidationException must leave the transaction active, state is {_tx.State}");
            return "rejected";
        }
        throw new SmokeMismatchException($"{op} should have thrown BLiteValidationException");
    }

    private async Task VerifyAsync(string when)
    {
        var db = _db;
        var scanned = await ReadAll(db.Docs.FindAllAsync());
        CheckSame(_committed, scanned, $"{when}: FindAllAsync");
        var count = await db.Docs.CountAsync();
        Check(count == _committed.Count, $"{when}: CountAsync returned {count}, the model has {_committed.Count}");
        if (_tx != null)
            CheckSame(_pending, await ReadAll(db.Docs.FindAllAsync(_tx)), $"{when}: FindAllAsync(tx)");

        // The model-declared indexes, through QueryIndexAsync (inclusive bounds).
        for (int k = 0; k < 10; k += 3)
            await CheckIds(db.Docs.QueryIndexAsync(Program.CountIndex, k, k), d => d.Count == k, $"{when}: {Program.CountIndex} [{k},{k}]");
        await CheckIds(db.Docs.QueryIndexAsync(Program.CountIndex, 3, 6), d => d.Count >= 3 && d.Count <= 6, $"{when}: {Program.CountIndex} [3,6]");
        foreach (var code in _committed.Values.Select(d => d.Code).Where(c => c != null).Distinct().Take(3))
            await CheckIds(db.Docs.QueryIndexAsync(Program.CodeIndex, code, code), d => d.Code == code, $"{when}: {Program.CodeIndex} [{code},{code}]");

        await CheckIds(db.Docs.ScanAsync((BsonReaderPredicate)Program.CountAtLeastFive), d => d.Count >= 5, $"{when}: ScanAsync(count >= 5)");
    }

    private async Task CheckIds(IAsyncEnumerable<StoreDoc> docs, Func<StoreDoc, bool> expected, string what)
    {
        var actual = new List<int>();
        await foreach (var d in docs) actual.Add(d.Id);
        actual.Sort();
        var want = _committed.Values.Where(expected).Select(d => d.Id).OrderBy(i => i).ToList();
        Check(actual.SequenceEqual(want), $"{what} returned [{string.Join(",", actual)}], the model has [{string.Join(",", want)}]");
    }

    public static async Task<Dictionary<int, StoreDoc>> ReadAll(IAsyncEnumerable<StoreDoc> docs)
    {
        var result = new Dictionary<int, StoreDoc>();
        await foreach (var d in docs)
            Check(result.TryAdd(d.Id, d), $"the scan returned id {d.Id} twice");
        return result;
    }

    public static void CheckSame(Dictionary<int, StoreDoc> model, Dictionary<int, StoreDoc> actual, string what)
    {
        var missing = model.Keys.Where(k => !actual.ContainsKey(k)).OrderBy(k => k).ToList();
        var extra = actual.Keys.Where(k => !model.ContainsKey(k)).OrderBy(k => k).ToList();
        Check(missing.Count == 0 && extra.Count == 0,
            $"{what}: ids differ from the model; missing [{string.Join(",", missing)}], unexpected [{string.Join(",", extra)}]");
        foreach (var (id, doc) in model)
            CheckDoc(doc, actual[id], $"{what}: document {id}");
    }

    private static void CheckDoc(StoreDoc? expected, StoreDoc? actual, string what)
    {
        if (expected == null && actual == null) return;
        Check(expected != null && actual != null && Dump.Describe(expected) == Dump.Describe(actual) && expected.Payload == actual.Payload,
            $"{what}: expected {(expected == null ? "null" : Dump.Describe(expected))}, got {(actual == null ? "null" : Dump.Describe(actual))}");
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new SmokeMismatchException(message);
    }
}

internal sealed class SmokeMismatchException(string message) : Exception(message);
