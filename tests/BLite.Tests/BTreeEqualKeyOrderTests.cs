using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Equal keys come back from a forward scan in insertion order (#256). The leaf insert used a
/// lower-bound search, which put each new copy of a key in front of the copies already there, so a
/// forward scan returned them newest first. Typed collections append the document id to every key
/// and never hold equal keys; a <see cref="DynamicCollection"/> index does.
/// </summary>
public sealed class BTreeEqualKeyOrderTests : IDisposable
{
    private readonly string _dir;

    public BTreeEqualKeyOrderTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_btree_ties_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    [Fact]
    public void Duplicates_Within_A_Leaf_Scan_Forward_In_Insertion_Order()
    {
        using var storage = new StorageEngine(Path.Combine(_dir, "leaf.db"), PageFileConfig.Default);
        var index = new BTreeIndex(storage, IndexOptions.CreateBTree("k"));
        var txn = storage.BeginTransaction().TransactionId;

        index.Insert(IndexKey.Create(10), new DocumentLocation(100, 0), txn);
        for (ushort i = 1; i <= 3; i++)
            index.Insert(IndexKey.Create(28), new DocumentLocation(1, i), txn);
        index.Insert(IndexKey.Create(40), new DocumentLocation(200, 0), txn);

        var forward = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn)
            .Select(e => (e.Key.As<int>(), e.Location.SlotIndex)).ToList();
        Assert.Equal(new[] { (10, (ushort)0), (28, (ushort)1), (28, (ushort)2), (28, (ushort)3), (40, (ushort)0) }, forward);

        var backward = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Backward, txn)
            .Select(e => (e.Key.As<int>(), e.Location.SlotIndex)).ToList();
        forward.Reverse();
        Assert.Equal(forward, backward);
    }

    [Fact]
    public void A_Run_Of_Duplicates_Across_Leaf_Splits_Scans_Forward_In_Insertion_Order()
    {
        using var storage = new StorageEngine(Path.Combine(_dir, "split.db"), PageFileConfig.Default);
        var index = new BTreeIndex(storage, IndexOptions.CreateBTree("k"));
        var txn = storage.BeginTransaction().TransactionId;

        // Enough copies of one key, between smaller and larger keys inserted in between, to
        // split leaves several times inside the run.
        const int copies = 2_000;
        for (uint i = 0; i < copies; i++)
        {
            index.Insert(IndexKey.Create(500), new DocumentLocation(i + 1, 0), txn);
            if (i % 100 == 0)
            {
                index.Insert(IndexKey.Create(100 + (int)i / 100), new DocumentLocation(0, 0), txn);
                index.Insert(IndexKey.Create(900 + (int)i / 100), new DocumentLocation(0, 0), txn);
            }
        }

        var run = index.Range(IndexKey.Create(500), IndexKey.Create(500), IndexDirection.Forward, txn)
            .Select(e => e.Location.PageId).ToList();
        Assert.Equal(Enumerable.Range(1, copies).Select(i => (uint)i), run);

        var runBackward = index.Range(IndexKey.Create(500), IndexKey.Create(500), IndexDirection.Backward, txn)
            .Select(e => e.Location.PageId).ToList();
        Assert.Equal(Enumerable.Range(1, copies).Reverse().Select(i => (uint)i), runBackward);
    }

    [Fact]
    public async Task DynamicCollection_QueryIndex_Returns_Ties_In_Insertion_Order_Ascending()
    {
        using var engine = new BLiteEngine(Path.Combine(_dir, "dyn.db"));
        var col = engine.GetOrCreateCollection("people");
        await col.CreateIndexAsync("age", "idx_age", unique: false);
        foreach (var n in new[] { "d1", "d2", "d3" })
            await col.InsertAsync(col.CreateDocument(["name", "age"], b => b.AddString("name", n).AddInt32("age", 5)));

        static string Name(BLite.Bson.BsonDocument d) => d.TryGetString("name", out var n) ? n : "?";

        var ascending = await col.QueryIndexAsync("idx_age", 5, 5).ToListAsync();
        Assert.Equal(new[] { "d1", "d2", "d3" }, ascending.Select(Name));

        var descending = await col.QueryIndexAsync("idx_age", 5, 5, false).ToListAsync();
        Assert.Equal(new[] { "d3", "d2", "d1" }, descending.Select(Name));
    }
}
