using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Core.Storage;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for B-Tree node splits and descent:
/// <list type="bullet">
///   <item>#217: nodes split by entry count only, so variable-length keys filled a node page
///   before it reached <see cref="BTreeIndex.MaxEntriesPerNode"/> entries and the next insert
///   overflowed the page.</item>
///   <item>#213: a leaf split promotes the first key of the right half, and descent went right
///   when the search key equalled that separator, so a forward range scan or a delete started
///   past duplicates of the key left behind in the left leaf.</item>
/// </list>
/// </summary>
public class BTreeSplitTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"btree_split_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            if (File.Exists(f)) File.Delete(f);
    }

    private static string LongName(int i, int padding) => $"{i:D5}{new string('x', padding)}";

    // ── #217: split by size ─────────────────────────────────────────────────

    [Theory]
    [InlineData(100)]
    [InlineData(400)]
    [InlineData(2000)]
    public async Task Dynamic_LongStringKeys_AllInsertsSucceed_AndAreFound(int padding)
    {
        const int count = 200;
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("people");
        await col.CreateIndexAsync("name", "idx", unique: false);

        for (int i = 0; i < count; i++)
        {
            var name = LongName(i, padding);
            await col.InsertAsync(col.CreateDocument(["name"], b => b.AddString("name", name)));
        }

        var all = await col.QueryIndexAsync("idx", null, null).ToListAsync();
        Assert.Equal(count, all.Count);
        for (int i = 0; i < count; i += 7)
        {
            var name = LongName(i, padding);
            Assert.Single(await col.QueryIndexAsync("idx", name, name).ToListAsync());
            Assert.Single(await col.QueryIndexAsync("idx", name, name, false).ToListAsync());
        }
    }

    [Theory]
    [InlineData(400)]
    [InlineData(2000)]
    public async Task Typed_LongStringKeys_AllInsertsSucceed_AndAreFound(int padding)
    {
        const int count = 120;
        using (var db = new TestDbContext(_dbPath))
        {
            await db.PeopleV2.CreateIndexAsync(p => p.Name, "idx_name", unique: false);
            for (int i = 0; i < count; i++)
                await db.PeopleV2.InsertAsync(new PersonV2 { Name = LongName(i, padding), Age = i });

            for (int i = 0; i < count; i += 11)
            {
                var name = LongName(i, padding);
                var hits = await db.PeopleV2.AsQueryable().Where(p => p.Name == name).ToListAsync();
                Assert.Single(hits);
                Assert.Equal(i, hits[0].Age);
            }
        }

        using var reopened = new TestDbContext(_dbPath);
        Assert.Equal(count, await reopened.PeopleV2.CountAsync());
        var last = LongName(count - 1, padding);
        Assert.Single(await reopened.PeopleV2.AsQueryable().Where(p => p.Name == last).ToListAsync());
    }

    [Fact]
    public async Task Dynamic_KeyAboveMaxKeySize_IsRejectedUpFront_WithTheLimitInTheMessage()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("people");
        await col.CreateIndexAsync("name", "idx", unique: false);
        await col.InsertAsync(col.CreateDocument(["name"], b => b.AddString("name", "small")));

        // A 9000-character key cannot share a 16 KB node with a sibling entry.
        var ex = await Assert.ThrowsAsync<BLiteValidationException>(() =>
            col.InsertAsync(col.CreateDocument(["name"], b => b.AddString("name", new string('y', 9000)))).AsTask());
        Assert.Contains("too large for index 'idx'", ex.Message);

        Assert.Single(await col.QueryIndexAsync("idx", null, null).ToListAsync());
    }

    [Fact]
    public void BTree_KeyAboveMaxKeySize_ThrowsArgumentException_BeforeWriting()
    {
        using var storage = new StorageEngine(_dbPath, PageFileConfig.Default);
        var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"));
        var txn = storage.BeginTransaction().TransactionId;

        var tooLong = IndexKey.Create(new string('z', index.MaxKeySize + 1));
        Assert.True(tooLong.Data.Length > index.MaxKeySize);
        var ex = Assert.Throws<ArgumentException>(() => index.Insert(tooLong, new DocumentLocation(1, 0), txn));
        Assert.Contains(index.MaxKeySize.ToString(), ex.Message);

        Assert.Empty(index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn));
    }

    [Fact]
    public void BTree_KeysAtMaxKeySize_MixedWithSmallKeys_SplitIntoNodesThatFit()
    {
        // Very uneven entry sizes: a split by entry count would put several maximum-size keys
        // in one half; the split must be chosen by bytes.
        using var storage = new StorageEngine(_dbPath, PageFileConfig.Small);
        var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"));
        var txn = storage.BeginTransaction().TransactionId;

        var expected = new List<(IndexKey Key, DocumentLocation Loc)>();
        ushort slot = 0;
        for (int i = 0; i < 300; i++)
        {
            var key = i % 5 == 0 ? KeyOfSize(index.MaxKeySize, (char)('a' + i % 26), i) : IndexKey.Create($"{i:D4}");
            var loc = new DocumentLocation((uint)(1 + i / 100), slot++);
            index.Insert(key, loc, txn);
            expected.Add((key, loc));
        }

        var actual = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn).ToList();
        Assert.Equal(expected.Count, actual.Count);
        Assert.Equal(expected.Select(e => e.Key).OrderBy(k => k).ToList(), actual.Select(e => e.Key).ToList());
    }

    /// <summary>A string key whose encoded size is exactly <paramref name="size"/> bytes.</summary>
    private static IndexKey KeyOfSize(int size, char fill, int discriminator)
    {
        var prefix = $"{fill}{discriminator:D5}";
        var probe = IndexKey.Create(prefix);
        var key = IndexKey.Create(prefix + new string(fill, size - probe.Data.Length));
        Assert.Equal(size, key.Data.Length);
        return key;
    }

    // ── #213: duplicates across a leaf split ─────────────────────────────────

    /// <summary>"p0".."p1999" without p5, with a second document for every i % 7 == 0.</summary>
    private static async Task<Dictionary<string, List<BsonId>>> InsertIssue213DataAsync(DynamicCollection col)
    {
        var ids = new Dictionary<string, List<BsonId>>();
        for (int i = 0; i < 2000; i++)
        {
            if (i == 5) continue;
            var name = $"p{i}";
            var copies = i % 7 == 0 ? 2 : 1;
            for (int c = 0; c < copies; c++)
            {
                var id = await col.InsertAsync(col.CreateDocument(["name"], b => b.AddString("name", name)));
                if (!ids.TryGetValue(name, out var list)) ids[name] = list = new List<BsonId>();
                list.Add(id);
            }
        }
        return ids;
    }

    [Fact]
    public async Task Dynamic_NonUniqueIndex_DuplicatesAcrossSplit_FoundForwardAndBackward()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("people");
        await col.CreateIndexAsync("name", "idx", unique: false);
        await InsertIssue213DataAsync(col);

        Assert.Equal(2, (await col.QueryIndexAsync("idx", "p70", "p70").ToListAsync()).Count);
        Assert.Equal(2, (await col.QueryIndexAsync("idx", "p70", "p70", false).ToListAsync()).Count);
    }

    [Fact]
    public async Task Dynamic_NonUniqueIndex_EveryDuplicatedKey_FoundForwardAndBackward()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("people");
        await col.CreateIndexAsync("name", "idx", unique: false);
        await InsertIssue213DataAsync(col);

        var missing = new List<string>();
        for (int i = 0; i < 2000; i += 7)
        {
            var name = $"p{i}";
            if ((await col.QueryIndexAsync("idx", name, name).ToListAsync()).Count != 2
                || (await col.QueryIndexAsync("idx", name, name, false).ToListAsync()).Count != 2)
                missing.Add(name);
        }
        Assert.Empty(missing);
    }

    [Fact]
    public async Task Dynamic_NonUniqueIndex_DeleteDuplicatesAcrossSplit_RemovesEveryIndexEntry()
    {
        var straddling = new[] { "p70", "p364", "p490", "p567", "p1148", "p1764", "p1890" };
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("people");
        await col.CreateIndexAsync("name", "idx", unique: false);
        var ids = await InsertIssue213DataAsync(col);

        foreach (var name in straddling)
            foreach (var id in ids[name])
                Assert.True(await col.DeleteAsync(id));

        // A stale index entry would point at a slot the new documents may reuse.
        foreach (var name in straddling)
            await col.InsertAsync(col.CreateDocument(["name"], b => b.AddString("name", name)));

        foreach (var name in straddling)
        {
            Assert.Single(await col.QueryIndexAsync("idx", name, name).ToListAsync());
            Assert.Single(await col.QueryIndexAsync("idx", name, name, false).ToListAsync());
        }
    }

    [Fact]
    public void BTree_NonUnique_DeleteDuplicateLeftOfSeparator_ReturnsTrueAndRemovesIt()
    {
        using var storage = new StorageEngine(_dbPath, PageFileConfig.Default);
        var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"));
        var txn = storage.BeginTransaction().TransactionId;

        // Same layout as #213: duplicates of every 7th key, so some pairs straddle a leaf split.
        var locations = new Dictionary<string, List<DocumentLocation>>();
        ushort slot = 0;
        uint page = 1;
        for (int i = 0; i < 2000; i++)
        {
            if (i == 5) continue;
            var name = $"p{i}";
            for (int c = 0; c < (i % 7 == 0 ? 2 : 1); c++)
            {
                if (slot == ushort.MaxValue) { slot = 0; page++; }
                var loc = new DocumentLocation(page, slot++);
                index.Insert(IndexKey.Create(name), loc, txn);
                if (!locations.TryGetValue(name, out var list)) locations[name] = list = new();
                list.Add(loc);
            }
        }

        var failures = new List<string>();
        for (int i = 0; i < 2000; i += 7)
        {
            var name = $"p{i}";
            var key = IndexKey.Create(name);
            foreach (var loc in locations[name])
                if (!index.Delete(key, loc, txn)) failures.Add(name);
            if (index.Range(key, key, IndexDirection.Forward, txn).Any()) failures.Add(name + " (left behind)");
        }
        Assert.Empty(failures);
    }

    [Fact]
    public void BTree_NonUnique_KeyDuplicatedAcrossManyLeaves_EveryCopyFoundAndDeleted()
    {
        using var storage = new StorageEngine(_dbPath, PageFileConfig.Small);
        var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"));
        var txn = storage.BeginTransaction().TransactionId;

        // 400 copies of one 500-byte key between smaller and larger neighbours: the copies fill
        // many leaves, and several internal separators are equal to the key.
        var same = IndexKey.Create("m" + new string('k', 500));
        var copies = new List<DocumentLocation>();
        for (int i = 0; i < 400; i++)
        {
            index.Insert(IndexKey.Create($"a{i:D4}"), new DocumentLocation(1, (ushort)i), txn);
            var loc = new DocumentLocation(2, (ushort)i);
            index.Insert(same, loc, txn);
            copies.Add(loc);
            index.Insert(IndexKey.Create($"z{i:D4}"), new DocumentLocation(3, (ushort)i), txn);
        }

        Assert.Equal(400, index.Range(same, same, IndexDirection.Forward, txn).Count());
        Assert.Equal(400, index.Range(same, same, IndexDirection.Backward, txn).Count());
        Assert.Equal(400, index.CountRange(same, same, txn));

        var rng = new Random(7);
        int left = copies.Count;
        foreach (var loc in copies.OrderBy(_ => rng.Next()))
        {
            Assert.True(index.Delete(same, loc, txn));
            left--;
            if (left % 50 == 0)
                Assert.Equal(left, index.Range(same, same, IndexDirection.Forward, txn).Count());
        }
        Assert.False(index.TryFind(same, out _, txn));
        Assert.Equal(800, index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn).Count());
    }

    // ── Randomised: long keys, duplicates, deletes, reopen ───────────────────

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(106)] // leaves an empty leaf that cannot be merged away; the cursor must skip it
    [InlineData(107)]
    public void BTree_RandomKeys_MatchSortedMultiset_BeforeAndAfterReopen(int seed)
    {
        var rng = new Random(seed);
        var config = PageFileConfig.Small; // 8 KB pages: more splits for the same data
        var model = new List<(string Key, DocumentLocation Loc)>();
        uint rootPageId;
        int maxKeySize;

        using (var storage = new StorageEngine(_dbPath, config))
        {
            var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"));
            rootPageId = index.RootPageId;
            maxKeySize = index.MaxKeySize;
            var txn = storage.BeginTransaction().TransactionId;

            // A pool of keys so that some of them are inserted several times.
            var pool = Enumerable.Range(0, 600).Select(_ => RandomKey(rng, maxKeySize)).ToList();
            ushort slot = 0;
            uint page = 1;
            for (int i = 0; i < 1500; i++)
            {
                var key = pool[rng.Next(pool.Count)];
                if (slot == ushort.MaxValue) { slot = 0; page++; }
                var loc = new DocumentLocation(page, slot++);
                index.Insert(IndexKey.Create(key), loc, txn);
                model.Add((key, loc));
            }

            // Delete about half of the entries, in random order.
            foreach (var victim in model.OrderBy(_ => rng.Next()).Take(model.Count / 2).ToList())
            {
                Assert.True(index.Delete(IndexKey.Create(victim.Key), victim.Loc, txn), $"delete of '{Abbrev(victim.Key)}' failed");
                model.Remove(victim);
            }

            // And insert some more on top of the shrunken tree.
            for (int i = 0; i < 300; i++)
            {
                var key = rng.Next(3) == 0 ? RandomKey(rng, maxKeySize) : pool[rng.Next(pool.Count)];
                var loc = new DocumentLocation(page, slot++);
                index.Insert(IndexKey.Create(key), loc, txn);
                model.Add((key, loc));
            }

            VerifyAgainstModel(index, model, pool, txn);
            storage.CommitTransactionAsync(txn).GetAwaiter().GetResult();
        }

        using (var storage = new StorageEngine(_dbPath, config))
        {
            var index = new BTreeIndex(storage, IndexOptions.CreateBTree("field"), rootPageId);
            VerifyAgainstModel(index, model, model.Select(m => m.Key).Distinct().ToList(), 0);
        }
    }

    private static string RandomKey(Random rng, int maxKeySize)
    {
        int roll = rng.Next(100);
        int length = roll < 70 ? rng.Next(1, 40)
                   : roll < 92 ? rng.Next(100, 900)
                   : rng.Next(maxKeySize - 200, maxKeySize - 20);
        var chars = new char[length];
        for (int i = 0; i < length; i++) chars[i] = (char)('a' + rng.Next(4));
        var s = new string(chars);
        while (IndexKey.Create(s).Data.Length > maxKeySize) s = s[..^1];
        return s;
    }

    private static string Abbrev(string key) => key.Length <= 12 ? key : $"{key[..12]}…({key.Length})";

    private static void VerifyAgainstModel(BTreeIndex index, List<(string Key, DocumentLocation Loc)> model, List<string> probes, ulong txn)
    {
        static string Loc(DocumentLocation l) => $"{l.PageId}:{l.SlotIndex}";

        var expected = model
            .Select(m => (Key: IndexKey.Create(m.Key), m.Loc))
            .OrderBy(m => m.Key)
            .ToList();

        var forward = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Forward, txn).ToList();
        Assert.Equal(expected.Select(e => e.Key).ToList(), forward.Select(e => e.Key).ToList());
        Assert.Equal(expected.Select(e => Loc(e.Loc)).OrderBy(s => s), forward.Select(e => Loc(e.Location)).OrderBy(s => s));

        var backward = index.Range(IndexKey.MinKey, IndexKey.MaxKey, IndexDirection.Backward, txn).ToList();
        Assert.Equal(forward.Select(e => e.Key).Reverse().ToList(), backward.Select(e => e.Key).ToList());

        // The cursor walks the same leaf chain, skipping leaves left empty by deletes.
        int cursorForward = 0, cursorBackward = 0;
        using (var cursor = index.CreateCursor(txn))
        {
            if (cursor.MoveToFirst()) do cursorForward++; while (cursor.MoveNext());
            if (cursor.MoveToLast()) do cursorBackward++; while (cursor.MovePrev());
        }
        Assert.Equal(expected.Count, cursorForward);
        Assert.Equal(expected.Count, cursorBackward);

        var byKey = model.GroupBy(m => m.Key).ToDictionary(g => g.Key, g => g.Select(m => Loc(m.Loc)).OrderBy(s => s).ToList());
        var failures = new List<string>();
        foreach (var probe in probes.Distinct())
        {
            var key = IndexKey.Create(probe);
            var want = byKey.TryGetValue(probe, out var w) ? w : new List<string>();
            var fwd = index.Range(key, key, IndexDirection.Forward, txn).Select(e => Loc(e.Location)).OrderBy(s => s).ToList();
            var bwd = index.Range(key, key, IndexDirection.Backward, txn).Select(e => Loc(e.Location)).OrderBy(s => s).ToList();
            if (!want.SequenceEqual(fwd)) failures.Add($"forward '{Abbrev(probe)}': {fwd.Count} of {want.Count}");
            if (!want.SequenceEqual(bwd)) failures.Add($"backward '{Abbrev(probe)}': {bwd.Count} of {want.Count}");
            if (index.TryFind(key, out _, txn) != want.Count > 0) failures.Add($"TryFind '{Abbrev(probe)}'");
            if (index.CountRange(key, key, txn) != want.Count) failures.Add($"CountRange '{Abbrev(probe)}'");
            if (index.Equal(key, txn).Any() != want.Count > 0) failures.Add($"Equal '{Abbrev(probe)}'");
        }
        Assert.Empty(failures);
    }
}
