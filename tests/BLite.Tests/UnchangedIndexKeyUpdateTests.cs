using System.Collections;
using System.Reflection;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Updates whose index keys do not change (follow-ups to #217/#213):
/// <list type="bullet">
///   <item>An unchanged key keeps its index entry: left alone when the document stays in its
///   slot, re-pointed when it moves. Deleting and re-inserting it cost a search through every
///   copy of a duplicated key in a DynamicCollection index, whose keys carry no document id.</item>
///   <item>An unchanged key is not size-checked again, so a key stored before
///   <see cref="BTreeIndex.MaxKeySize"/> was lowered (#217) does not make its document
///   impossible to update. A new or changed oversized key is still rejected up front.</item>
/// </list>
/// </summary>
public class UnchangedIndexKeyUpdateTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"unchanged_key_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            if (File.Exists(f)) File.Delete(f);
    }

    /// <summary>Longer than MaxKeySize on 16 KB pages (8156 bytes), within the old limit (16320).</summary>
    private static readonly string LegacyLongValue = "legacy-" + new string('L', 8300);

    /// <summary>The limit of the versions before #217, for <see cref="BTreeIndex.MaxKeySizeOverride"/>.</summary>
    private static int LegacyMaxKeySize(BTreeIndex index) => index.MaxKeySize * 2 + 8;

    private static BTreeIndex DynamicBTree(DynamicCollection col, string indexName)
    {
        var indexes = (IDictionary)typeof(DynamicCollection)
            .GetField("_secondaryIndexes", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(col)!;
        var entry = indexes[indexName]!;
        return (BTreeIndex)entry.GetType().GetProperty("BTree")!.GetValue(entry)!;
    }

    private static BTreeIndex TypedBTree<TId, T>(BLite.Core.Collections.DocumentCollection<TId, T> col, string indexName) where T : class
    {
        var manager = (CollectionIndexManager<TId, T>)typeof(BLite.Core.Collections.DocumentCollection<TId, T>)
            .GetField("_indexManager", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(col)!;
        return manager.GetIndex(indexName)!.BTreeIndex!;
    }

    /// <summary>The content of every page of <paramref name="index"/>, keyed by page id.</summary>
    private static Dictionary<uint, byte[]> Snapshot(BLiteEngine engine, BTreeIndex index)
    {
        var pages = new Dictionary<uint, byte[]>();
        foreach (var pageId in index.CollectAllPages())
        {
            var page = new byte[engine.Storage.PageSize];
            engine.Storage.ReadPage(pageId, null, page);
            pages[pageId] = page;
        }
        return pages;
    }

    private static BsonDocument Doc(DynamicCollection col, BsonId id, string name, string tag, int counter, string pad = "")
        => col.CreateDocument(["_id", "name", "tag", "counter", "pad"], b => b
            .AddId(id)
            .AddString("name", name)
            .AddString("tag", tag)
            .AddInt32("counter", counter)
            .AddString("pad", pad));

    private static int Counter(BsonDocument doc)
    {
        Assert.True(doc.TryGetInt32("counter", out var value));
        return value;
    }

    // ── Unchanged keys keep their entries ───────────────────────────────────

    [Fact]
    public async Task Dynamic_UpdateOfUnrelatedField_LeavesUniqueAndNonUniqueEntriesIntact()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);
        await col.CreateIndexAsync("name", "idx_name", unique: true);
        await col.CreateIndexAsync("tag", "idx_tag", unique: false);
        for (int i = 0; i < 300; i++)
            await col.InsertAsync(Doc(col, i, $"n{i:D4}", $"t{i % 3}", 0));

        for (int i = 0; i < 300; i++)
            Assert.True(await col.UpdateAsync(i, Doc(col, i, $"n{i:D4}", $"t{i % 3}", i + 1)));

        Assert.Equal(300, (await col.QueryIndexAsync("idx_name", null, null).ToListAsync()).Count);
        Assert.Equal(300, (await col.QueryIndexAsync("idx_tag", null, null).ToListAsync()).Count);
        Assert.Equal(100, (await col.QueryIndexAsync("idx_tag", "t1", "t1").ToListAsync()).Count);
        var hit = Assert.Single(await col.QueryIndexAsync("idx_name", "n0042", "n0042").ToListAsync());
        Assert.Equal(43, Counter(hit));
    }

    [Fact]
    public async Task Dynamic_UpdateThatRelocatesTheDocument_IsStillFoundThroughEveryIndex()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);
        await col.CreateIndexAsync("name", "idx_name", unique: true);
        await col.CreateIndexAsync("tag", "idx_tag", unique: false);
        for (int i = 0; i < 40; i++)
            await col.InsertAsync(Doc(col, i, $"n{i:D4}", "same", 0));

        // Growing every document by 3 KB moves it to another page.
        for (int i = 0; i < 40; i++)
            Assert.True(await col.UpdateAsync(i, Doc(col, i, $"n{i:D4}", "same", 1, new string('p', 3000))));

        Assert.Equal(40, (await col.QueryIndexAsync("idx_tag", "same", "same").ToListAsync()).Count);
        Assert.Equal(40, (await col.QueryIndexAsync("idx_tag", "same", "same", false).ToListAsync()).Count);
        for (int i = 0; i < 40; i++)
        {
            var byName = Assert.Single(await col.QueryIndexAsync("idx_name", $"n{i:D4}", $"n{i:D4}").ToListAsync());
            Assert.Equal(1, Counter(byName));
            Assert.NotNull(await col.FindByIdAsync(i));
        }
    }

    [Fact]
    public async Task Dynamic_UpdateOfUnrelatedField_DoesNotTouchAHeavilyDuplicatedIndex()
    {
        const int count = 5000;
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);
        await col.CreateIndexAsync("tag", "idx_tag", unique: false);
        for (int i = 0; i < count; i++)
            await col.InsertAsync(Doc(col, i, $"n{i:D5}", "shared", 1000));

        var tagIndex = DynamicBTree(col, "idx_tag");
        var before = Snapshot(engine, tagIndex);

        // Same-size documents stay in their slots: the index is not rewritten at all, instead of
        // searching the leaves of 5000 copies of "shared" for every update.
        for (int i = 0; i < count; i++)
            Assert.True(await col.UpdateAsync(i, Doc(col, i, $"n{i:D5}", "shared", 2000 + i % 1000)));

        var after = Snapshot(engine, tagIndex);
        Assert.Equal(before.Keys.OrderBy(k => k), after.Keys.OrderBy(k => k));
        Assert.All(before, p => Assert.True(p.Value.AsSpan().SequenceEqual(after[p.Key]), $"index page {p.Key} was rewritten"));
        Assert.Equal(count, tagIndex.CountRange(IndexKey.Create("shared"), IndexKey.Create("shared")));
    }

    [Fact]
    public async Task Typed_UpdateOfUnrelatedField_LeavesUniqueAndNonUniqueEntriesIntact()
    {
        using var db = new TestDbContext(_dbPath);
        await db.PeopleV2.CreateIndexAsync(p => p.Name, "idx_name", unique: true);
        var people = Enumerable.Range(0, 200).Select(i => new PersonV2 { Name = $"n{i:D4}", Age = i % 4 }).ToList();
        foreach (var p in people) await db.PeopleV2.InsertAsync(p);

        // Age is indexed (non-unique) by the model; Name has a unique index.
        foreach (var p in people) await db.PeopleV2.UpdateAsync(p);
        foreach (var p in people.Take(100)) { p.Name = $"m{p.Name[1..]}"; await db.PeopleV2.UpdateAsync(p); }

        Assert.Equal(200, (await db.PeopleV2.QueryIndexAsync("idx_name", null, null).ToListAsync()).Count);
        Assert.Equal(50, await db.PeopleV2.AsQueryable().Where(p => p.Age == 2).CountAsync());
        Assert.Single(await db.PeopleV2.AsQueryable().Where(p => p.Name == "m0042").ToListAsync());
        Assert.Single(await db.PeopleV2.AsQueryable().Where(p => p.Name == "n0142").ToListAsync());
    }

    // ── Keys stored before MaxKeySize was lowered (#217) ─────────────────────

    [Fact]
    public async Task Dynamic_LegacyOversizedKey_UnrelatedUpdatesSucceed_NewOversizedKeysAreRejected()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.String);
        await col.CreateIndexAsync("name", "idx_name", unique: false);
        await col.CreateIndexAsync("tag", "idx_tag", unique: true);
        var nameIndex = DynamicBTree(col, "idx_name");

        // Stand-in for a database written by an older version: an indexed value between the new
        // limit and the old one. (A dynamic _id that long cannot be built: the BSON writer rejects
        // it, so the primary-key case is covered by the typed test below.)
        nameIndex.MaxKeySizeOverride = LegacyMaxKeySize(nameIndex);
        await col.InsertAsync(Doc(col, "a", LegacyLongValue, "t1", 0));
        await col.InsertAsync(Doc(col, "b", LegacyLongValue, "t2", 0));
        nameIndex.MaxKeySizeOverride = null;

        // In place, then relocated by growing the document.
        Assert.True(await col.UpdateAsync("a", Doc(col, "a", LegacyLongValue, "t1", 1)));
        Assert.True(await col.UpdateAsync("b", Doc(col, "b", LegacyLongValue, "t2", 1)));
        Assert.True(await col.UpdateAsync("b", Doc(col, "b", LegacyLongValue, "t2", 2, new string('p', 3000))));
        Assert.True(await col.UpdateAsync("a", Doc(col, "a", LegacyLongValue, "t3", 2, new string('p', 3000))));

        Assert.Equal(2, (await col.QueryIndexAsync("idx_name", LegacyLongValue, LegacyLongValue).ToListAsync()).Count);
        Assert.Equal(2, (await col.QueryIndexAsync("idx_name", LegacyLongValue, LegacyLongValue, false).ToListAsync()).Count);
        Assert.Equal(2, Counter((await col.FindByIdAsync("a"))!));
        Assert.Single(await col.QueryIndexAsync("idx_tag", "t3", "t3").ToListAsync());

        // A new or changed oversized key is still rejected up front.
        var otherLong = "other-" + new string('O', 8300);
        await Assert.ThrowsAsync<BLiteValidationException>(() =>
            col.InsertAsync(Doc(col, "c", otherLong, "t4", 0)).AsTask());
        await Assert.ThrowsAsync<BLiteValidationException>(() =>
            col.UpdateAsync("b", Doc(col, "b", otherLong, "t2", 3)).AsTask());
        Assert.Equal(2, await col.CountAsync());
        Assert.Equal(2, (await col.QueryIndexAsync("idx_name", LegacyLongValue, LegacyLongValue).ToListAsync()).Count);
    }

    [Fact]
    public async Task Typed_LegacyOversizedPrimaryKey_UpdatesInPlaceAndRelocated()
    {
        var legacyId = "id-" + new string('I', 8300);
        using var db = new TestDbContext(_dbPath);
        var primary = db.StringEntities._primaryIndex;

        primary.MaxKeySizeOverride = LegacyMaxKeySize(primary);
        await db.StringEntities.InsertAsync(new StringEntity { Id = legacyId, Value = "a" });
        primary.MaxKeySizeOverride = null;

        await db.StringEntities.UpdateAsync(new StringEntity { Id = legacyId, Value = "b" });
        Assert.Equal("b", (await db.StringEntities.FindByIdAsync(legacyId))!.Value);

        // Growing the document moves it: the primary entry follows it.
        await db.StringEntities.UpdateAsync(new StringEntity { Id = legacyId, Value = new string('c', 3000) });
        Assert.Equal(3000, (await db.StringEntities.FindByIdAsync(legacyId))!.Value!.Length);
        Assert.Equal(1, await db.StringEntities.CountAsync());

        await Assert.ThrowsAsync<BLiteValidationException>(() =>
            db.StringEntities.InsertAsync(new StringEntity { Id = "id-" + new string('J', 8300), Value = "x" }).AsTask());
    }

    [Fact]
    public async Task Typed_LegacyOversizedIndexedValue_UnrelatedUpdateSucceeds_ChangedOversizedValueIsRejected()
    {
        using var db = new TestDbContext(_dbPath);
        await db.PeopleV2.CreateIndexAsync(p => p.Name, "idx_name", unique: false);
        var nameIndex = TypedBTree(db.PeopleV2, "idx_name");

        nameIndex.MaxKeySizeOverride = LegacyMaxKeySize(nameIndex);
        var legacy = new PersonV2 { Name = LegacyLongValue, Age = 1 };
        await db.PeopleV2.InsertAsync(legacy);
        nameIndex.MaxKeySizeOverride = null;

        legacy.Age = 2;
        await db.PeopleV2.UpdateAsync(legacy);
        var found = Assert.Single(await db.PeopleV2.AsQueryable().Where(p => p.Name == LegacyLongValue).ToListAsync());
        Assert.Equal(2, found.Age);

        legacy.Name = "other-" + new string('O', 8300);
        await Assert.ThrowsAsync<BLiteValidationException>(() => db.PeopleV2.UpdateAsync(legacy).AsTask());
        await Assert.ThrowsAsync<BLiteValidationException>(() =>
            db.PeopleV2.InsertAsync(new PersonV2 { Name = "new-" + new string('N', 8300), Age = 3 }).AsTask());
        Assert.Equal(2, (await db.PeopleV2.FindByIdAsync(legacy.Id))!.Age);
    }

    [Fact]
    public async Task Typed_LegacyOversizedIndexedValue_FollowsItsDocumentWhenItMoves()
    {
        using var db = new TestDbContext(_dbPath);
        var indexName = db.TestDocuments.GetIndexes().Single(i => i.PropertyPaths.SequenceEqual(["Category"])).Name;
        var categoryIndex = TypedBTree(db.TestDocuments, indexName);

        categoryIndex.MaxKeySizeOverride = LegacyMaxKeySize(categoryIndex);
        var legacy = new TestDocument { Category = LegacyLongValue, Amount = 1, Name = "small" };
        await db.TestDocuments.InsertAsync(legacy);
        categoryIndex.MaxKeySizeOverride = null;

        // Growing the document moves it: the entry of its unchanged, oversized key is re-pointed.
        legacy.Name = new string('g', 3000);
        await db.TestDocuments.UpdateAsync(legacy);

        var found = Assert.Single(await db.TestDocuments.AsQueryable().Where(d => d.Category == LegacyLongValue).ToListAsync());
        Assert.Equal(3000, found.Name.Length);
        Assert.Equal(1, await db.TestDocuments.AsQueryable().CountAsync(d => d.Category == LegacyLongValue));
    }
}
