using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// A write that fails inside a caller-owned transaction (#185).
/// <list type="bullet">
///   <item>Input rejected up front (<see cref="BLiteValidationException"/>) leaves nothing behind and
///   the transaction usable: the caller can carry on and commit the other writes.</item>
///   <item>Any other failure while writing rolls the transaction back, so a half-applied write can
///   never be committed.</item>
/// </list>
/// After every commit the tests check that the data and every index agree: the primary count,
/// each index's entry count and lookups, and the live slots on the data pages (a failed insert must
/// not leave an orphaned slot behind).
/// </summary>
public class ExplicitTransactionFailureTests : IDisposable
{
    private readonly string _dbPath;

    public ExplicitTransactionFailureTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_tx_failure_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        foreach (var p in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal"), _dbPath + "-shm" })
            if (File.Exists(p)) File.Delete(p);
    }

    // ── Typed collections: validation failures keep the transaction usable ───

    [Fact]
    public async Task InsertAsync_Duplicate_Id_Keeps_Transaction_And_Leaves_No_Orphan_Slot()
    {
        using var db = new TestDbContext(_dbPath);
        using var tx = await db.BeginTransactionAsync();

        await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 1, Name = "first" }, tx);

        var ex = await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 1, Name = "duplicate" }, tx));
        Assert.Contains("Duplicate key violation", ex.Message);
        Assert.Equal(TransactionState.Active, tx.State);

        await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 2, Name = "second" }, tx);
        await tx.CommitAsync();

        Assert.Equal("first", (await db.AsyncDocs.FindByIdAsync(1))?.Name);
        Assert.Equal("second", (await db.AsyncDocs.FindByIdAsync(2))?.Name);
        Assert.Equal(2, await db.AsyncDocs.CountAsync());
        Assert.Equal(2, CountLiveSlots(db.Storage, db.AsyncDocs.CollectionName));
    }

    [Fact]
    public async Task InsertBulkAsync_Rejected_First_Document_Keeps_Transaction()
    {
        using var db = new TestDbContext(_dbPath);
        using var tx = await db.BeginTransactionAsync();

        await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 1, Name = "first" }, tx);

        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await db.AsyncDocs.InsertBulkAsync([new AsyncDoc { Id = 1, Name = "duplicate" }], tx));
        Assert.Equal(TransactionState.Active, tx.State);

        await tx.CommitAsync();

        Assert.Equal("first", (await db.AsyncDocs.FindByIdAsync(1))?.Name);
        Assert.Equal(1, await db.AsyncDocs.CountAsync());
        Assert.Equal(1, CountLiveSlots(db.Storage, db.AsyncDocs.CollectionName));
    }

    [Fact]
    public async Task InsertBulkAsync_Rejected_Later_Document_Rolls_Back_Transaction()
    {
        using var db = new TestDbContext(_dbPath);
        await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 1, Name = "committed" });

        using var tx = await db.BeginTransactionAsync();
        await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 2, Name = "earlier in tx" }, tx);

        // Document 3 is written before document 1 is rejected: the call cannot be undone on
        // its own, so the whole transaction is rolled back.
        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await db.AsyncDocs.InsertBulkAsync(
                [new AsyncDoc { Id = 3, Name = "written" }, new AsyncDoc { Id = 1, Name = "duplicate" }], tx));
        Assert.Equal(TransactionState.Aborted, tx.State);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await tx.CommitAsync());

        Assert.Equal(1, await db.AsyncDocs.CountAsync());
        Assert.Equal("committed", (await db.AsyncDocs.FindByIdAsync(1))?.Name);
        Assert.Null(await db.AsyncDocs.FindByIdAsync(2));
        Assert.Null(await db.AsyncDocs.FindByIdAsync(3));
    }

    [Fact]
    public async Task UpsertAsync_Bad_Embedding_Leaves_Every_Index_Consistent()
    {
        using var db = new TestDbContext(_dbPath);
        // idx_vector (3 dimensions) is declared on the model; idx_title comes after it.
        await db.VectorItems.CreateIndexAsync(x => x.Title, "idx_title");
        using var tx = await db.BeginTransactionAsync();

        var good = new VectorEntity { Title = "good", Embedding = [1f, 2f, 3f] };
        await db.VectorItems.UpsertAsync(good, tx);

        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await db.VectorItems.UpsertAsync(new VectorEntity { Title = "bad", Embedding = [1f, 2f] }, tx));
        Assert.Equal(TransactionState.Active, tx.State);

        await tx.CommitAsync();

        Assert.Equal("good", (await db.VectorItems.FindByIdAsync(good.Id))?.Title);
        await AssertTypedVectorItemsConsistent(db, expectedCount: 1);
    }

    [Fact]
    public async Task UpsertBulkAsync_Bad_Embedding_First_Keeps_Transaction()
    {
        using var db = new TestDbContext(_dbPath);
        await db.VectorItems.CreateIndexAsync(x => x.Title, "idx_title");
        using var tx = await db.BeginTransactionAsync();

        var good = new VectorEntity { Title = "good", Embedding = [1f, 2f, 3f] };
        await db.VectorItems.UpsertAsync(good, tx);

        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await db.VectorItems.UpsertBulkAsync([new VectorEntity { Title = "bad", Embedding = [1f, 2f] }], tx));

        await tx.CommitAsync();

        Assert.Equal("good", (await db.VectorItems.FindByIdAsync(good.Id))?.Title);
        await AssertTypedVectorItemsConsistent(db, expectedCount: 1);
    }

    [Fact]
    public async Task UpdateAsync_Bad_Embedding_Leaves_Every_Index_Consistent()
    {
        using var db = new TestDbContext(_dbPath);
        await db.VectorItems.CreateIndexAsync(x => x.Title, "idx_title");
        var a = new VectorEntity { Title = "a", Embedding = [1f, 2f, 3f] };
        var b = new VectorEntity { Title = "b", Embedding = [4f, 5f, 6f] };
        await db.VectorItems.InsertAsync(a);
        await db.VectorItems.InsertAsync(b);

        using var tx = await db.BeginTransactionAsync();
        a.Title = "a2";
        Assert.True(await db.VectorItems.UpdateAsync(a, tx));

        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await db.VectorItems.UpdateAsync(new VectorEntity { Id = b.Id, Title = "z", Embedding = [1f, 2f] }, tx));
        Assert.Equal(TransactionState.Active, tx.State);

        await tx.CommitAsync();

        Assert.Equal("a2", (await db.VectorItems.FindByIdAsync(a.Id))?.Title);
        Assert.Equal("b", (await db.VectorItems.FindByIdAsync(b.Id))?.Title);
        Assert.Empty(await db.VectorItems.QueryIndexAsync("idx_title", "z", "z").ToListAsync());
        await AssertTypedVectorItemsConsistent(db, expectedCount: 2);
    }

    [Fact]
    public async Task UniqueIndex_Violation_Keeps_Transaction()
    {
        using var db = new TestDbContext(_dbPath);
        await db.PeopleV2.CreateIndexAsync(p => p.Name, "idx_name", unique: true);
        using var tx = await db.BeginTransactionAsync();

        await db.PeopleV2.InsertAsync(new PersonV2 { Name = "dup", Age = 1 }, tx);
        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await db.PeopleV2.InsertAsync(new PersonV2 { Name = "dup", Age = 2 }, tx));
        await db.PeopleV2.InsertAsync(new PersonV2 { Name = "other", Age = 3 }, tx);
        await tx.CommitAsync();

        Assert.Equal(2, await db.PeopleV2.CountAsync());
        Assert.Equal(2, (await db.PeopleV2.QueryIndexAsync("idx_name", null, null).ToListAsync()).Count);
        // PersonV2 is declared with an index on Age.
        Assert.Equal(2, (await db.PeopleV2.QueryIndexAsync("idx_Age", null, null).ToListAsync()).Count);
        Assert.Equal(2, CountLiveSlots(db.Storage, db.PeopleV2.CollectionName));
    }

    // ── Typed collections: other failures roll the transaction back ──────────

    [Fact]
    public async Task Failure_After_The_First_Page_Write_Rolls_Back_Callers_Transaction()
    {
        using var db = new TestDbContext(_dbPath);
        await db.PeopleV2.CreateIndexAsync(p => p.Name, "idx_name");
        await db.PeopleV2.InsertAsync(new PersonV2 { Name = "committed", Age = 1 });

        using var tx = await db.BeginTransactionAsync();
        CorruptIndexRootInTransaction(db.Storage, db.PeopleV2.CollectionName, "idx_name", tx);
        var ex = await Record.ExceptionAsync(() =>
            db.PeopleV2.InsertAsync(new PersonV2 { Name = "failing", Age = 2 }, tx).AsTask());
        Assert.NotNull(ex);

        // The failing insert had already written its data page and the primary index.
        Assert.IsNotType<BLiteValidationException>(ex);
        Assert.Equal(TransactionState.Aborted, tx.State);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await db.PeopleV2.InsertAsync(new PersonV2 { Name = "after", Age = 2 }, tx));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await tx.CommitAsync());

        Assert.Equal(1, await db.PeopleV2.CountAsync());
        Assert.Single(await db.PeopleV2.QueryIndexAsync("idx_name", null, null).ToListAsync());
        Assert.Equal(1, CountLiveSlots(db.Storage, db.PeopleV2.CollectionName));
    }

    [Fact]
    public async Task Implicit_Transaction_Is_Still_Rolled_Back_On_Failure()
    {
        using var db = new TestDbContext(_dbPath);

        await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 1, Name = "first" });
        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 1, Name = "duplicate" }));

        // A later auto-committed write must not be blocked by a dangling transaction.
        await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = 2, Name = "second" });

        Assert.Equal("first", (await db.AsyncDocs.FindByIdAsync(1))?.Name);
        Assert.Equal("second", (await db.AsyncDocs.FindByIdAsync(2))?.Name);
        Assert.Equal(2, CountLiveSlots(db.Storage, db.AsyncDocs.CollectionName));
    }

    // ── Dynamic collections: validation failures keep the transaction usable ─

    [Fact]
    public async Task Dynamic_InsertAsync_Duplicate_Id_Keeps_Transaction_And_Leaves_No_Orphan_Slot()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);
        var tx = await engine.BeginTransactionAsync();

        await col.InsertAsync(Doc(col, 1, "first"), tx);

        var ex = await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await col.InsertAsync(Doc(col, 1, "duplicate"), tx));
        Assert.Contains("Duplicate key violation", ex.Message);
        Assert.Equal(TransactionState.Active, tx.State);

        await col.InsertAsync(Doc(col, 2, "second"), tx);
        await tx.CommitAsync();

        Assert.Equal("first", await NameOf(col, 1));
        Assert.Equal("second", await NameOf(col, 2));
        Assert.Equal(2, await col.CountAsync());
        Assert.Equal(2, CountLiveSlots(engine.Storage, col.Name));
    }

    [Fact]
    public async Task Dynamic_InsertBulkAsync_Rejected_First_Document_Keeps_Transaction()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);
        var tx = await engine.BeginTransactionAsync();

        await col.InsertAsync(Doc(col, 1, "first"), tx);

        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await col.InsertBulkAsync([Doc(col, 1, "duplicate")], tx));

        await tx.CommitAsync();

        Assert.Equal("first", await NameOf(col, 1));
        Assert.Equal(1, await col.CountAsync());
        Assert.Equal(1, CountLiveSlots(engine.Storage, col.Name));
    }

    [Fact]
    public async Task Dynamic_UniqueViolation_After_Another_Index_Leaves_Every_Index_Consistent()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("people", BsonIdType.Int32);
        // idx_email is maintained after idx_tag, so a late duplicate check would leave the
        // document in the primary index and in idx_tag.
        await col.CreateIndexAsync("tag", "idx_tag");
        await col.CreateIndexAsync("email", "idx_email", unique: true);
        var tx = await engine.BeginTransactionAsync();

        await col.InsertAsync(Person(col, 1, "a@x", "t"), tx);
        var ex = await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await col.InsertAsync(Person(col, 2, "a@x", "t"), tx));
        Assert.Contains("Duplicate key violation", ex.Message);
        await col.InsertAsync(Person(col, 3, "b@x", "t"), tx);
        await tx.CommitAsync();

        Assert.Null(await col.FindByIdAsync((BsonId)2));
        await AssertDynamicBTreeIndexConsistent(col, "idx_tag", "tag");
        await AssertDynamicBTreeIndexConsistent(col, "idx_email", "email");
        Assert.Equal(2, await col.CountAsync());
        Assert.Equal(2, CountLiveSlots(engine.Storage, col.Name));
    }

    [Fact]
    public async Task Dynamic_Vector_Dimension_Mismatch_After_BTree_Index_Leaves_Every_Index_Consistent()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("vecs", BsonIdType.Int32);
        await col.CreateIndexAsync("tag", "idx_tag");
        await col.CreateVectorIndexAsync("emb", dimensions: 3, name: "idx_emb");
        var tx = await engine.BeginTransactionAsync();

        await col.InsertAsync(VecDoc(col, 1, [1f, 2f, 3f]), tx);
        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await col.InsertAsync(VecDoc(col, 2, [1f, 2f]), tx));
        await tx.CommitAsync();

        Assert.Null(await col.FindByIdAsync((BsonId)2));
        await AssertDynamicVectorItemsConsistent(engine, col, expectedCount: 1);
    }

    [Fact]
    public async Task Dynamic_UpsertAsync_Bad_Vector_Leaves_Every_Index_Consistent()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("vecs", BsonIdType.Int32);
        await col.CreateIndexAsync("tag", "idx_tag");
        await col.CreateVectorIndexAsync("emb", dimensions: 3, name: "idx_emb");
        var tx = await engine.BeginTransactionAsync();

        await col.UpsertAsync(VecDoc(col, 1, [1f, 2f, 3f]), tx);
        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await col.UpsertAsync(VecDoc(col, 2, [1f, 2f]), tx));
        Assert.Equal(TransactionState.Active, tx.State);

        await tx.CommitAsync();

        Assert.NotNull(await col.FindByIdAsync((BsonId)1));
        await AssertDynamicVectorItemsConsistent(engine, col, expectedCount: 1);
    }

    [Fact]
    public async Task Dynamic_UpdateAsync_Bad_Vector_Leaves_Every_Index_Consistent()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("vecs", BsonIdType.Int32);
        await col.CreateIndexAsync("tag", "idx_tag");
        await col.CreateVectorIndexAsync("emb", dimensions: 3, name: "idx_emb");
        await col.InsertAsync(VecDoc(col, 1, [1f, 2f, 3f], "x"));
        await col.InsertAsync(VecDoc(col, 2, [4f, 5f, 6f], "x"));

        var tx = await engine.BeginTransactionAsync();
        Assert.True(await col.UpdateAsync((BsonId)1, VecDoc(col, 1, [7f, 8f, 9f], "updated"), tx));

        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await col.UpdateAsync((BsonId)2, VecDoc(col, 2, [1f, 2f], "z"), tx));
        Assert.Equal(TransactionState.Active, tx.State);

        await tx.CommitAsync();

        Assert.Equal("updated", await TagOf(col, 1));
        Assert.Equal("x", await TagOf(col, 2));
        Assert.Empty(await col.QueryIndexAsync("idx_tag", "z", "z").ToListAsync());
        Assert.Equal([2], await IdsInIndex(col, "idx_tag", "x"));
        await AssertDynamicVectorItemsConsistent(engine, col, expectedCount: 2);
    }

    [Fact]
    public async Task Dynamic_UpdateBulkAsync_Rejected_Later_Document_Rolls_Back_Transaction()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("vecs", BsonIdType.Int32);
        await col.CreateIndexAsync("tag", "idx_tag");
        await col.CreateVectorIndexAsync("emb", dimensions: 3, name: "idx_emb");
        await col.InsertAsync(VecDoc(col, 1, [1f, 2f, 3f], "x"));
        await col.InsertAsync(VecDoc(col, 2, [4f, 5f, 6f], "x"));

        var tx = await engine.BeginTransactionAsync();
        await Assert.ThrowsAsync<BLiteValidationException>(async () =>
            await col.UpdateBulkAsync(
                [((BsonId)1, VecDoc(col, 1, [7f, 8f, 9f], "y")), ((BsonId)2, VecDoc(col, 2, [1f, 2f], "z"))], tx));
        Assert.Equal(TransactionState.Aborted, tx.State);

        Assert.Equal("x", await TagOf(col, 1));
        Assert.Equal("x", await TagOf(col, 2));
        await AssertDynamicVectorItemsConsistent(engine, col, expectedCount: 2);
    }

    // ── Dynamic collections: other failures roll the transaction back ────────

    [Fact]
    public async Task Dynamic_Failure_After_The_First_Page_Write_Rolls_Back_Callers_Transaction()
    {
        using var engine = new BLiteEngine(_dbPath);
        var col = engine.GetOrCreateCollection("docs", BsonIdType.Int32);
        await col.CreateIndexAsync("name", "idx_name");
        await col.InsertAsync(Doc(col, 0, "committed"));

        var tx = await engine.BeginTransactionAsync();
        CorruptIndexRootInTransaction(engine.Storage, col.Name, "idx_name", tx);
        var ex = await Record.ExceptionAsync(() => col.InsertAsync(Doc(col, 1, "failing"), tx).AsTask());
        Assert.NotNull(ex);

        Assert.IsNotType<BLiteValidationException>(ex);
        Assert.Equal(TransactionState.Aborted, tx.State);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await col.InsertAsync(Doc(col, 10_000, "after"), tx));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await tx.CommitAsync());

        Assert.Equal(1, await col.CountAsync());
        await AssertDynamicBTreeIndexConsistent(col, "idx_name", "name");
        Assert.Equal(1, CountLiveSlots(engine.Storage, col.Name));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Makes the next insert into the B-Tree index <paramref name="indexName"/> fail while the key
    /// is being inserted, after the document's data page and primary index entry were written:
    /// a real failure in the middle of a write. (These tests used to rely on long keys
    /// overflowing a node page, which #217 fixed.) The index root, a leaf holding one key, is
    /// rewritten inside <paramref name="tx"/> with a key length that runs past the end of the
    /// page, so the rollback discards the damage along with the failed write.
    /// </summary>
    private static void CorruptIndexRootInTransaction(StorageEngine storage, string collection, string indexName, ITransaction tx)
    {
        var rootPageId = storage.GetCollectionMetadata(collection)!.Indexes.Single(i => i.Name == indexName).RootPageId;
        var page = new byte[storage.PageSize];
        storage.ReadPage(rootPageId, tx.TransactionId, page);
        BitConverter.TryWriteBytes(page.AsSpan(32 + 20, 4), int.MaxValue / 2); // first key's length
        storage.WritePage(rootPageId, tx.TransactionId, page);
    }

    private static async Task AssertTypedVectorItemsConsistent(TestDbContext db, int expectedCount)
    {
        var docs = await db.VectorItems.FindAllAsync().ToListAsync();
        Assert.Equal(expectedCount, docs.Count);
        Assert.Equal(expectedCount, await db.VectorItems.CountAsync());
        Assert.Equal(expectedCount, CountLiveSlots(db.Storage, db.VectorItems.CollectionName));

        var byTitle = await db.VectorItems.QueryIndexAsync("idx_title", null, null).ToListAsync();
        Assert.Equal(docs.Select(d => d.Id).OrderBy(i => i), byTitle.Select(d => d.Id).OrderBy(i => i));
        foreach (var doc in docs)
            Assert.Contains(await db.VectorItems.QueryIndexAsync("idx_title", doc.Title, doc.Title).ToListAsync(),
                d => d.Id == doc.Id);

        var byVector = await db.VectorItems.VectorSearchAsync("idx_vector", [0f, 0f, 0f], k: 100).ToListAsync();
        Assert.Equal(docs.Select(d => d.Id).OrderBy(i => i), byVector.Select(d => d.Id).OrderBy(i => i));
    }

    private static async Task AssertDynamicVectorItemsConsistent(BLiteEngine engine, DynamicCollection col, int expectedCount)
    {
        var docs = await col.FindAllAsync().ToListAsync();
        Assert.Equal(expectedCount, docs.Count);
        Assert.Equal(expectedCount, await col.CountAsync());
        Assert.Equal(expectedCount, CountLiveSlots(engine.Storage, col.Name));

        await AssertDynamicBTreeIndexConsistent(col, "idx_tag", "tag");

        var byVector = await col.VectorSearchAsync("idx_emb", [0f, 0f, 0f], k: 100).ToListAsync();
        Assert.Equal(docs.Select(IdOf).OrderBy(i => i), byVector.Select(IdOf).OrderBy(i => i));
    }

    /// <summary>
    /// The index holds exactly one entry per document, and looking up each document's value
    /// finds that document.
    /// </summary>
    private static async Task AssertDynamicBTreeIndexConsistent(DynamicCollection col, string indexName, string field)
    {
        var docs = await col.FindAllAsync().ToListAsync();
        var indexed = await col.QueryIndexAsync(indexName, null, null).ToListAsync();
        Assert.Equal(docs.Select(IdOf).OrderBy(i => i), indexed.Select(IdOf).OrderBy(i => i));

        foreach (var doc in docs)
        {
            Assert.True(doc.TryGetString(field, out var value));
            Assert.Contains(IdOf(doc), await IdsInIndex(col, indexName, value));
        }
    }

    private static async Task<List<int>> IdsInIndex(DynamicCollection col, string indexName, string value) =>
        (await col.QueryIndexAsync(indexName, value, value).ToListAsync()).Select(IdOf).ToList();

    /// <summary>Live (not deleted) slots on the collection's data pages.</summary>
    private static int CountLiveSlots(StorageEngine storage, string collectionName)
    {
        var buffer = new byte[storage.PageSize];
        int live = 0;
        foreach (var pageId in storage.GetCollectionPageIds(collectionName))
        {
            storage.ReadPage(pageId, null, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Data) continue;

            for (int i = 0; i < header.SlotCount; i++)
            {
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(SlottedPageHeader.Size + i * SlotEntry.Size));
                if ((slot.Flags & SlotFlags.Deleted) == 0) live++;
            }
        }
        return live;
    }

    private static int IdOf(BsonDocument doc)
    {
        Assert.True(doc.TryGetId(out var id));
        return id.AsInt32();
    }

    private static BsonDocument Doc(DynamicCollection col, int id, string name) =>
        col.CreateDocument(["_id", "name"], b => b.AddId((BsonId)id).AddString("name", name));

    private static BsonDocument Person(DynamicCollection col, int id, string email, string tag) =>
        col.CreateDocument(["_id", "email", "tag"], b => b.AddId((BsonId)id).AddString("email", email).AddString("tag", tag));

    private static BsonDocument VecDoc(DynamicCollection col, int id, float[] emb, string tag = "orig") =>
        col.CreateDocument(["_id", "emb", "tag"], b => b.AddId((BsonId)id).AddFloatArray("emb", emb).AddString("tag", tag));

    private static async Task<string?> NameOf(DynamicCollection col, int id)
    {
        var doc = await col.FindByIdAsync((BsonId)id);
        return doc != null && doc.TryGetString("name", out var name) ? name : null;
    }

    private static async Task<string?> TagOf(DynamicCollection col, int id)
    {
        var doc = await col.FindByIdAsync((BsonId)id);
        return doc != null && doc.TryGetString("tag", out var tag) ? tag : null;
    }
}
