using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;

namespace BLite.Tests;

/// <summary>
/// A unique <see cref="DynamicCollection.CreateIndexAsync"/> that fails because existing documents
/// share a value leaves no index behind (#271). It used to stay registered, half filled, until the
/// next reopen: listed, answering nothing for the documents the fill had not reached, enforcing
/// nothing for them, and blocking a retry with "already exists".
/// </summary>
public sealed class DynamicUniqueIndexCreateFailureTests : IDisposable
{
    private readonly string _dir;

    public DynamicUniqueIndexCreateFailureTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"blite_dyn_unique_fail_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private static BsonDocument Doc(DynamicCollection col, string name, string code) =>
        col.CreateDocument(["name", "code"], b => b.AddString("name", name).AddString("code", code));

    private static string Name(BsonDocument d) => d.TryGetString("name", out var n) ? n : "?";

    [Fact]
    public async Task Failed_Unique_CreateIndex_Leaves_No_Index_And_Can_Be_Retried()
    {
        var path = Path.Combine(_dir, "db.blite");
        using (var engine = new BLiteEngine(path))
        {
            var col = engine.GetOrCreateCollection("items");
            await col.InsertAsync(Doc(col, "a", "x"));
            await col.InsertAsync(Doc(col, "b", "y"));
            var c = await col.InsertAsync(Doc(col, "c", "x"));

            await Assert.ThrowsAnyAsync<InvalidOperationException>(() => col.CreateIndexAsync("code", unique: true));

            Assert.Empty(col.ListIndexes());
            // BLQL scans again instead of reading the half-filled index.
            Assert.Equal(new[] { "a", "c" }, col.Query(BlqlFilter.Eq("code", "x")).ToList().Select(Name).OrderBy(n => n));

            // The data is fixed and the same index is created.
            Assert.True(await col.DeleteAsync(c));
            await col.CreateIndexAsync("code", unique: true);
            Assert.Equal(new[] { "idx_code" }, col.ListIndexes());
            Assert.Equal(new[] { "a" }, (await col.QueryIndexAsync("idx_code", "x", "x").ToListAsync()).Select(Name));
            await Assert.ThrowsAnyAsync<InvalidOperationException>(async () => await col.InsertAsync(Doc(col, "d", "y")));
        }

        using (var reopened = new BLiteEngine(path))
        {
            var col = reopened.GetOrCreateCollection("items");
            Assert.Equal(new[] { "idx_code" }, col.ListIndexes());
            Assert.Equal(new[] { "b" }, (await col.QueryIndexAsync("idx_code", "y", "y").ToListAsync()).Select(Name));
        }
    }
}
