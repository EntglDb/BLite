using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Unit tests for <see cref="HashIndex"/> — exact-match in-memory index.
/// Tests Insert, TryFind, Remove, FindAll, and the Unique constraint.
/// </summary>
public class HashIndexTests
{
    private static HashIndex CreateIndex(bool unique = false)
    {
        var options = unique
            ? new IndexOptions { Type = IndexType.Hash, Unique = true, Fields = ["key"] }
            : IndexOptions.CreateHash("key");
        return new HashIndex(options);
    }

    private static IndexKey MakeKey(int value) => IndexKey.Create(value);
    private static IndexKey MakeKey(string value) => IndexKey.Create(value);

    private static DocumentLocation MakeLoc(uint pageId, ushort slot = 0)
        => new DocumentLocation(pageId, slot);

    // ── Insert / TryFind ──────────────────────────────────────────────────────

    [Fact]
    public void TryFind_EmptyIndex_ReturnsFalse()
    {
        var index = CreateIndex();
        Assert.False(index.TryFind(MakeKey(1), out _));
    }

    [Fact]
    public void Insert_ThenTryFind_ReturnsTrue()
    {
        var index = CreateIndex();
        var key = MakeKey(42);
        var loc = MakeLoc(1, 0);

        index.Insert(key, loc);

        Assert.True(index.TryFind(key, out var found));
        Assert.Equal(loc.PageId, found.PageId);
        Assert.Equal(loc.SlotIndex, found.SlotIndex);
    }

    [Fact]
    public void TryFind_WrongKey_ReturnsFalse()
    {
        var index = CreateIndex();
        index.Insert(MakeKey(1), MakeLoc(1));

        Assert.False(index.TryFind(MakeKey(2), out _));
    }

    [Fact]
    public void Insert_MultipleKeys_FindsEachCorrectly()
    {
        var index = CreateIndex();
        for (int i = 1; i <= 10; i++)
            index.Insert(MakeKey(i), MakeLoc((uint)i));

        for (int i = 1; i <= 10; i++)
        {
            Assert.True(index.TryFind(MakeKey(i), out var loc));
            Assert.Equal((uint)i, loc.PageId);
        }
    }

    [Fact]
    public void Insert_StringKeys_FindsCorrectly()
    {
        var index = CreateIndex();
        index.Insert(MakeKey("alice"), MakeLoc(10));
        index.Insert(MakeKey("bob"), MakeLoc(20));

        Assert.True(index.TryFind(MakeKey("alice"), out var aliceLoc));
        Assert.Equal(10u, aliceLoc.PageId);

        Assert.True(index.TryFind(MakeKey("bob"), out var bobLoc));
        Assert.Equal(20u, bobLoc.PageId);
    }

    // ── Remove ───────────────────────────────────────────────────────────────

    [Fact]
    public void Remove_ExistingEntry_ReturnsTrue()
    {
        var index = CreateIndex();
        var key = MakeKey(5);
        var loc = MakeLoc(3);
        index.Insert(key, loc);

        Assert.True(index.Remove(key, loc));
    }

    [Fact]
    public void Remove_AfterRemove_TryFindReturnsFalse()
    {
        var index = CreateIndex();
        var key = MakeKey(5);
        var loc = MakeLoc(3);
        index.Insert(key, loc);

        index.Remove(key, loc);

        Assert.False(index.TryFind(key, out _));
    }

    [Fact]
    public void Remove_NonExistingEntry_ReturnsFalse()
    {
        var index = CreateIndex();

        Assert.False(index.Remove(MakeKey(99), MakeLoc(1)));
    }

    [Fact]
    public void Remove_WrongLocation_ReturnsFalse()
    {
        var index = CreateIndex();
        var key = MakeKey(1);
        index.Insert(key, MakeLoc(1, 0));

        // Same key but different location
        Assert.False(index.Remove(key, MakeLoc(2, 0)));
    }

    [Fact]
    public void Remove_OnlyMatchingEntry_LeavesOthersIntact()
    {
        var index = CreateIndex();
        var key = MakeKey(1);
        index.Insert(key, MakeLoc(1, 0));
        index.Insert(key, MakeLoc(1, 1));

        index.Remove(key, MakeLoc(1, 0));

        // Second entry still present via FindAll
        var remaining = index.FindAll(key).ToList();
        Assert.Single(remaining);
        Assert.Equal(1u, remaining[0].Location.SlotIndex);
    }

    // ── FindAll ───────────────────────────────────────────────────────────────

    [Fact]
    public void FindAll_EmptyIndex_ReturnsEmpty()
    {
        var index = CreateIndex();
        Assert.Empty(index.FindAll(MakeKey(1)));
    }

    [Fact]
    public void FindAll_NoMatchingKey_ReturnsEmpty()
    {
        var index = CreateIndex();
        index.Insert(MakeKey(1), MakeLoc(1));

        Assert.Empty(index.FindAll(MakeKey(2)));
    }

    [Fact]
    public void FindAll_DuplicateKeys_ReturnsAll()
    {
        var index = CreateIndex(); // not unique
        var key = MakeKey(7);
        index.Insert(key, MakeLoc(1, 0));
        index.Insert(key, MakeLoc(2, 0));
        index.Insert(key, MakeLoc(3, 0));

        var results = index.FindAll(key).ToList();

        Assert.Equal(3, results.Count);
    }

    // ── Unique constraint ─────────────────────────────────────────────────────

    [Fact]
    public void Insert_UniqueIndex_DuplicateKey_Throws()
    {
        var index = CreateIndex(unique: true);
        var key = MakeKey(1);
        index.Insert(key, MakeLoc(1));

        Assert.Throws<InvalidOperationException>(() => index.Insert(key, MakeLoc(2)));
    }

    [Fact]
    public void Insert_UniqueIndex_DifferentKeys_Allowed()
    {
        var index = CreateIndex(unique: true);

        index.Insert(MakeKey(1), MakeLoc(1));
        index.Insert(MakeKey(2), MakeLoc(2)); // should not throw

        Assert.True(index.TryFind(MakeKey(1), out _));
        Assert.True(index.TryFind(MakeKey(2), out _));
    }

    [Fact]
    public void Insert_UniqueIndex_AfterRemove_AllowsReinsert()
    {
        var index = CreateIndex(unique: true);
        var key = MakeKey(1);
        var loc = MakeLoc(1);
        index.Insert(key, loc);
        index.Remove(key, loc);

        // Re-insert same key after remove must succeed
        index.Insert(key, MakeLoc(2));

        Assert.True(index.TryFind(key, out var found));
        Assert.Equal(2u, found.PageId);
    }
}
