using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for the bug: FirstOrDefaultAsync(x =&gt; x.Id == value) always returns null
/// for DocumentCollection&lt;string, T&gt; when a secondary index on Id is present.
///
/// Root cause: IndexOptimizer finds a secondary index on "Id" (C# property name) and uses it
/// via QueryIndexAsync. However when no secondary index exists, BsonExpressionEvaluator maps
/// "Id" → "_id" for the BSON scan path. When an index on Id IS present the secondary-index
/// path is taken instead, and range-query boundaries must match the composite key stored.
///
/// The ToLowerInvariant() workaround bypasses both the index optimizer and the BSON evaluator
/// (because the expression is no longer a simple member access), falling all the way through to
/// FindAllAsync() + in-memory LINQ filter, which always works.
/// </summary>
public class StringMapperIdLookupTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public StringMapperIdLookupTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"string_mapper_id_lookup_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    /// <summary>
    /// Validates the basic BSON-scan path: no secondary index on Id, x.Id == constant.
    /// BsonExpressionEvaluator maps "Id" → "_id" and scans raw BSON pages.
    /// </summary>
    [Fact]
    public async Task StringId_FirstOrDefaultAsync_ConstantEquality_NoSecondaryIndex_ReturnsDocument()
    {
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "device1", Value = "test" });
        await _db.SaveChangesAsync();

        // Sanity: full scan (no predicate) must work
        var all = await _db.StringEntities.AsQueryable().FirstOrDefaultAsync();
        Assert.NotNull(all);
        Assert.Equal("device1", all!.Id);

        // Primary path under test: constant equality on Id
        var result = await _db.StringEntities.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id == "device1");

        Assert.NotNull(result);
        Assert.Equal("device1", result!.Id);
    }

    /// <summary>
    /// Same as above but the right-hand side is a closure variable rather than a literal.
    /// </summary>
    [Fact]
    public async Task StringId_FirstOrDefaultAsync_ClosureEquality_NoSecondaryIndex_ReturnsDocument()
    {
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "device1", Value = "test" });
        await _db.SaveChangesAsync();

        var id = "device1";
        var result = await _db.StringEntities.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id == id);

        Assert.NotNull(result);
        Assert.Equal("device1", result!.Id);
    }

    /// <summary>
    /// Validates that the secondary-index path works correctly when a unique index on Id is
    /// explicitly defined. The IndexOptimizer will prefer this index over the BSON scan.
    /// This is the exact scenario reported in the issue.
    /// </summary>
    [Fact]
    public async Task StringId_FirstOrDefaultAsync_WithSecondaryIndexOnId_ReturnsDocument()
    {
        // Create a secondary unique index on Id (mirrors the user's .HasIndex(x => x.Id, unique: true))
        await _db.StringEntities.CreateIndexAsync(x => x.Id, unique: true, name: "idx_Id");

        await _db.StringEntities.InsertAsync(new StringEntity { Id = "device1", Value = "test" });
        await _db.SaveChangesAsync();

        // Sanity: count and full scan must work
        var count = await _db.StringEntities.CountAsync();
        Assert.Equal(1, count);

        var allFirst = await _db.StringEntities.AsQueryable().FirstOrDefaultAsync();
        Assert.NotNull(allFirst);
        Assert.Equal("device1", allFirst!.Id);

        // Primary path under test: equality on Id with a secondary index present
        // Before the fix this returned null because the IndexOptimizer selected the secondary
        // index but the BSON scan path (which correctly maps "Id" → "_id") was skipped.
        var result = await _db.StringEntities.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id == "device1");

        Assert.NotNull(result);
        Assert.Equal("device1", result!.Id);
    }

    /// <summary>
    /// Same as above but with a closure variable on the right-hand side.
    /// </summary>
    [Fact]
    public async Task StringId_FirstOrDefaultAsync_ClosureEquality_WithSecondaryIndexOnId_ReturnsDocument()
    {
        await _db.StringEntities.CreateIndexAsync(x => x.Id, unique: true, name: "idx_Id");

        await _db.StringEntities.InsertAsync(new StringEntity { Id = "device1", Value = "test" });
        await _db.SaveChangesAsync();

        var id = "device1";
        var result = await _db.StringEntities.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id == id);

        Assert.NotNull(result);
        Assert.Equal("device1", result!.Id);
    }

    /// <summary>
    /// Confirms the workaround reported in the issue still functions correctly
    /// (ToLowerInvariant forces a full scan + in-memory filter).
    /// </summary>
    [Fact]
    public async Task StringId_FirstOrDefaultAsync_ToLowerInvariant_WorkaroundStillWorks()
    {
        await _db.StringEntities.CreateIndexAsync(x => x.Id, unique: true, name: "idx_Id");

        await _db.StringEntities.InsertAsync(new StringEntity { Id = "device1", Value = "test" });
        await _db.SaveChangesAsync();

        var result = await _db.StringEntities.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id.ToLowerInvariant() == "device1".ToLowerInvariant());

        Assert.NotNull(result);
        Assert.Equal("device1", result!.Id);
    }

    /// <summary>
    /// Multiple documents: the equality filter must select the correct one.
    /// </summary>
    [Fact]
    public async Task StringId_Where_Equality_WithSecondaryIndexOnId_ReturnsOnlyMatchingDocument()
    {
        await _db.StringEntities.CreateIndexAsync(x => x.Id, unique: true, name: "idx_Id");

        await _db.StringEntities.InsertAsync(new StringEntity { Id = "apple", Value = "a" });
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "banana", Value = "b" });
        await _db.StringEntities.InsertAsync(new StringEntity { Id = "cherry", Value = "c" });
        await _db.SaveChangesAsync();

        var results = await _db.StringEntities.AsQueryable()
            .Where(x => x.Id == "banana")
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("banana", results[0].Id);
        Assert.Equal("b", results[0].Value);
    }

    /// <summary>
    /// Equality on a non-existent Id must return null (not throw).
    /// </summary>
    [Fact]
    public async Task StringId_FirstOrDefaultAsync_WithSecondaryIndexOnId_NoMatch_ReturnsNull()
    {
        await _db.StringEntities.CreateIndexAsync(x => x.Id, unique: true, name: "idx_Id");

        await _db.StringEntities.InsertAsync(new StringEntity { Id = "device1", Value = "test" });
        await _db.SaveChangesAsync();

        var result = await _db.StringEntities.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id == "NOPE");

        Assert.Null(result);
    }
}
