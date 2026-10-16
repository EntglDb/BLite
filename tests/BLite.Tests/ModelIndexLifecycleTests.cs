using System.Diagnostics;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Metadata;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Lifecycle of model-declared indexes (<c>HasIndex</c> / <c>HasVectorIndex</c> / ...) across app
/// versions and processes:
/// <list type="bullet">
/// <item>#191: a declared index whose stored definition differs (unique flag, key selector under an
/// explicit name, vector dimensions / metric) is dropped and rebuilt from the documents on open,
/// through the backfill path of #175, so a rebuild that fails saves nothing. An index the model no
/// longer declares is reported, never dropped.</item>
/// <item>#192: two processes opening a model with a new index at the same time end up sharing one
/// index, and saving the metadata from one process keeps the indexes another process added.</item>
/// </list>
/// The "processes" are separate engine instances on the same files within this process, as in
/// <see cref="MultiProcessConcurrentCreateTests"/>.
/// </summary>
public sealed class ModelIndexLifecycleTests : IDisposable
{
    private const string Collection = "lifecycle_docs";
    private static readonly PageFileConfig MultiProcess = PageFileConfig.Default with { AllowMultiProcessAccess = true };

    private readonly string _dbPath;
    private readonly WarningCollector _warnings = new(Collection);

    public ModelIndexLifecycleTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"idx_lifecycle_{Guid.NewGuid():N}.db");
        Trace.Listeners.Add(_warnings);
    }

    public void Dispose()
    {
        Trace.Listeners.Remove(_warnings);
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal"), Path.ChangeExtension(_dbPath, ".wal-shm") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private static LifecycleDoc Doc(int i, string? email = null) => new()
    {
        Name = $"n{i}", Email = email ?? $"e{i}@x", Age = i % 10, Score = i % 7, Embedding = [i, i % 3, i % 5],
    };

    private async Task SeedAsync(PageFileConfig? config = null)
    {
        using var v1 = new LcBaseContext(_dbPath, config ?? PageFileConfig.Default);
        for (int i = 0; i < 50; i++)
            await v1.Docs.InsertAsync(Doc(i));
    }

    private IndexMetadata Stored(StorageEngine storage, string name)
        => Assert.Single(storage.GetCollectionMetadata(Collection)!.Indexes, i => i.Name == name);

    // ── #191: changed definitions ─────────────────────────────────────────────

    [Fact]
    public async Task ChangedUniqueFlag_RebuildsTheIndexAsUnique()
    {
        await SeedAsync();

        using var v2 = new LcUniqueContext(_dbPath);

        Assert.True(Assert.Single(v2.Docs.GetIndexes(), i => i.Name == "idx_email").IsUnique);
        Assert.True(Stored(v2.Storage, "idx_email").IsUnique);
        Assert.Contains(_warnings.Messages, m => m.Contains("idx_email") && m.Contains("unique"));

        // The rebuilt index is enforced right away and still covers the existing documents.
        await Assert.ThrowsAnyAsync<Exception>(async () => await v2.Docs.InsertAsync(new LifecycleDoc { Name = "dup", Email = "e7@x" }));
        Assert.Equal("n7", Assert.Single(await v2.Docs.QueryIndexAsync("idx_email", "e7@x", "e7@x").ToListAsync()).Name);
    }

    [Fact]
    public async Task ChangedSelectorUnderExplicitName_RebuildsTheIndexOnTheNewProperty()
    {
        await SeedAsync();
        uint oldRoot;
        using (var v1 = new LcBaseContext(_dbPath))
            oldRoot = Stored(v1.Storage, "idx_email").RootPageId;

        using var v2 = new LcSelectorContext(_dbPath);

        var stored = Stored(v2.Storage, "idx_email");
        Assert.Equal(new[] { "Name" }, stored.PropertyPaths);
        Assert.NotEqual(oldRoot, stored.RootPageId);
        Assert.Equal(new[] { "Name" }, Assert.Single(v2.Docs.GetIndexes(), i => i.Name == "idx_email").PropertyPaths);
        Assert.Equal("e7@x", Assert.Single(await v2.Docs.QueryIndexAsync("idx_email", "n7", "n7").ToListAsync()).Email);
        Assert.Contains(_warnings.Messages, m => m.Contains("idx_email") && m.Contains("Email") && m.Contains("Name"));
    }

    [Fact]
    public async Task ChangedVectorMetric_RebuildsTheVectorIndex()
    {
        await SeedAsync();

        using var v2 = new LcVectorMetricContext(_dbPath);

        Assert.Equal(VectorMetric.L2, Stored(v2.Storage, "idx_vec").Metric);
        Assert.Equal(VectorMetric.L2, v2.Docs.IndexManager.GetIndex("idx_vec")!.Definition.Metric);
        var nearest = await v2.Docs.VectorSearchAsync("idx_vec", [23f, 2f, 3f], 1).ToListAsync();
        Assert.Equal("n23", Assert.Single(nearest).Name);
        Assert.Contains(_warnings.Messages, m => m.Contains("idx_vec") && m.Contains("L2"));
    }

    [Fact]
    public async Task ChangedVectorDimensions_ThatCannotBeRebuilt_FailTheOpen_AndKeepTheOldIndex()
    {
        await SeedAsync();
        uint oldRoot;
        using (var v1 = new LcBaseContext(_dbPath))
            oldRoot = Stored(v1.Storage, "idx_vec").RootPageId;

        // The stored embeddings have 3 dimensions; the new definition expects 4.
        var sizes = new List<long>();
        for (int open = 0; open < 3; open++)
        {
            var ex = Assert.Throws<InvalidOperationException>(() => new LcVectorDimensionsContext(_dbPath));
            Assert.Contains("idx_vec", ex.Message);
            Assert.Contains("Nothing was saved", ex.Message);
            sizes.Add(new FileInfo(_dbPath).Length);
        }
        // The failed rebuilds free their pages: repeated opens do not grow the file.
        Assert.All(sizes.Skip(1), size => Assert.Equal(sizes[0], size));

        // The previous definition stays in place, intact and usable.
        using var v1Again = new LcBaseContext(_dbPath);
        var stored = Stored(v1Again.Storage, "idx_vec");
        Assert.Equal(oldRoot, stored.RootPageId);
        Assert.Equal(3, stored.Dimensions);
        var nearest = await v1Again.Docs.VectorSearchAsync("idx_vec", [23f, 2f, 3f], 1).ToListAsync();
        Assert.Equal("n23", Assert.Single(nearest).Name);
    }

    [Fact]
    public async Task UnchangedDefinition_IsNotRebuilt()
    {
        await SeedAsync();
        var roots = new Dictionary<string, uint>();
        using (var v1 = new LcBaseContext(_dbPath))
            foreach (var idx in v1.Storage.GetCollectionMetadata(Collection)!.Indexes)
                roots[idx.Name] = idx.RootPageId;
        Assert.Equal(3, roots.Count);

        using var again = new LcBaseContext(_dbPath);

        // A rebuild allocates a new root, so unchanged roots prove the indexes were kept as they were.
        foreach (var idx in again.Storage.GetCollectionMetadata(Collection)!.Indexes)
            Assert.Equal(roots[idx.Name], idx.RootPageId);
        Assert.Empty(_warnings.Messages);
    }

    // ── #191: indexes the model no longer declares ────────────────────────────

    [Fact]
    public async Task ChangedSelectorUnderGeneratedName_AddsTheNewIndex_AndReportsTheOldOne()
    {
        await SeedAsync();

        using var v2 = new LcGeneratedSelectorContext(_dbPath);

        // The new index is built and filled; the old one is kept and reported, not dropped.
        var names = v2.Docs.GetIndexes().Select(i => i.Name).ToList();
        Assert.Contains("idx_Score", names);
        Assert.Contains("idx_Age", names);
        Assert.Equal(7, (await v2.Docs.QueryIndexAsync("idx_Score", 3, 3).ToListAsync()).Count);
        Assert.Equal(5, (await v2.Docs.QueryIndexAsync("idx_Age", 3, 3).ToListAsync()).Count);
        var warning = Assert.Single(_warnings.Messages.Distinct(), m => m.Contains("'idx_Age'"));
        Assert.Contains("does not declare", warning);
        Assert.DoesNotContain(_warnings.Messages, m => m.Contains("'idx_Score'") || m.Contains("'idx_email'") || m.Contains("'idx_vec'"));
    }

    [Fact]
    public async Task IndexNoLongerDeclared_IsReportedAndKept_AndStillMaintained()
    {
        await SeedAsync();

        using (var v0 = new LcNoIndexContext(_dbPath))
        {
            Assert.Equal(3, v0.Docs.GetIndexes().Count());
            Assert.Equal(3, v0.Storage.GetCollectionMetadata(Collection)!.Indexes.Count);
            foreach (var name in new[] { "idx_email", "idx_vec", "idx_Age" })
                Assert.Contains(_warnings.Messages, m => m.Contains($"'{name}'") && m.Contains("does not declare"));

            await v0.Docs.InsertAsync(Doc(100, "late@x"));
        }

        // Written while undeclared, found through the index afterwards.
        using var v1 = new LcBaseContext(_dbPath);
        Assert.Equal("n100", Assert.Single(await v1.Docs.QueryIndexAsync("idx_email", "late@x", "late@x").ToListAsync()).Name);
    }

    private sealed class WarningCollector(string collection) : TraceListener
    {
        private readonly List<string> _messages = new();

        public List<string> Messages { get { lock (_messages) return _messages.ToList(); } }

        public override void Write(string? message) { }

        public override void WriteLine(string? message)
        {
            if (message != null && message.Contains($"'{collection}'"))
                lock (_messages) _messages.Add(message);
        }
    }
}
