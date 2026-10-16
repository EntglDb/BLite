using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Metadata;
using BLite.Core.Storage;

namespace BLite.Shared;

// Contexts for ModelIndexLifecycleTests (#191, #192): the same collection under model versions whose
// index definitions differ.

public sealed class LifecycleDoc
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public int Age { get; set; }
    public int Score { get; set; }
    public float[] Embedding { get; set; } = [];
}

/// <summary>"App v1": a named B-Tree index, a named vector index and a generated-name B-Tree index.</summary>
public partial class LcBaseContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, LifecycleDoc> Docs { get; set; } = null!;
    public LcBaseContext(string path) : base(path) => InitializeCollections();
    public LcBaseContext(string path, PageFileConfig config) : base(path, config) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LifecycleDoc>().ToCollection("lifecycle_docs")
            .HasIndex(x => x.Email, name: "idx_email")
            .HasVectorIndex(x => x.Embedding, dimensions: 3, metric: VectorMetric.Cosine, name: "idx_vec")
            .HasIndex(x => x.Age);
    }
}

/// <summary>Same as v1, but <c>idx_email</c> is now unique.</summary>
public partial class LcUniqueContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, LifecycleDoc> Docs { get; set; } = null!;
    public LcUniqueContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LifecycleDoc>().ToCollection("lifecycle_docs")
            .HasIndex(x => x.Email, name: "idx_email", unique: true)
            .HasVectorIndex(x => x.Embedding, dimensions: 3, metric: VectorMetric.Cosine, name: "idx_vec")
            .HasIndex(x => x.Age);
    }
}

/// <summary>Same as v1, but <c>idx_email</c> now indexes <c>Name</c>.</summary>
public partial class LcSelectorContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, LifecycleDoc> Docs { get; set; } = null!;
    public LcSelectorContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LifecycleDoc>().ToCollection("lifecycle_docs")
            .HasIndex(x => x.Name, name: "idx_email")
            .HasVectorIndex(x => x.Embedding, dimensions: 3, metric: VectorMetric.Cosine, name: "idx_vec")
            .HasIndex(x => x.Age);
    }
}

/// <summary>Same as v1, but <c>idx_vec</c> now uses the L2 metric.</summary>
public partial class LcVectorMetricContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, LifecycleDoc> Docs { get; set; } = null!;
    public LcVectorMetricContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LifecycleDoc>().ToCollection("lifecycle_docs")
            .HasIndex(x => x.Email, name: "idx_email")
            .HasVectorIndex(x => x.Embedding, dimensions: 3, metric: VectorMetric.L2, name: "idx_vec")
            .HasIndex(x => x.Age);
    }
}

/// <summary>Same as v1, but <c>idx_vec</c> now expects 4 dimensions, which the stored embeddings lack.</summary>
public partial class LcVectorDimensionsContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, LifecycleDoc> Docs { get; set; } = null!;
    public LcVectorDimensionsContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LifecycleDoc>().ToCollection("lifecycle_docs")
            .HasIndex(x => x.Email, name: "idx_email")
            .HasVectorIndex(x => x.Embedding, dimensions: 4, metric: VectorMetric.Cosine, name: "idx_vec")
            .HasIndex(x => x.Age);
    }
}

/// <summary>Same as v1, but the generated-name index moved from <c>Age</c> to <c>Score</c>.</summary>
public partial class LcGeneratedSelectorContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, LifecycleDoc> Docs { get; set; } = null!;
    public LcGeneratedSelectorContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LifecycleDoc>().ToCollection("lifecycle_docs")
            .HasIndex(x => x.Email, name: "idx_email")
            .HasVectorIndex(x => x.Embedding, dimensions: 3, metric: VectorMetric.Cosine, name: "idx_vec")
            .HasIndex(x => x.Score);
    }
}

/// <summary>The collection without any declared index.</summary>
public partial class LcNoIndexContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, LifecycleDoc> Docs { get; set; } = null!;
    public LcNoIndexContext(string path) : base(path) => InitializeCollections();
    public LcNoIndexContext(string path, PageFileConfig config) : base(path, config) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LifecycleDoc>().ToCollection("lifecycle_docs");
    }
}
