using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.Shared;

public enum DiffKind
{
    Red = 0,
    Green = 1,
    Blue = 2,
    Black = 7,
}

/// <summary>
/// One property per kind of value the LINQ provider compares: string (plain and nullable), int,
/// long, double, decimal, DateTime, bool, nullable int and an enum.
/// </summary>
public class DiffDoc
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Nick { get; set; }
    public int Count { get; set; }
    public long Total { get; set; }
    public double Ratio { get; set; }
    public decimal Price { get; set; }
    public DateTime When { get; set; }
    public bool Flag { get; set; }
    public int? MaybeCount { get; set; }
    public DiffKind Kind { get; set; }
}

/// <summary><see cref="DiffDoc"/> with a B-Tree index on every queried property.</summary>
public partial class DiffIndexedDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, DiffDoc> Docs { get; set; } = null!;

    public DiffIndexedDbContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DiffDoc>().ToCollection("diff_docs")
            .HasIndex(d => d.Name)
            .HasIndex(d => d.Nick)
            .HasIndex(d => d.Count)
            .HasIndex(d => d.Total)
            .HasIndex(d => d.Ratio)
            .HasIndex(d => d.Price)
            .HasIndex(d => d.When)
            .HasIndex(d => d.Flag)
            .HasIndex(d => d.MaybeCount)
            .HasIndex(d => d.Kind);
    }
}

/// <summary><see cref="DiffDoc"/> without any secondary index: every query is a scan.</summary>
public partial class DiffPlainDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, DiffDoc> Docs { get; set; } = null!;

    public DiffPlainDbContext(string path) : base(path) => InitializeCollections();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.Entity<DiffDoc>().ToCollection("diff_docs");
}
